//! Central cost model for snapshot refresh fast paths.
//!
//! The live snapshot stack has several places that need to answer the same
//! question: when a small set of root rows is dirty, should we drive execution
//! from that subset, or should we let indexes intersect the subset with the
//! normal access path? Keeping the answer here prevents compile-time and
//! runtime heuristics from drifting apart.

use cynos_query::context::RestrictedAccessMode;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RootSubsetPlanVariant {
    Small,
    Large,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RootSubsetDecisionReason {
    WithinAbsoluteSubsetCeiling,
    WithinSubsetFractionCeiling,
    PreferIndexDrivenIntersect,
    CompiledSmallProfile,
    CompiledLargeProfile,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RootSubsetRefreshDecision {
    pub variant: RootSubsetPlanVariant,
    pub reason: RootSubsetDecisionReason,
    pub affected_row_count: usize,
    pub table_row_count: usize,
    pub preferred_access_mode: RestrictedAccessMode,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct RootSubsetPlanningDecision {
    pub variant: RootSubsetPlanVariant,
    pub reason: RootSubsetDecisionReason,
    pub effective_subset_rows: usize,
    pub subset_fraction: Option<f64>,
    pub preferred_access_mode: RestrictedAccessMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RootSubsetCostPolicy {
    /// Subset-driven execution remains cheap below this absolute row-count
    /// ceiling even when the subset is not tiny relative to the table.
    max_subset_driven_rows: usize,
    /// Above the absolute ceiling, subset-driven execution is selected only
    /// while the affected subset is at most
    /// `1 / subset_to_table_ratio_denominator` of the root table.
    subset_to_table_ratio_denominator: usize,
    /// Synthetic row count exposed to the planner for the small/subset-driven
    /// profile. This is intentionally lower than the real table cardinality so
    /// the CBO can choose plans appropriate for subset execution.
    small_profile_effective_rows: usize,
}

impl RootSubsetCostPolicy {
    pub(crate) const DEFAULT: Self = Self {
        max_subset_driven_rows: 8_192,
        subset_to_table_ratio_denominator: 4,
        small_profile_effective_rows: 1_024,
    };

    pub(crate) fn decide_refresh(
        self,
        affected_row_count: usize,
        table_row_count: usize,
    ) -> RootSubsetRefreshDecision {
        let (variant, reason) = if affected_row_count <= self.max_subset_driven_rows {
            (
                RootSubsetPlanVariant::Small,
                RootSubsetDecisionReason::WithinAbsoluteSubsetCeiling,
            )
        } else if affected_row_count.saturating_mul(self.subset_to_table_ratio_denominator)
            <= table_row_count
        {
            (
                RootSubsetPlanVariant::Small,
                RootSubsetDecisionReason::WithinSubsetFractionCeiling,
            )
        } else {
            (
                RootSubsetPlanVariant::Large,
                RootSubsetDecisionReason::PreferIndexDrivenIntersect,
            )
        };

        RootSubsetRefreshDecision {
            variant,
            reason,
            affected_row_count,
            table_row_count,
            preferred_access_mode: variant.preferred_access_mode(),
        }
    }

    pub(crate) fn planning_decision(
        self,
        variant: RootSubsetPlanVariant,
        table_row_count: usize,
    ) -> Option<RootSubsetPlanningDecision> {
        let effective_subset_rows = self.effective_subset_rows(variant, table_row_count)?;
        let subset_fraction = if table_row_count > 0 {
            Some(effective_subset_rows as f64 / table_row_count as f64)
        } else {
            None
        };

        Some(RootSubsetPlanningDecision {
            variant,
            reason: match variant {
                RootSubsetPlanVariant::Small => RootSubsetDecisionReason::CompiledSmallProfile,
                RootSubsetPlanVariant::Large => RootSubsetDecisionReason::CompiledLargeProfile,
            },
            effective_subset_rows,
            subset_fraction,
            preferred_access_mode: variant.preferred_access_mode(),
        })
    }

    fn effective_subset_rows(
        self,
        variant: RootSubsetPlanVariant,
        table_row_count: usize,
    ) -> Option<usize> {
        match variant {
            RootSubsetPlanVariant::Small => {
                Some(table_row_count.min(self.small_profile_effective_rows))
            }
            RootSubsetPlanVariant::Large => {
                if table_row_count == 0 {
                    return None;
                }
                let lower_bound = self
                    .max_subset_driven_rows
                    .saturating_add(1)
                    .min(table_row_count.max(1));
                let ratio_bound =
                    core::cmp::max(table_row_count / self.subset_to_table_ratio_denominator, 1);
                Some(core::cmp::max(lower_bound, ratio_bound))
            }
        }
    }
}

impl RootSubsetPlanVariant {
    pub(crate) fn preferred_access_mode(self) -> RestrictedAccessMode {
        match self {
            RootSubsetPlanVariant::Small => RestrictedAccessMode::SubsetDriven,
            RootSubsetPlanVariant::Large => RestrictedAccessMode::IndexDrivenIntersect,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PartialRefreshWindowReason {
    MinimumFloor,
    ProportionalToVisibleLimit,
    MaximumCeiling,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PartialRefreshWindowDecision {
    pub overscan: usize,
    pub reason: PartialRefreshWindowReason,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PartialRefreshWindowPolicy {
    min_overscan: usize,
    max_overscan: usize,
    overscan_divisor: usize,
}

impl PartialRefreshWindowPolicy {
    pub(crate) const DEFAULT: Self = Self {
        min_overscan: 256,
        max_overscan: 1_024,
        overscan_divisor: 4,
    };

    pub(crate) fn decide(self, limit: usize) -> PartialRefreshWindowDecision {
        let divisor = self.overscan_divisor.max(1);
        let proportional = limit / divisor;
        let capped = core::cmp::min(proportional, self.max_overscan);
        let overscan = core::cmp::max(self.min_overscan, capped);
        let reason = if proportional < self.min_overscan {
            PartialRefreshWindowReason::MinimumFloor
        } else if proportional > self.max_overscan {
            PartialRefreshWindowReason::MaximumCeiling
        } else {
            PartialRefreshWindowReason::ProportionalToVisibleLimit
        };

        PartialRefreshWindowDecision { overscan, reason }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotRefreshCostModel {
    root_subset: RootSubsetCostPolicy,
    partial_window: PartialRefreshWindowPolicy,
}

impl SnapshotRefreshCostModel {
    pub(crate) const DEFAULT: Self = Self {
        root_subset: RootSubsetCostPolicy::DEFAULT,
        partial_window: PartialRefreshWindowPolicy::DEFAULT,
    };

    pub(crate) fn decide_root_subset_refresh(
        self,
        affected_row_count: usize,
        table_row_count: usize,
    ) -> RootSubsetRefreshDecision {
        self.root_subset
            .decide_refresh(affected_row_count, table_row_count)
    }

    pub(crate) fn root_subset_planning_decision(
        self,
        variant: RootSubsetPlanVariant,
        table_row_count: usize,
    ) -> Option<RootSubsetPlanningDecision> {
        self.root_subset.planning_decision(variant, table_row_count)
    }

    pub(crate) fn decide_partial_refresh_window(
        self,
        visible_limit: usize,
    ) -> PartialRefreshWindowDecision {
        self.partial_window.decide(visible_limit)
    }
}
