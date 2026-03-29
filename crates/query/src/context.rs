//! Execution context for query execution.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;

/// Index type enumeration for query optimization.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum QueryIndexType {
    /// Hash index - O(1) point lookups.
    Hash,
    /// B+Tree index - O(log n) range queries.
    #[default]
    BTree,
    /// GIN (Generalized Inverted Index) - for JSONB containment queries.
    Gin,
}

/// Preferred restricted access mode when executing against a row-id subset.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum RestrictedAccessMode {
    /// Let the planner choose between subset-driven probing and global index intersection.
    #[default]
    Auto,
    /// Prefer iterating the restricted row-id subset directly.
    SubsetDriven,
    /// Prefer global index access followed by subset intersection.
    IndexDrivenIntersect,
}

/// High-level planning mode for the current query compilation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum PlanningMode {
    /// Ordinary full-relation planning.
    #[default]
    Default,
    /// Planning for a query whose root relation is restricted to a row-id subset.
    RestrictedRelation,
}

/// Internal planner feature flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct PlannerFeatureFlags {
    pub restricted_relation_cbo: bool,
}

/// Additional planning-time hints for restricted-relation execution.
#[derive(Clone, Debug)]
pub struct PlanningIntent {
    pub mode: PlanningMode,
    pub restricted_table: Option<String>,
    pub exact_subset_rows: Option<usize>,
    pub subset_fraction: Option<f64>,
    pub preferred_access_mode: RestrictedAccessMode,
    pub anchor_table: Option<String>,
    pub allow_global_fallback: bool,
}

impl Default for PlanningIntent {
    fn default() -> Self {
        Self {
            mode: PlanningMode::Default,
            restricted_table: None,
            exact_subset_rows: None,
            subset_fraction: None,
            preferred_access_mode: RestrictedAccessMode::Auto,
            anchor_table: None,
            allow_global_fallback: true,
        }
    }
}

/// Statistics about a table for query optimization.
#[derive(Clone, Debug, Default)]
pub struct TableStats {
    /// Number of rows in the table.
    pub row_count: usize,
    /// Whether the table is sorted by primary key.
    pub is_sorted: bool,
    /// Available indexes on this table.
    pub indexes: Vec<IndexInfo>,
}

/// Information about an index.
#[derive(Clone, Debug)]
pub struct IndexInfo {
    /// Index name.
    pub name: String,
    /// Column names in the index.
    pub columns: Vec<String>,
    /// Whether this is a unique index.
    pub is_unique: bool,
    /// Index type (BTree or GIN).
    pub index_type: QueryIndexType,
    /// Optional normalized JSON paths covered by a GIN index.
    pub gin_paths: Option<Vec<String>>,
}

impl IndexInfo {
    /// Creates a new index info with default BTree type.
    pub fn new(name: impl Into<String>, columns: Vec<String>, is_unique: bool) -> Self {
        Self {
            name: name.into(),
            columns,
            is_unique,
            index_type: QueryIndexType::BTree,
            gin_paths: None,
        }
    }

    /// Creates a new GIN index info.
    pub fn new_gin(name: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            name: name.into(),
            columns,
            is_unique: false, // GIN indexes are never unique
            index_type: QueryIndexType::Gin,
            gin_paths: None,
        }
    }

    /// Sets the index type.
    pub fn with_type(mut self, index_type: QueryIndexType) -> Self {
        self.index_type = index_type;
        self
    }

    /// Restricts a GIN index to a set of normalized JSON paths.
    pub fn with_gin_paths(mut self, gin_paths: Vec<String>) -> Self {
        self.gin_paths = if gin_paths.is_empty() {
            None
        } else {
            Some(gin_paths)
        };
        self
    }

    /// Returns true if this is a GIN index.
    pub fn is_gin(&self) -> bool {
        self.index_type == QueryIndexType::Gin
    }

    /// Returns true if this is a hash index.
    pub fn is_hash(&self) -> bool {
        self.index_type == QueryIndexType::Hash
    }

    /// Returns true if this index can satisfy point lookups.
    pub fn supports_point_lookup(&self) -> bool {
        !self.is_gin()
    }

    /// Returns true if this index can satisfy ordered/range scans.
    pub fn supports_range(&self) -> bool {
        self.index_type == QueryIndexType::BTree
    }

    /// Returns true if this index preserves key order.
    pub fn supports_ordering(&self) -> bool {
        self.supports_range()
    }

    /// Returns whether this GIN index supports the given normalized path.
    pub fn supports_gin_path(&self, path: &str) -> bool {
        self.is_gin()
            && self.gin_paths.as_ref().map_or(true, |paths| {
                paths.iter().any(|candidate| candidate == path)
            })
    }
}

/// Execution context providing access to table metadata and statistics.
#[derive(Clone, Debug, Default)]
pub struct ExecutionContext {
    /// Table statistics for optimization.
    table_stats: BTreeMap<String, TableStats>,
    /// Optional effective row-count overrides used by restricted-relation planning.
    effective_row_counts: BTreeMap<String, usize>,
    /// Optional distinct key counts keyed by `(table, index_name)`.
    index_distinct_counts: BTreeMap<(String, String), usize>,
    /// Precomputed GIN key posting costs keyed by `(table, index_name, key)`.
    gin_key_costs: BTreeMap<(String, String, String), usize>,
    /// Precomputed GIN key/value posting costs keyed by `(table, index_name, key, value)`.
    gin_key_value_costs: BTreeMap<(String, String, String, String), usize>,
    /// Internal planner feature flags.
    planner_feature_flags: PlannerFeatureFlags,
    /// Additional planning hints.
    planning_intent: PlanningIntent,
}

impl ExecutionContext {
    /// Creates a new empty execution context.
    pub fn new() -> Self {
        Self {
            table_stats: BTreeMap::new(),
            effective_row_counts: BTreeMap::new(),
            index_distinct_counts: BTreeMap::new(),
            gin_key_costs: BTreeMap::new(),
            gin_key_value_costs: BTreeMap::new(),
            planner_feature_flags: PlannerFeatureFlags::default(),
            planning_intent: PlanningIntent::default(),
        }
    }

    /// Registers table statistics.
    pub fn register_table(&mut self, table: impl Into<String>, stats: TableStats) {
        self.table_stats.insert(table.into(), stats);
    }

    /// Gets statistics for a table.
    pub fn get_stats(&self, table: &str) -> Option<&TableStats> {
        self.table_stats.get(table)
    }

    /// Returns the base row count for a table, before any restricted-relation override.
    pub fn base_row_count(&self, table: &str) -> usize {
        self.table_stats
            .get(table)
            .map(|s| s.row_count)
            .unwrap_or(0)
    }

    /// Gets the row count for a table.
    pub fn row_count(&self, table: &str) -> usize {
        self.effective_row_counts
            .get(table)
            .copied()
            .unwrap_or_else(|| self.base_row_count(table))
    }

    /// Checks if a table has an index on the given columns.
    pub fn has_index(&self, table: &str, columns: &[&str]) -> bool {
        self.table_stats
            .get(table)
            .map(|s| {
                s.indexes.iter().any(|idx| {
                    idx.columns.len() == columns.len()
                        && idx.columns.iter().zip(columns.iter()).all(|(a, b)| a == *b)
                })
            })
            .unwrap_or(false)
    }

    /// Finds an index whose key columns exactly match the requested columns.
    pub fn find_index(&self, table: &str, columns: &[&str]) -> Option<&IndexInfo> {
        self.table_stats.get(table).and_then(|s| {
            s.indexes.iter().find(|idx| {
                idx.columns.len() == columns.len()
                    && idx.columns.iter().zip(columns.iter()).all(|(a, b)| a == *b)
            })
        })
    }

    /// Finds an index whose leading columns match the requested columns.
    pub fn find_index_prefix(&self, table: &str, columns: &[&str]) -> Option<&IndexInfo> {
        self.table_stats.get(table).and_then(|s| {
            s.indexes.iter().find(|idx| {
                idx.supports_ordering()
                    && idx.columns.len() >= columns.len()
                    && idx.columns.iter().zip(columns.iter()).all(|(a, b)| a == *b)
            })
        })
    }

    /// Finds an index by its name.
    pub fn find_index_by_name(&self, table: &str, index_name: &str) -> Option<&IndexInfo> {
        self.table_stats
            .get(table)
            .and_then(|s| s.indexes.iter().find(|idx| idx.name == index_name))
    }

    /// Finds a GIN index for the given column.
    pub fn find_gin_index(&self, table: &str, column: &str) -> Option<&IndexInfo> {
        self.table_stats.get(table).and_then(|s| {
            s.indexes
                .iter()
                .find(|idx| idx.is_gin() && idx.columns.iter().any(|c| c == column))
        })
    }

    /// Finds the most specific GIN index for the given column/path pair.
    pub fn find_gin_index_for_path(
        &self,
        table: &str,
        column: &str,
        path: &str,
    ) -> Option<&IndexInfo> {
        let stats = self.table_stats.get(table)?;

        stats
            .indexes
            .iter()
            .find(|idx| {
                idx.is_gin()
                    && idx.columns.iter().any(|c| c == column)
                    && idx
                        .gin_paths
                        .as_ref()
                        .is_some_and(|paths| paths.iter().any(|candidate| candidate == path))
            })
            .or_else(|| {
                stats.indexes.iter().find(|idx| {
                    idx.is_gin()
                        && idx.columns.iter().any(|c| c == column)
                        && idx.gin_paths.is_none()
                })
            })
    }

    /// Finds the primary key index (unique BTree index) for a table.
    /// Returns the first unique BTree index found, which is typically the primary key.
    pub fn find_primary_index(&self, table: &str) -> Option<&IndexInfo> {
        self.table_stats.get(table).and_then(|s| {
            s.indexes
                .iter()
                .find(|idx| idx.is_unique && idx.supports_ordering())
        })
    }

    /// Registers an effective row-count override for a table.
    pub fn register_effective_row_count(&mut self, table: impl Into<String>, row_count: usize) {
        self.effective_row_counts.insert(table.into(), row_count);
    }

    /// Returns the effective row-count override for a table, if one exists.
    pub fn effective_row_count_override(&self, table: &str) -> Option<usize> {
        self.effective_row_counts.get(table).copied()
    }

    /// Registers the distinct-key count for a specific index.
    pub fn register_index_distinct_count(
        &mut self,
        table: impl Into<String>,
        index_name: impl Into<String>,
        distinct_keys: usize,
    ) {
        self.index_distinct_counts
            .insert((table.into(), index_name.into()), distinct_keys);
    }

    /// Returns the distinct-key count for a specific index, if known.
    pub fn index_distinct_count(&self, table: &str, index_name: &str) -> Option<usize> {
        self.index_distinct_counts
            .get(&(table.into(), index_name.into()))
            .copied()
    }

    /// Returns an estimated row count for a point lookup on the given index.
    pub fn estimate_point_lookup_rows(&self, table: &str, index_name: &str, is_unique: bool) -> usize {
        if is_unique {
            return 1;
        }

        let table_rows = self.row_count(table);
        if table_rows == 0 {
            return 0;
        }

        if let Some(distinct) = self.index_distinct_count(table, index_name) {
            return core::cmp::max(table_rows / distinct.max(1), 1);
        }

        core::cmp::max(table_rows / 10, 1)
    }

    /// Registers a GIN key posting cost.
    pub fn register_gin_key_cost(
        &mut self,
        table: impl Into<String>,
        index_name: impl Into<String>,
        key: impl Into<String>,
        cost: usize,
    ) {
        self.gin_key_costs
            .insert((table.into(), index_name.into(), key.into()), cost);
    }

    /// Returns a precomputed GIN key posting cost, if one exists.
    pub fn gin_key_cost(&self, table: &str, index_name: &str, key: &str) -> Option<usize> {
        self.gin_key_costs
            .get(&(table.into(), index_name.into(), key.into()))
            .copied()
    }

    /// Registers a GIN key/value posting cost.
    pub fn register_gin_key_value_cost(
        &mut self,
        table: impl Into<String>,
        index_name: impl Into<String>,
        key: impl Into<String>,
        value: impl Into<String>,
        cost: usize,
    ) {
        self.gin_key_value_costs.insert(
            (table.into(), index_name.into(), key.into(), value.into()),
            cost,
        );
    }

    /// Returns a precomputed GIN key/value posting cost, if one exists.
    pub fn gin_key_value_cost(
        &self,
        table: &str,
        index_name: &str,
        key: &str,
        value: &str,
    ) -> Option<usize> {
        self.gin_key_value_costs
            .get(&(table.into(), index_name.into(), key.into(), value.into()))
            .copied()
    }

    /// Replaces the current internal planner feature flags.
    pub fn set_planner_feature_flags(&mut self, flags: PlannerFeatureFlags) {
        self.planner_feature_flags = flags;
    }

    /// Returns the current internal planner feature flags.
    pub fn planner_feature_flags(&self) -> PlannerFeatureFlags {
        self.planner_feature_flags
    }

    /// Replaces the current planning intent.
    pub fn set_planning_intent(&mut self, planning_intent: PlanningIntent) {
        self.planning_intent = planning_intent;
    }

    /// Returns the current planning intent.
    pub fn planning_intent(&self) -> &PlanningIntent {
        &self.planning_intent
    }

    /// Returns true when restricted-relation CBO is enabled for the current plan.
    pub fn restricted_relation_cbo_enabled(&self) -> bool {
        self.planner_feature_flags.restricted_relation_cbo
            && self.planning_intent.mode == PlanningMode::RestrictedRelation
    }

    /// Returns true when `table` is the restricted relation for the current plan.
    pub fn is_restricted_relation(&self, table: &str) -> bool {
        self.restricted_relation_cbo_enabled()
            && self
                .planning_intent
                .restricted_table
                .as_deref()
                .is_some_and(|candidate| candidate == table)
    }

    /// Returns true when `table` is the anchor relation for the current plan.
    pub fn is_anchor_relation(&self, table: &str) -> bool {
        self.restricted_relation_cbo_enabled()
            && self
                .planning_intent
                .anchor_table
                .as_deref()
                .is_some_and(|candidate| candidate == table)
    }

    /// Returns the preferred restricted access mode for `table`, if applicable.
    pub fn restricted_access_mode(&self, table: &str) -> RestrictedAccessMode {
        if self.is_restricted_relation(table) {
            self.planning_intent.preferred_access_mode
        } else {
            RestrictedAccessMode::Auto
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_context() {
        let mut ctx = ExecutionContext::new();

        let stats = TableStats {
            row_count: 1000,
            is_sorted: true,
            indexes: alloc::vec![IndexInfo::new("idx_id", alloc::vec!["id".into()], true)],
        };

        ctx.register_table("users", stats);

        assert_eq!(ctx.row_count("users"), 1000);
        assert!(ctx.has_index("users", &["id"]));
        assert!(!ctx.has_index("users", &["name"]));
    }

    #[test]
    fn test_find_index() {
        let mut ctx = ExecutionContext::new();

        let stats = TableStats {
            row_count: 100,
            is_sorted: false,
            indexes: alloc::vec![
                IndexInfo::new("idx_id", alloc::vec!["id".into()], true),
                IndexInfo::new(
                    "idx_name_age",
                    alloc::vec!["name".into(), "age".into()],
                    false
                ),
            ],
        };

        ctx.register_table("users", stats);

        let idx = ctx.find_index("users", &["id"]);
        assert!(idx.is_some());
        assert_eq!(idx.unwrap().name, "idx_id");

        let idx = ctx.find_index("users", &["name"]);
        assert!(idx.is_none());

        let idx = ctx.find_index_prefix("users", &["name"]);
        assert!(idx.is_some());
        assert_eq!(idx.unwrap().name, "idx_name_age");

        let idx = ctx.find_index("users", &["email"]);
        assert!(idx.is_none());
    }

    #[test]
    fn test_find_gin_index_for_path() {
        let mut ctx = ExecutionContext::new();

        let stats = TableStats {
            row_count: 100,
            is_sorted: false,
            indexes: alloc::vec![
                IndexInfo::new_gin("idx_metadata_tier", alloc::vec!["metadata".into()])
                    .with_gin_paths(alloc::vec!["customer.tier".into()]),
                IndexInfo::new_gin("idx_metadata_full", alloc::vec!["metadata".into()]),
            ],
        };

        ctx.register_table("issues", stats);

        let idx = ctx
            .find_gin_index_for_path("issues", "metadata", "customer.tier")
            .unwrap();
        assert_eq!(idx.name, "idx_metadata_tier");

        let fallback = ctx
            .find_gin_index_for_path("issues", "metadata", "risk.bucket")
            .unwrap();
        assert_eq!(fallback.name, "idx_metadata_full");
    }
}
