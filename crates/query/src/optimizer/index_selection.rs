//! Index selection optimization pass.

use crate::ast::{BinaryOp, Expr};
use crate::context::{ExecutionContext, IndexInfo, RestrictedAccessMode};
use crate::optimizer::OptimizerPass;
use crate::planner::{IndexBounds, LogicalPlan};
use alloc::boxed::Box;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use cynos_core::Value;
use cynos_index::contains_trigram_pairs;
use cynos_index::KeyRange;
use cynos_jsonb::JsonPath;

/// Index selection optimization.
///
/// Analyzes predicates and selects appropriate indexes for scans.
/// This pass identifies Filter(Scan) patterns where the filter predicate
/// can be satisfied using an index, converting them to IndexScan operations.
///
/// Supports:
/// - Point lookups: `col = value` → IndexGet
/// - Range scans: `col > value`, `col < value`, etc. → IndexScan
/// - IN queries: `col IN (v1, v2, v3)` → IndexInGet
/// - JSONB queries with GIN indexes → GinIndexScan
pub struct IndexSelection {
    /// Execution context with table statistics and index information.
    context: Option<ExecutionContext>,
}

impl Default for IndexSelection {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexSelection {
    /// Creates a new index selection pass without context.
    pub fn new() -> Self {
        Self { context: None }
    }

    /// Creates a new index selection pass with execution context.
    pub fn with_context(context: ExecutionContext) -> Self {
        Self {
            context: Some(context),
        }
    }
}

impl OptimizerPass for IndexSelection {
    fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        self.select_indexes(plan)
    }

    fn name(&self) -> &'static str {
        "index_selection"
    }
}

/// Information extracted from a predicate for index selection.
#[derive(Debug, Clone)]
pub struct PredicateInfo {
    /// Table name referenced by the predicate.
    pub table: String,
    /// Column name referenced by the predicate.
    pub column: String,
    /// The comparison operator.
    pub op: BinaryOp,
    /// The literal value being compared (if any).
    pub value: Option<Value>,
    /// Whether this is a range predicate (can use index range scan).
    pub is_range: bool,
    /// Whether this is a point lookup (can use index get).
    pub is_point_lookup: bool,
}

/// Merged range bounds for a single column.
/// Used when multiple range predicates on the same column can be combined.
#[derive(Debug, Clone)]
struct MergedRange {
    /// Lower bound value (None means -∞)
    lower_bound: Option<Value>,
    /// Whether the lower bound is inclusive (>=) or exclusive (>)
    lower_inclusive: bool,
    /// Upper bound value (None means +∞)
    upper_bound: Option<Value>,
    /// Whether the upper bound is inclusive (<=) or exclusive (<)
    upper_inclusive: bool,
}

impl MergedRange {
    /// Creates a new unbounded range (-∞, +∞)
    fn new() -> Self {
        Self {
            lower_bound: None,
            lower_inclusive: true,
            upper_bound: None,
            upper_inclusive: true,
        }
    }

    /// Updates the lower bound with a new constraint.
    /// Takes the more restrictive (larger) lower bound.
    fn update_lower(&mut self, value: Value, inclusive: bool) {
        match &self.lower_bound {
            None => {
                self.lower_bound = Some(value);
                self.lower_inclusive = inclusive;
            }
            Some(existing) => {
                use core::cmp::Ordering;
                match value.cmp(existing) {
                    Ordering::Greater => {
                        // New value is larger, use it
                        self.lower_bound = Some(value);
                        self.lower_inclusive = inclusive;
                    }
                    Ordering::Equal => {
                        // Same value: exclusive (>) is more restrictive than inclusive (>=)
                        if !inclusive {
                            self.lower_inclusive = false;
                        }
                    }
                    Ordering::Less => {
                        // Existing is larger, keep it
                    }
                }
            }
        }
    }

    /// Updates the upper bound with a new constraint.
    /// Takes the more restrictive (smaller) upper bound.
    fn update_upper(&mut self, value: Value, inclusive: bool) {
        match &self.upper_bound {
            None => {
                self.upper_bound = Some(value);
                self.upper_inclusive = inclusive;
            }
            Some(existing) => {
                use core::cmp::Ordering;
                match value.cmp(existing) {
                    Ordering::Less => {
                        // New value is smaller, use it
                        self.upper_bound = Some(value);
                        self.upper_inclusive = inclusive;
                    }
                    Ordering::Equal => {
                        // Same value: exclusive (<) is more restrictive than inclusive (<=)
                        if !inclusive {
                            self.upper_inclusive = false;
                        }
                    }
                    Ordering::Greater => {
                        // Existing is smaller, keep it
                    }
                }
            }
        }
    }

    /// Converts to IndexScan range parameters.
    fn to_range_params(self) -> (Option<Value>, Option<Value>, bool, bool) {
        (
            self.lower_bound,
            self.upper_bound,
            self.lower_inclusive,
            self.upper_inclusive,
        )
    }
}

/// Information extracted from an IN predicate for index selection.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct InPredicateInfo {
    /// Table name referenced by the predicate.
    pub table: String,
    /// Column name referenced by the predicate.
    pub column: String,
    /// The literal values in the IN list.
    pub values: Vec<Value>,
}

/// Information about a GIN-indexable predicate.
#[derive(Debug, Clone)]
struct GinPredicateInfo {
    index: String,
    column: String,
    column_index: usize,
    path: String,
    value: Option<Value>,
    prefilter_pairs: Option<Vec<(String, Value)>>,
    match_all: bool,
    query_type: String,
    original_predicate: Expr,
    requires_post_filter: bool,
    supports_multi_scan: bool,
}

impl IndexSelection {
    fn select_indexes(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            // Look for Filter on Scan patterns that could use an index
            LogicalPlan::Filter { input, predicate } => {
                let optimized_input = self.select_indexes(*input);

                // Try to convert Filter(Scan) to IndexScan
                if let LogicalPlan::Scan { ref table } = optimized_input {
                    if let Some(index_plan) =
                        self.try_use_index(table, &predicate, optimized_input.clone())
                    {
                        return index_plan;
                    }
                }

                LogicalPlan::Filter {
                    input: Box::new(optimized_input),
                    predicate,
                }
            }

            LogicalPlan::Project { input, columns } => LogicalPlan::Project {
                input: Box::new(self.select_indexes(*input)),
                columns,
            },

            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type,
                output_tables,
            } => LogicalPlan::Join {
                left: Box::new(self.select_indexes(*left)),
                right: Box::new(self.select_indexes(*right)),
                condition,
                join_type,
                output_tables,
            },

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => LogicalPlan::Aggregate {
                input: Box::new(self.select_indexes(*input)),
                group_by,
                aggregates,
            },

            LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
                input: Box::new(self.select_indexes(*input)),
                order_by,
            },

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(self.select_indexes(*input)),
                limit,
                offset,
            },

            LogicalPlan::CrossProduct { left, right } => LogicalPlan::CrossProduct {
                left: Box::new(self.select_indexes(*left)),
                right: Box::new(self.select_indexes(*right)),
            },

            LogicalPlan::Union { left, right, all } => LogicalPlan::Union {
                left: Box::new(self.select_indexes(*left)),
                right: Box::new(self.select_indexes(*right)),
                all,
            },

            // Leaf nodes
            LogicalPlan::Scan { .. }
            | LogicalPlan::IndexScan { .. }
            | LogicalPlan::IndexGet { .. }
            | LogicalPlan::IndexInGet { .. }
            | LogicalPlan::GinIndexScan { .. }
            | LogicalPlan::GinIndexScanMulti { .. }
            | LogicalPlan::Empty => plan,
        }
    }

    /// Attempts to use an index for the given predicate.
    fn try_use_index(
        &self,
        table: &str,
        predicate: &Expr,
        _original: LogicalPlan,
    ) -> Option<LogicalPlan> {
        // Check if we have context with index information
        let ctx = self.context.as_ref()?;

        // For small restricted subsets, preserving a table scan lets the restricted
        // datasource iterate only the affected row ids and recheck predicates in-place.
        if ctx.is_restricted_relation(table)
            && ctx.restricted_access_mode(table) == RestrictedAccessMode::SubsetDriven
        {
            return None;
        }

        // First, try to handle IN predicates and equivalent OR chains.
        if let Some(in_info) = self
            .analyze_in_predicate(predicate)
            .or_else(|| self.analyze_or_in_predicate(predicate))
        {
            // Find an index that covers the IN column
            if let Some(index) = ctx.find_index(table, &[in_info.column.as_str()]) {
                // Use IndexInGet for IN queries with indexed columns
                if index.supports_point_lookup() {
                    return Some(LogicalPlan::IndexInGet {
                        table: table.into(),
                        index: index.name.clone(),
                        keys: in_info.values,
                    });
                }
            }
        }

        // Try to handle BETWEEN predicates
        if let Some(between_plan) = self.try_use_between_index(table, predicate, ctx) {
            return Some(between_plan);
        }

        // Then, try to use GIN index for JSONB function queries
        if let Some(gin_plan) = self.try_use_gin_index(table, predicate, ctx) {
            return Some(gin_plan);
        }

        // Then, try composite tuple bounds for multi-column B-Tree indexes
        if let Some(composite_plan) = self.try_use_composite_btree_with_and(table, predicate, ctx) {
            return Some(composite_plan);
        }

        // Try to handle AND compound predicates for B-Tree indexes
        if let Some(btree_plan) = self.try_use_btree_with_and(table, predicate, ctx) {
            return Some(btree_plan);
        }

        // Extract predicate information for B-Tree index (simple predicate)
        let pred_info = self.analyze_predicate(predicate)?;

        // Find an index that covers the predicate column
        let index = ctx.find_index(table, &[pred_info.column.as_str()])?;

        // Skip GIN indexes for regular predicates
        if index.is_gin() {
            return None;
        }

        // Decide whether to use IndexScan or IndexGet based on predicate type
        if pred_info.is_point_lookup && index.supports_point_lookup() {
            // Use IndexGet for equality lookups
            if let Some(value) = pred_info.value {
                return Some(LogicalPlan::IndexGet {
                    table: table.into(),
                    index: index.name.clone(),
                    key: value,
                });
            }
        } else if pred_info.is_range && index.supports_range() {
            // Use IndexScan for range predicates
            let (range_start, range_end, include_start, include_end) =
                self.compute_range(&pred_info);
            return Some(LogicalPlan::IndexScan {
                table: table.into(),
                index: index.name.clone(),
                bounds: IndexBounds::from_scalar_range(
                    range_start,
                    range_end,
                    include_start,
                    include_end,
                ),
            });
        }

        None
    }

    /// Attempts to use an index for BETWEEN predicates.
    fn try_use_between_index(
        &self,
        table: &str,
        predicate: &Expr,
        ctx: &ExecutionContext,
    ) -> Option<LogicalPlan> {
        if let Expr::Between { expr, low, high } = predicate {
            // Check if expr is a column reference
            if let Expr::Column(col) = expr.as_ref() {
                // Check if low and high are literals
                if let (Expr::Literal(low_val), Expr::Literal(high_val)) =
                    (low.as_ref(), high.as_ref())
                {
                    // Find an index for this column
                    if let Some(index) = ctx.find_index(table, &[col.column.as_str()]) {
                        if !index.supports_range() {
                            return None;
                        }
                        // Use IndexScan for BETWEEN
                        return Some(LogicalPlan::IndexScan {
                            table: table.into(),
                            index: index.name.clone(),
                            bounds: IndexBounds::Scalar(KeyRange::bound(
                                low_val.clone(),
                                high_val.clone(),
                                false,
                                false,
                            )),
                        });
                    }
                }
            }
        }
        None
    }

    /// Attempts to use a composite B-Tree index for AND predicates.
    ///
    /// This is intentionally conservative: it only generates tuple bounds when
    /// every indexed column is constrained, with equality on the leading
    /// columns and equality or a fully-bounded range on the final column.
    fn try_use_composite_btree_with_and(
        &self,
        table: &str,
        predicate: &Expr,
        ctx: &ExecutionContext,
    ) -> Option<LogicalPlan> {
        if !matches!(
            predicate,
            Expr::BinaryOp {
                op: BinaryOp::And,
                ..
            }
        ) {
            return None;
        }

        let predicates = self.flatten_and_predicates(predicate);
        let analyzed: Vec<(Expr, PredicateInfo)> = predicates
            .iter()
            .filter_map(|expr| {
                let info = self.analyze_predicate(expr)?;
                if info.table == table && info.value.is_some() {
                    Some((expr.clone(), info))
                } else {
                    None
                }
            })
            .collect();

        let stats = ctx.get_stats(table)?;
        let mut best: Option<(usize, bool, LogicalPlan, Vec<Expr>)> = None;

        for index in &stats.indexes {
            if !index.supports_range() || index.columns.len() <= 1 {
                continue;
            }

            let Some((bounds, used_predicates, is_point_lookup)) =
                self.build_composite_bounds_for_index(&index.columns, &analyzed)
            else {
                continue;
            };

            let index_plan = LogicalPlan::IndexScan {
                table: table.into(),
                index: index.name.clone(),
                bounds,
            };

            let mut remaining = Vec::new();
            for expr in &predicates {
                if !used_predicates.iter().any(|used| Self::expr_eq(used, expr)) {
                    remaining.push(expr.clone());
                }
            }

            let candidate = (
                used_predicates.len(),
                is_point_lookup,
                self.wrap_with_filter_if_needed(index_plan, remaining),
                used_predicates,
            );

            let replace = match &best {
                None => true,
                Some((best_used, best_point, _, _)) => {
                    candidate.0 > *best_used
                        || (candidate.0 == *best_used && candidate.1 && !*best_point)
                }
            };

            if replace {
                best = Some(candidate);
            }
        }

        best.map(|(_, _, plan, _)| plan)
    }

    /// Attempts to use a B-Tree index for AND compound predicates.
    /// Extracts sub-predicates from AND, merges range predicates on the same column,
    /// converts to IndexScan/IndexGet, and keeps remaining predicates as Filter.
    fn try_use_btree_with_and(
        &self,
        table: &str,
        predicate: &Expr,
        ctx: &ExecutionContext,
    ) -> Option<LogicalPlan> {
        // Only handle AND compound predicates
        if !matches!(
            predicate,
            Expr::BinaryOp {
                op: BinaryOp::And,
                ..
            }
        ) {
            return None;
        }

        // Extract all sub-predicates from AND
        let (indexable, remaining) =
            self.extract_btree_and_remaining_predicates(predicate, table, ctx);

        if indexable.is_empty() {
            return None;
        }

        // Try to find a point lookup first (highest priority)
        for (pred, info, index) in &indexable {
            if info.is_point_lookup && info.value.is_some() {
                // Build IndexGet plan
                let index_plan = LogicalPlan::IndexGet {
                    table: table.into(),
                    index: index.name.clone(),
                    key: info.value.clone()?,
                };

                // Collect remaining predicates (non-indexable + other indexable)
                let mut all_remaining: Vec<Expr> = remaining;
                for (other_pred, _, _) in &indexable {
                    if !Self::expr_eq(other_pred, pred) {
                        all_remaining.push(other_pred.clone());
                    }
                }

                return Some(self.wrap_with_filter_if_needed(index_plan, all_remaining));
            }
        }

        // No point lookup found, try to merge range predicates on the same column
        // Group range predicates by (column, index)
        let merged = self.merge_range_predicates_by_column(&indexable);

        if merged.is_empty() {
            return None;
        }

        // Select the best merged range (prefer ranges with both bounds)
        let (best_column, best_index, best_range, used_predicates) =
            self.select_best_merged_range(&merged)?;

        // Build IndexScan with merged range
        let (range_start, range_end, include_start, include_end) = best_range.to_range_params();
        let index_plan = LogicalPlan::IndexScan {
            table: table.into(),
            index: best_index.name.clone(),
            bounds: IndexBounds::from_scalar_range(
                range_start,
                range_end,
                include_start,
                include_end,
            ),
        };

        // Collect remaining predicates:
        // - All non-indexable predicates
        // - Indexable predicates on other columns
        // - Indexable predicates on the same column but not used in the merge
        let mut all_remaining: Vec<Expr> = remaining;
        for (pred, info, _) in &indexable {
            // Skip predicates that were used in the merged range
            if info.column == best_column && used_predicates.iter().any(|p| Self::expr_eq(p, pred))
            {
                continue;
            }
            all_remaining.push(pred.clone());
        }

        Some(self.wrap_with_filter_if_needed(index_plan, all_remaining))
    }

    /// Wraps an index plan with a Filter if there are remaining predicates.
    fn wrap_with_filter_if_needed(
        &self,
        index_plan: LogicalPlan,
        remaining: Vec<Expr>,
    ) -> LogicalPlan {
        if remaining.is_empty() {
            index_plan
        } else {
            let combined_predicate = remaining
                .into_iter()
                .reduce(|acc, pred| Expr::and(acc, pred))
                .unwrap();

            LogicalPlan::Filter {
                input: Box::new(index_plan),
                predicate: combined_predicate,
            }
        }
    }

    /// Groups range predicates by column and merges them into combined ranges.
    /// Returns a map of (column_name, index, merged_range, used_predicates).
    fn merge_range_predicates_by_column(
        &self,
        indexable: &[(Expr, PredicateInfo, IndexInfo)],
    ) -> Vec<(String, IndexInfo, MergedRange, Vec<Expr>)> {
        use hashbrown::HashMap;

        // Group by column name
        let mut by_column: HashMap<String, Vec<(Expr, PredicateInfo, IndexInfo)>> = HashMap::new();
        for (pred, info, index) in indexable {
            if info.is_range && info.value.is_some() {
                by_column.entry(info.column.clone()).or_default().push((
                    pred.clone(),
                    info.clone(),
                    index.clone(),
                ));
            }
        }

        // Merge ranges for each column
        let mut result = Vec::new();
        for (column, predicates) in by_column {
            if predicates.is_empty() {
                continue;
            }

            // All predicates on the same column should use the same index
            let index = predicates[0].2.clone();

            let mut merged = MergedRange::new();
            let mut used_preds = Vec::new();

            for (pred, info, _) in &predicates {
                if let Some(value) = &info.value {
                    match info.op {
                        BinaryOp::Gt => {
                            merged.update_lower(value.clone(), false);
                            used_preds.push(pred.clone());
                        }
                        BinaryOp::Ge => {
                            merged.update_lower(value.clone(), true);
                            used_preds.push(pred.clone());
                        }
                        BinaryOp::Lt => {
                            merged.update_upper(value.clone(), false);
                            used_preds.push(pred.clone());
                        }
                        BinaryOp::Le => {
                            merged.update_upper(value.clone(), true);
                            used_preds.push(pred.clone());
                        }
                        _ => {}
                    }
                }
            }

            if !used_preds.is_empty() {
                result.push((column, index, merged, used_preds));
            }
        }

        result
    }

    /// Selects the best merged range for index usage.
    /// Priority: ranges with both bounds > ranges with only lower bound > ranges with only upper bound
    fn select_best_merged_range(
        &self,
        merged: &[(String, IndexInfo, MergedRange, Vec<Expr>)],
    ) -> Option<(String, IndexInfo, MergedRange, Vec<Expr>)> {
        // Priority 1: Ranges with both bounds (most selective)
        for (col, idx, range, preds) in merged {
            if range.lower_bound.is_some() && range.upper_bound.is_some() {
                return Some((col.clone(), idx.clone(), range.clone(), preds.clone()));
            }
        }

        // Priority 2: Ranges with lower bound only (> or >=)
        for (col, idx, range, preds) in merged {
            if range.lower_bound.is_some() {
                return Some((col.clone(), idx.clone(), range.clone(), preds.clone()));
            }
        }

        // Priority 3: Ranges with upper bound only (< or <=)
        for (col, idx, range, preds) in merged {
            if range.upper_bound.is_some() {
                return Some((col.clone(), idx.clone(), range.clone(), preds.clone()));
            }
        }

        None
    }

    /// Extracts B-Tree indexable predicates and remaining predicates from an AND expression.
    fn extract_btree_and_remaining_predicates(
        &self,
        predicate: &Expr,
        table: &str,
        ctx: &ExecutionContext,
    ) -> (Vec<(Expr, PredicateInfo, IndexInfo)>, Vec<Expr>) {
        let mut indexable = Vec::new();
        let mut remaining = Vec::new();
        self.extract_btree_and_remaining_recursive(
            predicate,
            table,
            ctx,
            &mut indexable,
            &mut remaining,
        );
        (indexable, remaining)
    }

    fn extract_btree_and_remaining_recursive(
        &self,
        predicate: &Expr,
        table: &str,
        ctx: &ExecutionContext,
        indexable: &mut Vec<(Expr, PredicateInfo, IndexInfo)>,
        remaining: &mut Vec<Expr>,
    ) {
        match predicate {
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                self.extract_btree_and_remaining_recursive(left, table, ctx, indexable, remaining);
                self.extract_btree_and_remaining_recursive(right, table, ctx, indexable, remaining);
            }
            _ => {
                // Try to analyze as a simple predicate
                if let Some(pred_info) = self.analyze_predicate(predicate) {
                    // Check if there's a B-Tree index for this column
                    if let Some(index) = ctx.find_index(table, &[pred_info.column.as_str()]) {
                        let supports_predicate = (pred_info.is_point_lookup
                            && index.supports_point_lookup())
                            || (pred_info.is_range && index.supports_range());
                        if supports_predicate {
                            indexable.push((predicate.clone(), pred_info, index.clone()));
                            return;
                        }
                    }
                }
                // Not indexable - add to remaining
                remaining.push(predicate.clone());
            }
        }
    }

    fn flatten_and_predicates(&self, predicate: &Expr) -> Vec<Expr> {
        let mut predicates = Vec::new();
        Self::flatten_and_predicates_into(predicate, &mut predicates);
        predicates
    }

    fn flatten_and_predicates_into(predicate: &Expr, predicates: &mut Vec<Expr>) {
        match predicate {
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                Self::flatten_and_predicates_into(left, predicates);
                Self::flatten_and_predicates_into(right, predicates);
            }
            _ => predicates.push(predicate.clone()),
        }
    }

    fn flatten_or_predicates(&self, predicate: &Expr) -> Vec<Expr> {
        let mut predicates = Vec::new();
        Self::flatten_or_predicates_into(predicate, &mut predicates);
        predicates
    }

    fn flatten_or_predicates_into(predicate: &Expr, predicates: &mut Vec<Expr>) {
        match predicate {
            Expr::BinaryOp {
                left,
                op: BinaryOp::Or,
                right,
            } => {
                Self::flatten_or_predicates_into(left, predicates);
                Self::flatten_or_predicates_into(right, predicates);
            }
            _ => predicates.push(predicate.clone()),
        }
    }

    fn build_composite_bounds_for_index(
        &self,
        index_columns: &[String],
        analyzed: &[(Expr, PredicateInfo)],
    ) -> Option<(IndexBounds, Vec<Expr>, bool)> {
        let mut prefix_values = Vec::with_capacity(index_columns.len());
        let mut used_predicates = Vec::new();

        for column in &index_columns[..index_columns.len().saturating_sub(1)] {
            let (expr, info) = analyzed.iter().find(|(_, info)| {
                info.column == *column && info.is_point_lookup && info.value.is_some()
            })?;
            prefix_values.push(info.value.clone()?);
            used_predicates.push(expr.clone());
        }

        let last_column = index_columns.last()?;
        if let Some((expr, info)) = analyzed.iter().find(|(_, info)| {
            info.column == *last_column && info.is_point_lookup && info.value.is_some()
        }) {
            let mut values = prefix_values;
            values.push(info.value.clone()?);
            used_predicates.push(expr.clone());
            return Some((
                IndexBounds::Composite(KeyRange::only(values)),
                used_predicates,
                true,
            ));
        }

        let mut range = MergedRange::new();
        let mut range_predicates = Vec::new();
        for (expr, info) in analyzed.iter().filter(|(_, info)| {
            info.column == *last_column && info.is_range && info.value.is_some()
        }) {
            let value = info.value.clone()?;
            match info.op {
                BinaryOp::Gt => {
                    range.update_lower(value, false);
                    range_predicates.push(expr.clone());
                }
                BinaryOp::Ge => {
                    range.update_lower(value, true);
                    range_predicates.push(expr.clone());
                }
                BinaryOp::Lt => {
                    range.update_upper(value, false);
                    range_predicates.push(expr.clone());
                }
                BinaryOp::Le => {
                    range.update_upper(value, true);
                    range_predicates.push(expr.clone());
                }
                _ => {}
            }
        }

        let lower = range.lower_bound?;
        let upper = range.upper_bound?;
        let lower_inclusive = range.lower_inclusive;
        let upper_inclusive = range.upper_inclusive;

        let mut lower_values = prefix_values.clone();
        lower_values.push(lower);
        let mut upper_values = prefix_values;
        upper_values.push(upper);

        used_predicates.extend(range_predicates);

        Some((
            IndexBounds::Composite(KeyRange::bound(
                lower_values,
                upper_values,
                !lower_inclusive,
                !upper_inclusive,
            )),
            used_predicates,
            false,
        ))
    }

    /// Selects the best B-Tree predicate for index usage.
    /// Priority: point lookup (IndexGet) > range scan (IndexScan)
    ///
    /// Note: This function is kept for potential future use but is currently
    /// superseded by `merge_range_predicates_by_column` + `select_best_merged_range`
    /// which can merge multiple range predicates on the same column.
    #[allow(dead_code)]
    fn select_best_btree_predicate(
        &self,
        indexable: &[(Expr, PredicateInfo, IndexInfo)],
    ) -> Option<(Expr, PredicateInfo, IndexInfo)> {
        // First, try to find a point lookup (equality)
        for (pred, info, index) in indexable {
            if info.is_point_lookup && info.value.is_some() {
                return Some((pred.clone(), info.clone(), index.clone()));
            }
        }

        // Fall back to range scan
        for (pred, info, index) in indexable {
            if info.is_range && info.value.is_some() {
                return Some((pred.clone(), info.clone(), index.clone()));
            }
        }

        None
    }

    /// Simple expression equality check for filtering out the selected predicate.
    fn expr_eq(a: &Expr, b: &Expr) -> bool {
        // Use debug representation for simple equality check
        format!("{:?}", a) == format!("{:?}", b)
    }

    /// Analyzes an IN predicate to extract index-relevant information.
    fn analyze_in_predicate(&self, predicate: &Expr) -> Option<InPredicateInfo> {
        match predicate {
            Expr::In { expr, list } => {
                // Check if expr is a column reference
                if let Expr::Column(col) = expr.as_ref() {
                    // Extract all literal values from the list
                    let values: Vec<Value> = list
                        .iter()
                        .filter_map(|item| {
                            if let Expr::Literal(val) = item {
                                Some(val.clone())
                            } else {
                                None
                            }
                        })
                        .collect();

                    // Only use index if all values are literals
                    if values.len() == list.len() && !values.is_empty() {
                        return Some(InPredicateInfo {
                            table: col.table.clone(),
                            column: col.column.clone(),
                            values,
                        });
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Analyzes an OR-of-equality predicate that is equivalent to IN.
    fn analyze_or_in_predicate(&self, predicate: &Expr) -> Option<InPredicateInfo> {
        let predicates = self.flatten_or_predicates(predicate);
        if predicates.len() < 2 {
            return None;
        }

        let mut table = None;
        let mut column = None;
        let mut values = Vec::with_capacity(predicates.len());

        for predicate in predicates {
            let info = self.analyze_predicate(&predicate)?;
            if !info.is_point_lookup {
                return None;
            }

            let value = info.value?;
            match (&table, &column) {
                (None, None) => {
                    table = Some(info.table.clone());
                    column = Some(info.column.clone());
                }
                (Some(existing_table), Some(existing_column))
                    if *existing_table == info.table && *existing_column == info.column => {}
                _ => return None,
            }

            if !values.iter().any(|existing| existing == &value) {
                values.push(value);
            }
        }

        Some(InPredicateInfo {
            table: table?,
            column: column?,
            values,
        })
    }

    /// Attempts to use a GIN index for JSONB function queries.
    /// Supports both single predicates and AND combinations of multiple predicates.
    ///
    /// Returns a tuple of (GIN plan, remaining predicates that couldn't be handled by GIN).
    /// The remaining predicates should be wrapped as a Filter around the GIN plan.
    fn try_use_gin_index(
        &self,
        table: &str,
        predicate: &Expr,
        ctx: &ExecutionContext,
    ) -> Option<LogicalPlan> {
        // Extract GIN predicates and collect remaining non-GIN predicates
        let (gin_predicates, mut remaining_predicates) =
            self.extract_gin_and_remaining_predicates(predicate, table, ctx);

        if gin_predicates.is_empty() {
            return None;
        }

        let mut used_predicates = alloc::vec![false; gin_predicates.len()];

        let gin_plan = if let Some(group) = self.choose_best_multi_gin_group(&gin_predicates) {
            for &idx in &group {
                used_predicates[idx] = true;
            }

            let first = &gin_predicates[group[0]];
            let pairs = group
                .iter()
                .map(|&idx| {
                    let info = &gin_predicates[idx];
                    (
                        info.path.clone(),
                        info.value
                            .clone()
                            .expect("multi GIN scan only uses predicates with literal values"),
                    )
                })
                .collect();

            LogicalPlan::GinIndexScanMulti {
                table: table.into(),
                index: first.index.clone(),
                column: first.column.clone(),
                pairs,
                match_all: true,
                recheck: None,
            }
        } else {
            let best_idx = self.choose_best_single_gin_predicate(table, &gin_predicates, ctx)?;
            used_predicates[best_idx] = true;
            let info = &gin_predicates[best_idx];
            if let Some(pairs) = &info.prefilter_pairs {
                LogicalPlan::GinIndexScanMulti {
                    table: table.into(),
                    index: info.index.clone(),
                    column: info.column.clone(),
                    pairs: pairs.clone(),
                    match_all: info.match_all,
                    recheck: None,
                }
            } else {
                LogicalPlan::GinIndexScan {
                    table: table.into(),
                    index: info.index.clone(),
                    column: info.column.clone(),
                    column_index: info.column_index,
                    path: info.path.clone(),
                    value: info.value.clone(),
                    query_type: info.query_type.clone(),
                    recheck: None,
                }
            }
        };

        let recheck = gin_predicates
            .iter()
            .enumerate()
            .filter(|(idx, info)| used_predicates[*idx] && !info.requires_post_filter)
            .map(|(_, info)| info.original_predicate.clone())
            .reduce(Expr::and);

        let gin_plan = match gin_plan {
            LogicalPlan::GinIndexScan {
                table,
                index,
                column,
                column_index,
                path,
                value,
                query_type,
                ..
            } => LogicalPlan::GinIndexScan {
                table,
                index,
                column,
                column_index,
                path,
                value,
                query_type,
                recheck,
            },
            LogicalPlan::GinIndexScanMulti {
                table,
                index,
                column,
                pairs,
                match_all,
                ..
            } => LogicalPlan::GinIndexScanMulti {
                table,
                index,
                column,
                pairs,
                match_all,
                recheck,
            },
            _ => unreachable!("GIN selection must produce a GIN scan"),
        };

        for (idx, info) in gin_predicates.into_iter().enumerate() {
            if !used_predicates[idx] || info.requires_post_filter {
                remaining_predicates.push(info.original_predicate);
            }
        }

        // If there are remaining predicates, wrap the GIN plan with a Filter
        if remaining_predicates.is_empty() {
            Some(gin_plan)
        } else {
            // Combine remaining predicates with AND
            let combined_predicate = remaining_predicates
                .into_iter()
                .reduce(|acc, pred| Expr::and(acc, pred))
                .unwrap();

            Some(LogicalPlan::Filter {
                input: Box::new(gin_plan),
                predicate: combined_predicate,
            })
        }
    }

    fn choose_best_multi_gin_group(
        &self,
        gin_predicates: &[GinPredicateInfo],
    ) -> Option<Vec<usize>> {
        let mut best_group = Vec::new();

        for (idx, candidate) in gin_predicates.iter().enumerate() {
            if !candidate.supports_multi_scan {
                continue;
            }

            let mut group = alloc::vec![idx];
            for (other_idx, other) in gin_predicates.iter().enumerate().skip(idx + 1) {
                if other.supports_multi_scan
                    && other.index == candidate.index
                    && other.column == candidate.column
                {
                    group.push(other_idx);
                }
            }

            if group.len() > best_group.len() {
                best_group = group;
            }
        }

        if best_group.len() > 1 {
            Some(best_group)
        } else {
            None
        }
    }

    fn choose_best_single_gin_predicate(
        &self,
        table: &str,
        gin_predicates: &[GinPredicateInfo],
        ctx: &ExecutionContext,
    ) -> Option<usize> {
        gin_predicates
            .iter()
            .enumerate()
            .min_by_key(|(_, info)| {
                (
                    Self::gin_predicate_cost(table, info, ctx),
                    u8::MAX - Self::gin_single_predicate_rank(info),
                )
            })
            .map(|(idx, _)| idx)
    }

    fn gin_predicate_cost(table: &str, info: &GinPredicateInfo, ctx: &ExecutionContext) -> usize {
        if let Some(value) = &info.value {
            if let Some(literal) = Self::literal_to_gin_cost_value(value) {
                if let Some(cost) =
                    ctx.gin_key_value_cost(table, &info.index, &info.path, literal.as_str())
                {
                    if cost > 0 {
                        return cost;
                    }
                }
            }
        }

        if let Some(pairs) = &info.prefilter_pairs {
            if let Some(cost) = pairs
                .iter()
                .filter_map(|(key, value)| {
                    let literal = Self::literal_to_gin_cost_value(value)?;
                    ctx.gin_key_value_cost(table, &info.index, key, literal.as_str())
                })
                .min()
            {
                if cost > 0 {
                    return cost;
                }
            }
        }

        if let Some(cost) = ctx.gin_key_cost(table, &info.index, &info.path) {
            if cost > 0 {
                return cost;
            }
        }

        match info.query_type.as_str() {
            "eq" => 8,
            "exists" => 64,
            "contains" => 256,
            _ => 1024,
        }
    }

    fn literal_to_gin_cost_value(value: &Value) -> Option<String> {
        match value {
            Value::String(value) => Some(value.clone()),
            Value::Int32(value) => Some(value.to_string()),
            Value::Int64(value) => Some(value.to_string()),
            Value::Float64(value) => Some(value.to_string()),
            Value::Boolean(value) => Some(if *value { "true" } else { "false" }.into()),
            _ => None,
        }
    }

    fn gin_single_predicate_rank(info: &GinPredicateInfo) -> u8 {
        if info.query_type == "eq" && !info.requires_post_filter {
            4
        } else if info.prefilter_pairs.is_some() {
            3
        } else if info.query_type == "exists" && !info.requires_post_filter {
            2
        } else if info.query_type == "contains" {
            1
        } else {
            0
        }
    }

    /// Extracts GIN-indexable predicates and remaining non-GIN predicates from an expression.
    /// Returns (gin_predicates, remaining_predicates).
    fn extract_gin_and_remaining_predicates(
        &self,
        predicate: &Expr,
        table: &str,
        ctx: &ExecutionContext,
    ) -> (Vec<GinPredicateInfo>, Vec<Expr>) {
        let mut gin_predicates = Vec::new();
        let mut remaining_predicates = Vec::new();
        self.extract_gin_and_remaining_recursive(
            predicate,
            table,
            ctx,
            &mut gin_predicates,
            &mut remaining_predicates,
        );
        (gin_predicates, remaining_predicates)
    }

    fn extract_gin_and_remaining_recursive(
        &self,
        predicate: &Expr,
        table: &str,
        ctx: &ExecutionContext,
        gin_result: &mut Vec<GinPredicateInfo>,
        remaining_result: &mut Vec<Expr>,
    ) {
        match predicate {
            // Handle AND combinations - recursively process both sides
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                self.extract_gin_and_remaining_recursive(
                    left,
                    table,
                    ctx,
                    gin_result,
                    remaining_result,
                );
                self.extract_gin_and_remaining_recursive(
                    right,
                    table,
                    ctx,
                    gin_result,
                    remaining_result,
                );
            }
            Expr::BinaryOp {
                op: BinaryOp::Or, ..
            } => {
                if let Some(info) = self.analyze_gin_or_predicate(predicate, table, ctx) {
                    gin_result.push(info);
                } else {
                    remaining_result.push(predicate.clone());
                }
            }
            // Handle JSONB function calls - these can potentially use GIN index
            Expr::Function { name, args } => {
                if let Some(info) = self.analyze_gin_function(predicate, name, args, table, ctx) {
                    gin_result.push(info);
                } else {
                    // Function that doesn't match GIN pattern - keep as remaining
                    remaining_result.push(predicate.clone());
                }
            }
            // All other predicates (BinaryOp with Eq/Lt/Gt, etc.) are non-GIN
            _ => {
                remaining_result.push(predicate.clone());
            }
        }
    }

    /// Analyzes a JSONB function call to extract GIN predicate information.
    fn analyze_gin_function(
        &self,
        predicate: &Expr,
        name: &str,
        args: &[Expr],
        table: &str,
        ctx: &ExecutionContext,
    ) -> Option<GinPredicateInfo> {
        let func_name = name.to_uppercase();
        match func_name.as_str() {
            "JSONB_PATH_EQ" if args.len() >= 3 => {
                if let Expr::Column(col) = &args[0] {
                    let column_name = &col.column;
                    let column_index = col.index;
                    let path = self.normalize_gin_path(&self.extract_string_literal(&args[1])?)?;
                    if let Some(index) = ctx.find_gin_index_for_path(table, column_name, &path) {
                        let value = self.extract_literal(&args[2])?;
                        return Some(GinPredicateInfo {
                            index: index.name.clone(),
                            column: column_name.clone(),
                            column_index,
                            path,
                            value: Some(value),
                            prefilter_pairs: None,
                            match_all: true,
                            query_type: "eq".into(),
                            original_predicate: predicate.clone(),
                            requires_post_filter: false,
                            supports_multi_scan: true,
                        });
                    }
                }
            }
            "JSONB_CONTAINS" if args.len() >= 3 => {
                if let Expr::Column(col) = &args[0] {
                    let column_name = &col.column;
                    let column_index = col.index;
                    let path = self.normalize_gin_path(&self.extract_string_literal(&args[1])?)?;
                    if let Some(index) = ctx.find_gin_index_for_path(table, column_name, &path) {
                        let prefilter_pairs =
                            self.extract_string_literal(&args[2]).and_then(|needle| {
                                let pairs = contains_trigram_pairs(&path, &needle);
                                if pairs.is_empty() {
                                    None
                                } else {
                                    Some(
                                        pairs
                                            .into_iter()
                                            .map(|(key, gram)| (key, Value::String(gram)))
                                            .collect(),
                                    )
                                }
                            });
                        return Some(GinPredicateInfo {
                            index: index.name.clone(),
                            column: column_name.clone(),
                            column_index,
                            path,
                            value: None,
                            prefilter_pairs,
                            match_all: true,
                            query_type: "contains".into(),
                            original_predicate: predicate.clone(),
                            requires_post_filter: true,
                            supports_multi_scan: false,
                        });
                    }
                }
            }
            "JSONB_EXISTS" if args.len() >= 2 => {
                if let Expr::Column(col) = &args[0] {
                    let column_name = &col.column;
                    let column_index = col.index;
                    let path = self.normalize_gin_path(&self.extract_string_literal(&args[1])?)?;
                    if let Some(index) = ctx.find_gin_index_for_path(table, column_name, &path) {
                        return Some(GinPredicateInfo {
                            index: index.name.clone(),
                            column: column_name.clone(),
                            column_index,
                            path,
                            value: None,
                            prefilter_pairs: None,
                            match_all: true,
                            query_type: "exists".into(),
                            original_predicate: predicate.clone(),
                            requires_post_filter: false,
                            supports_multi_scan: false,
                        });
                    }
                }
            }
            _ => {}
        }
        None
    }

    fn analyze_gin_or_predicate(
        &self,
        predicate: &Expr,
        table: &str,
        ctx: &ExecutionContext,
    ) -> Option<GinPredicateInfo> {
        let terms = self.flatten_or_predicates(predicate);
        if terms.len() < 2 {
            return None;
        }

        let mut first: Option<GinPredicateInfo> = None;
        let mut pairs = Vec::with_capacity(terms.len());

        for term in terms {
            let Expr::Function { name, args } = &term else {
                return None;
            };
            let info = self.analyze_gin_function(&term, name, args, table, ctx)?;
            if info.query_type != "eq"
                || info.requires_post_filter
                || info.prefilter_pairs.is_some()
                || info.value.is_none()
            {
                return None;
            }

            if let Some(existing) = &first {
                if existing.index != info.index
                    || existing.column != info.column
                    || existing.column_index != info.column_index
                    || existing.path != info.path
                {
                    return None;
                }
            } else {
                first = Some(info.clone());
            }

            let pair = (info.path.clone(), info.value.expect("checked above"));
            if !pairs.iter().any(|existing| existing == &pair) {
                pairs.push(pair);
            }
        }

        let first = first?;
        Some(GinPredicateInfo {
            index: first.index,
            column: first.column,
            column_index: first.column_index,
            path: first.path,
            value: None,
            prefilter_pairs: Some(pairs),
            match_all: false,
            query_type: "eq".into(),
            original_predicate: predicate.clone(),
            requires_post_filter: false,
            supports_multi_scan: false,
        })
    }

    fn normalize_gin_path(&self, path: &str) -> Option<String> {
        let parsed = JsonPath::parse(path).ok()?;
        let mut segments = Vec::new();
        if !Self::collect_gin_path_segments(&parsed, &mut segments) || segments.is_empty() {
            return None;
        }
        Some(Self::encode_gin_path_segments(&segments))
    }

    fn collect_gin_path_segments(path: &JsonPath, segments: &mut Vec<String>) -> bool {
        match path {
            JsonPath::Root => true,
            JsonPath::Field(parent, field) => {
                if !Self::collect_gin_path_segments(parent, segments) {
                    return false;
                }
                segments.push(field.clone());
                true
            }
            JsonPath::Index(parent, index) => {
                if !Self::collect_gin_path_segments(parent, segments) {
                    return false;
                }
                segments.push(index.to_string());
                true
            }
            JsonPath::Slice(_, _, _)
            | JsonPath::RecursiveField(_, _)
            | JsonPath::Wildcard(_)
            | JsonPath::Filter(_, _) => false,
        }
    }

    fn encode_gin_path_segments(segments: &[String]) -> String {
        let mut key = String::new();

        for segment in segments {
            if !key.is_empty() {
                key.push('.');
            }

            if segment.is_empty() {
                key.push('\\');
                key.push('0');
                continue;
            }

            for ch in segment.chars() {
                match ch {
                    '\\' => {
                        key.push('\\');
                        key.push('\\');
                    }
                    '.' => {
                        key.push('\\');
                        key.push('.');
                    }
                    _ => key.push(ch),
                }
            }
        }

        key
    }

    /// Extracts a string literal from an expression.
    fn extract_string_literal(&self, expr: &Expr) -> Option<String> {
        if let Expr::Literal(Value::String(s)) = expr {
            Some(s.clone())
        } else {
            None
        }
    }

    /// Extracts a literal value from an expression.
    fn extract_literal(&self, expr: &Expr) -> Option<Value> {
        if let Expr::Literal(v) = expr {
            Some(v.clone())
        } else {
            None
        }
    }

    /// Analyzes a predicate to extract index-relevant information.
    fn analyze_predicate(&self, predicate: &Expr) -> Option<PredicateInfo> {
        match predicate {
            Expr::BinaryOp { left, op, right } => {
                // Check for column = literal pattern
                if let (Expr::Column(col), Expr::Literal(val)) = (left.as_ref(), right.as_ref()) {
                    let is_point_lookup = *op == BinaryOp::Eq;
                    let is_range = matches!(
                        op,
                        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
                    );

                    return Some(PredicateInfo {
                        table: col.table.clone(),
                        column: col.column.clone(),
                        op: *op,
                        value: Some(val.clone()),
                        is_range,
                        is_point_lookup,
                    });
                }

                // Check for literal = column pattern (reversed)
                if let (Expr::Literal(val), Expr::Column(col)) = (left.as_ref(), right.as_ref()) {
                    let reversed_op = match op {
                        BinaryOp::Lt => BinaryOp::Gt,
                        BinaryOp::Le => BinaryOp::Ge,
                        BinaryOp::Gt => BinaryOp::Lt,
                        BinaryOp::Ge => BinaryOp::Le,
                        other => *other,
                    };
                    let is_point_lookup = *op == BinaryOp::Eq;
                    let is_range = matches!(
                        op,
                        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
                    );

                    return Some(PredicateInfo {
                        table: col.table.clone(),
                        column: col.column.clone(),
                        op: reversed_op,
                        value: Some(val.clone()),
                        is_range,
                        is_point_lookup,
                    });
                }

                None
            }
            _ => None,
        }
    }

    /// Computes the range bounds for an index scan.
    fn compute_range(
        &self,
        pred_info: &PredicateInfo,
    ) -> (Option<Value>, Option<Value>, bool, bool) {
        let value = pred_info.value.clone();

        match pred_info.op {
            BinaryOp::Eq => (value.clone(), value, true, true),
            BinaryOp::Lt => (None, value, true, false),
            BinaryOp::Le => (None, value, true, true),
            BinaryOp::Gt => (value, None, false, true),
            BinaryOp::Ge => (value, None, true, true),
            _ => (None, None, true, true),
        }
    }

    /// Extracts all simple predicates from a compound predicate.
    pub fn extract_predicates(&self, predicate: &Expr) -> Vec<PredicateInfo> {
        let mut result = Vec::new();
        self.extract_predicates_recursive(predicate, &mut result);
        result
    }

    fn extract_predicates_recursive(&self, predicate: &Expr, result: &mut Vec<PredicateInfo>) {
        match predicate {
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                self.extract_predicates_recursive(left, result);
                self.extract_predicates_recursive(right, result);
            }
            _ => {
                if let Some(info) = self.analyze_predicate(predicate) {
                    result.push(info);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{IndexInfo, QueryIndexType, TableStats};
    use alloc::collections::BTreeSet;

    fn test_contains_trigram_key(path: &str) -> String {
        alloc::format!("__cynos_contains3__:{path}")
    }

    fn test_contains_trigrams(value: &str) -> Vec<String> {
        let chars: Vec<char> = value.chars().collect();
        if chars.len() < 3 {
            return Vec::new();
        }

        let mut grams = BTreeSet::new();
        for window in chars.windows(3) {
            let gram: String = window.iter().collect();
            grams.insert(gram);
        }

        grams.into_iter().collect()
    }

    #[test]
    fn test_index_selection_basic() {
        let pass = IndexSelection::new();

        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(1i64)),
        );

        let optimized = pass.optimize(plan);
        // Without context, should remain unchanged
        assert!(matches!(optimized, LogicalPlan::Filter { .. }));
    }

    #[test]
    fn test_index_selection_with_context() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "users",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new("idx_id", alloc::vec!["id".into()], true)],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(1i64)),
        );

        let optimized = pass.optimize(plan);
        // With context and matching index, should convert to IndexGet
        assert!(matches!(optimized, LogicalPlan::IndexGet { .. }));
    }

    #[test]
    fn test_index_selection_range_scan() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "orders",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_amount",
                    alloc::vec!["amount".into()],
                    false
                )],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        let plan = LogicalPlan::filter(
            LogicalPlan::scan("orders"),
            Expr::gt(Expr::column("orders", "amount", 0), Expr::literal(100i64)),
        );

        let optimized = pass.optimize(plan);
        // Should convert to IndexScan for range predicate
        assert!(matches!(optimized, LogicalPlan::IndexScan { .. }));
    }

    #[test]
    fn test_hash_index_selection_uses_point_lookup() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "users",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![
                    IndexInfo::new("idx_id_hash", alloc::vec!["id".into()], true)
                        .with_type(QueryIndexType::Hash),
                ],
            },
        );

        let pass = IndexSelection::with_context(ctx);
        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(42i64)),
        );

        let optimized = pass.optimize(plan);
        assert!(matches!(optimized, LogicalPlan::IndexGet { .. }));
    }

    #[test]
    fn test_hash_index_selection_skips_range_scan() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "users",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![
                    IndexInfo::new("idx_id_hash", alloc::vec!["id".into()], true)
                        .with_type(QueryIndexType::Hash),
                ],
            },
        );

        let pass = IndexSelection::with_context(ctx);
        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::gt(Expr::column("users", "id", 0), Expr::literal(42i64)),
        );

        let optimized = pass.optimize(plan);
        assert!(matches!(optimized, LogicalPlan::Filter { .. }));
    }

    #[test]
    fn test_analyze_predicate() {
        let pass = IndexSelection::new();

        let pred = Expr::eq(Expr::column("users", "id", 0), Expr::literal(42i64));
        let info = pass.analyze_predicate(&pred).unwrap();

        assert_eq!(info.table, "users");
        assert_eq!(info.column, "id");
        assert!(info.is_point_lookup);
        assert!(!info.is_range);
    }

    #[test]
    fn test_analyze_range_predicate() {
        let pass = IndexSelection::new();

        let pred = Expr::gt(Expr::column("orders", "amount", 0), Expr::literal(100i64));
        let info = pass.analyze_predicate(&pred).unwrap();

        assert_eq!(info.table, "orders");
        assert_eq!(info.column, "amount");
        assert!(!info.is_point_lookup);
        assert!(info.is_range);
        assert_eq!(info.op, BinaryOp::Gt);
    }

    #[test]
    fn test_extract_compound_predicates() {
        let pass = IndexSelection::new();

        let pred = Expr::and(
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(1i64)),
            Expr::gt(Expr::column("users", "age", 1), Expr::literal(18i64)),
        );

        let predicates = pass.extract_predicates(&pred);
        assert_eq!(predicates.len(), 2);
        assert_eq!(predicates[0].column, "id");
        assert_eq!(predicates[1].column, "age");
    }

    #[test]
    fn test_no_index_available() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "users",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![], // No indexes
            },
        );

        let pass = IndexSelection::with_context(ctx);

        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::eq(Expr::column("users", "id", 0), Expr::literal(1i64)),
        );

        let optimized = pass.optimize(plan);
        // Without matching index, should remain as Filter
        assert!(matches!(optimized, LogicalPlan::Filter { .. }));
    }

    #[test]
    fn test_in_query_index_selection() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "users",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new("idx_id", alloc::vec!["id".into()], true)],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Filter: id IN (1, 2, 3)
        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::In {
                expr: Box::new(Expr::column("users", "id", 0)),
                list: alloc::vec![
                    Expr::literal(Value::Int64(1)),
                    Expr::literal(Value::Int64(2)),
                    Expr::literal(Value::Int64(3)),
                ],
            },
        );

        let optimized = pass.optimize(plan);
        // Should convert to IndexInGet for IN query with indexed column
        assert!(matches!(optimized, LogicalPlan::IndexInGet { .. }));

        if let LogicalPlan::IndexInGet { table, index, keys } = optimized {
            assert_eq!(table, "users");
            assert_eq!(index, "idx_id");
            assert_eq!(keys.len(), 3);
        }
    }

    #[test]
    fn test_in_query_no_index() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "users",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![], // No indexes
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Filter: id IN (1, 2, 3) but no index available
        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::In {
                expr: Box::new(Expr::column("users", "id", 0)),
                list: alloc::vec![
                    Expr::literal(Value::Int64(1)),
                    Expr::literal(Value::Int64(2)),
                ],
            },
        );

        let optimized = pass.optimize(plan);
        // Without index, should remain as Filter
        assert!(matches!(optimized, LogicalPlan::Filter { .. }));
    }

    #[test]
    fn test_analyze_in_predicate() {
        let pass = IndexSelection::new();

        let pred = Expr::In {
            expr: Box::new(Expr::column("users", "id", 0)),
            list: alloc::vec![
                Expr::literal(Value::Int64(1)),
                Expr::literal(Value::Int64(2)),
                Expr::literal(Value::Int64(3)),
            ],
        };

        let info = pass.analyze_in_predicate(&pred).unwrap();
        assert_eq!(info.table, "users");
        assert_eq!(info.column, "id");
        assert_eq!(info.values.len(), 3);
        assert_eq!(info.values[0], Value::Int64(1));
        assert_eq!(info.values[1], Value::Int64(2));
        assert_eq!(info.values[2], Value::Int64(3));
    }

    #[test]
    fn test_analyze_in_predicate_with_non_literals() {
        let pass = IndexSelection::new();

        // IN list with non-literal expression should not be optimized
        let pred = Expr::In {
            expr: Box::new(Expr::column("users", "id", 0)),
            list: alloc::vec![
                Expr::literal(Value::Int64(1)),
                Expr::column("other", "val", 0), // Non-literal
            ],
        };

        let info = pass.analyze_in_predicate(&pred);
        assert!(info.is_none());
    }

    #[test]
    fn test_or_equality_query_index_selection() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "users",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_status",
                    alloc::vec!["status".into()],
                    false
                )],
            },
        );

        let pass = IndexSelection::with_context(ctx);
        let plan = LogicalPlan::filter(
            LogicalPlan::scan("users"),
            Expr::or(
                Expr::eq(
                    Expr::column("users", "status", 1),
                    Expr::literal(Value::String("open".into())),
                ),
                Expr::eq(
                    Expr::column("users", "status", 1),
                    Expr::literal(Value::String("in_progress".into())),
                ),
            ),
        );

        let optimized = pass.optimize(plan);
        assert!(matches!(optimized, LogicalPlan::IndexInGet { .. }));

        if let LogicalPlan::IndexInGet { table, index, keys } = optimized {
            assert_eq!(table, "users");
            assert_eq!(index, "idx_status");
            assert_eq!(
                keys,
                alloc::vec![
                    Value::String("open".into()),
                    Value::String("in_progress".into())
                ]
            );
        }
    }

    #[test]
    fn test_analyze_or_in_predicate() {
        let pass = IndexSelection::new();
        let pred = Expr::or(
            Expr::eq(
                Expr::column("users", "status", 1),
                Expr::literal(Value::String("open".into())),
            ),
            Expr::eq(
                Expr::column("users", "status", 1),
                Expr::literal(Value::String("in_progress".into())),
            ),
        );

        let info = pass.analyze_or_in_predicate(&pred).unwrap();
        assert_eq!(info.table, "users");
        assert_eq!(info.column, "status");
        assert_eq!(
            info.values,
            alloc::vec![
                Value::String("open".into()),
                Value::String("in_progress".into())
            ]
        );
    }

    /// Test case for bug: mixed GIN + non-GIN predicates should preserve non-GIN predicates
    ///
    /// Query: col('status').eq('published') AND col('tags').get('$.primary').eq('tech')
    ///
    /// Expected behcynos:
    /// - The GIN predicate (tags.primary = 'tech') should use GinIndexScan
    /// - The non-GIN predicate (status = 'published') should be preserved as a Filter
    ///
    /// Bug: Currently the non-GIN predicate is completely dropped!
    #[test]
    fn test_mixed_gin_and_non_gin_predicates_bug() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "documents",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![
                    // GIN index on 'tags' column (JSONB)
                    IndexInfo::new_gin("idx_tags", alloc::vec!["tags".into()]),
                    // B-Tree index on 'status' column
                    IndexInfo::new("idx_status", alloc::vec!["status".into()], false),
                ],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Build predicate: status = 'published' AND JSONB_PATH_EQ(tags, '$.primary', 'tech')
        // This simulates: col('status').eq('published').and(col('tags').get('$.primary').eq('tech'))
        let predicate = Expr::and(
            // Non-GIN predicate: status = 'published'
            Expr::eq(
                Expr::column("documents", "status", 1),
                Expr::literal(Value::String("published".into())),
            ),
            // GIN predicate: JSONB_PATH_EQ(tags, '$.primary', 'tech')
            Expr::Function {
                name: "JSONB_PATH_EQ".into(),
                args: alloc::vec![
                    Expr::column("documents", "tags", 2),
                    Expr::literal(Value::String("$.primary".into())),
                    Expr::literal(Value::String("tech".into())),
                ],
            },
        );

        let plan = LogicalPlan::filter(LogicalPlan::scan("documents"), predicate);

        let optimized = pass.optimize(plan);

        // BUG: Currently this returns just GinIndexScan, dropping the status = 'published' predicate!
        // The correct behcynos should be:
        // Filter(status = 'published', GinIndexScan(tags, '$.primary', 'tech'))

        // This assertion currently FAILS - demonstrating the bug
        // The optimized plan should be a Filter wrapping a GinIndexScan
        match &optimized {
            LogicalPlan::Filter { input, predicate } => {
                // Good: we have a Filter
                // Check that the input is a GinIndexScan
                assert!(
                    matches!(input.as_ref(), LogicalPlan::GinIndexScan { .. }),
                    "Expected GinIndexScan as input to Filter, got: {:?}",
                    input
                );
                // Check that the predicate is the non-GIN predicate (status = 'published')
                if let Expr::BinaryOp { left, op, right: _ } = predicate {
                    assert_eq!(*op, BinaryOp::Eq);
                    if let Expr::Column(col) = left.as_ref() {
                        assert_eq!(col.column, "status");
                    } else {
                        panic!("Expected column reference in predicate left side");
                    }
                } else {
                    panic!("Expected BinaryOp predicate");
                }
            }
            LogicalPlan::GinIndexScan { .. } => {
                // BUG: This is what currently happens - the status predicate is dropped!
                panic!(
                    "BUG CONFIRMED: Non-GIN predicate (status = 'published') was dropped! \
                     The query will return incorrect results - all rows matching \
                     tags.primary = 'tech' regardless of status."
                );
            }
            other => {
                panic!("Unexpected plan type: {:?}", other);
            }
        }
    }

    /// Nested JSON paths should use the GIN index once storage can index
    /// nested path postings.
    #[test]
    fn test_nested_json_path_uses_gin_index() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "profiles",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new_gin(
                    "idx_profile",
                    alloc::vec!["profile".into()]
                ),],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        let predicate = Expr::Function {
            name: "JSONB_PATH_EQ".into(),
            args: alloc::vec![
                Expr::column("profiles", "profile", 1),
                Expr::literal(Value::String("$.address.city".into())),
                Expr::literal(Value::String("Beijing".into())),
            ],
        };

        let plan = LogicalPlan::filter(LogicalPlan::scan("profiles"), predicate);
        let optimized = pass.optimize(plan);

        match &optimized {
            LogicalPlan::GinIndexScan {
                index,
                path,
                value,
                query_type,
                ..
            } => {
                assert_eq!(index, "idx_profile");
                assert_eq!(path, "address.city");
                assert_eq!(value, &Some(Value::String("Beijing".into())));
                assert_eq!(query_type, "eq");
            }
            other => {
                panic!("Unexpected plan type: {:?}", other);
            }
        }
    }

    #[test]
    fn test_jsonb_or_equality_uses_gin_any_scan() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "issues",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new_gin(
                    "idx_metadata",
                    alloc::vec!["metadata".into()]
                ),],
            },
        );

        let pass = IndexSelection::with_context(ctx);
        let predicate = Expr::or(
            Expr::Function {
                name: "JSONB_PATH_EQ".into(),
                args: alloc::vec![
                    Expr::column("issues", "metadata", 2),
                    Expr::literal(Value::String("$.customer.tier".into())),
                    Expr::literal(Value::String("enterprise".into())),
                ],
            },
            Expr::Function {
                name: "JSONB_PATH_EQ".into(),
                args: alloc::vec![
                    Expr::column("issues", "metadata", 2),
                    Expr::literal(Value::String("$.customer.tier".into())),
                    Expr::literal(Value::String("mid_market".into())),
                ],
            },
        );

        let optimized = pass.optimize(LogicalPlan::filter(LogicalPlan::scan("issues"), predicate));
        match optimized {
            LogicalPlan::GinIndexScanMulti {
                index,
                column,
                pairs,
                match_all,
                recheck,
                ..
            } => {
                assert_eq!(index, "idx_metadata");
                assert_eq!(column, "metadata");
                assert!(!match_all);
                assert_eq!(
                    pairs,
                    alloc::vec![
                        ("customer.tier".into(), Value::String("enterprise".into())),
                        ("customer.tier".into(), Value::String("mid_market".into())),
                    ]
                );
                let recheck_debug =
                    format!("{:?}", recheck.expect("recheck should be preserved")).to_lowercase();
                assert!(recheck_debug.contains("jsonb_path_eq"));
                assert!(recheck_debug.contains("or"));
            }
            other => panic!("Unexpected plan type: {:?}", other),
        }
    }

    #[test]
    fn test_jsonb_or_equality_inside_and_preserves_remaining_predicates() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "issues",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![
                    IndexInfo::new_gin("idx_metadata", alloc::vec!["metadata".into()]),
                    IndexInfo::new("idx_estimate", alloc::vec!["estimate".into()], false),
                ],
            },
        );

        let pass = IndexSelection::with_context(ctx);
        let predicate = Expr::and(
            Expr::ge(
                Expr::column("issues", "estimate", 1),
                Expr::literal(Value::Int32(3)),
            ),
            Expr::or(
                Expr::Function {
                    name: "JSONB_PATH_EQ".into(),
                    args: alloc::vec![
                        Expr::column("issues", "metadata", 2),
                        Expr::literal(Value::String("$.customer.tier".into())),
                        Expr::literal(Value::String("enterprise".into())),
                    ],
                },
                Expr::Function {
                    name: "JSONB_PATH_EQ".into(),
                    args: alloc::vec![
                        Expr::column("issues", "metadata", 2),
                        Expr::literal(Value::String("$.customer.tier".into())),
                        Expr::literal(Value::String("mid_market".into())),
                    ],
                },
            ),
        );

        let optimized = pass.optimize(LogicalPlan::filter(LogicalPlan::scan("issues"), predicate));
        match optimized {
            LogicalPlan::Filter { input, predicate } => {
                let predicate_debug = format!("{:?}", predicate).to_lowercase();
                assert!(predicate_debug.contains("estimate"));
                match input.as_ref() {
                    LogicalPlan::GinIndexScanMulti {
                        pairs, match_all, ..
                    } => {
                        assert!(!match_all);
                        assert_eq!(pairs.len(), 2);
                    }
                    other => panic!("Expected GIN ANY scan under Filter, got {:?}", other),
                }
            }
            other => panic!("Unexpected plan type: {:?}", other),
        }
    }

    /// Test case for bug: a handled GIN equality predicate must not drop a second
    /// GIN predicate that currently cannot be merged into GinIndexScanMulti.
    #[test]
    fn test_mixed_gin_eq_and_exists_predicates_bug() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "articles",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new_gin("idx_tags", alloc::vec!["tags".into()]),],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        let predicate = Expr::and(
            Expr::Function {
                name: "JSONB_PATH_EQ".into(),
                args: alloc::vec![
                    Expr::column("articles", "tags", 1),
                    Expr::literal(Value::String("$.primary".into())),
                    Expr::literal(Value::String("tech".into())),
                ],
            },
            Expr::Function {
                name: "JSONB_EXISTS".into(),
                args: alloc::vec![
                    Expr::column("articles", "tags", 1),
                    Expr::literal(Value::String("$.secondary".into())),
                ],
            },
        );

        let plan = LogicalPlan::filter(LogicalPlan::scan("articles"), predicate);
        let optimized = pass.optimize(plan);

        match &optimized {
            LogicalPlan::Filter { input, predicate } => {
                assert!(
                    matches!(input.as_ref(), LogicalPlan::GinIndexScan { .. }),
                    "Expected GinIndexScan under Filter, got {:?}",
                    input
                );

                let predicate_debug = format!("{:?}", predicate).to_lowercase();
                assert!(
                    predicate_debug.contains("jsonb_exists"),
                    "Expected remaining JSONB_EXISTS predicate to be preserved, got {:?}",
                    predicate
                );
            }
            LogicalPlan::GinIndexScanMulti { .. } => {
                panic!("Expected JSONB_EXISTS to remain as a post-filter");
            }
            other => {
                panic!("Unexpected plan type: {:?}", other);
            }
        }
    }

    #[test]
    fn test_jsonb_contains_uses_gin_prefilter_and_preserves_post_filter() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "products",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new_gin(
                    "idx_metadata",
                    alloc::vec!["metadata".into()]
                ),],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        let predicate = Expr::Function {
            name: "JSONB_CONTAINS".into(),
            args: alloc::vec![
                Expr::column("products", "metadata", 1),
                Expr::literal(Value::String("$.tags".into())),
                Expr::literal(Value::String("portable".into())),
            ],
        };

        let plan = LogicalPlan::filter(LogicalPlan::scan("products"), predicate);
        let optimized = pass.optimize(plan);
        let expected_pairs: Vec<(String, Value)> = test_contains_trigrams("portable")
            .into_iter()
            .map(|gram| (test_contains_trigram_key("tags"), Value::String(gram)))
            .collect();

        match &optimized {
            LogicalPlan::Filter { input, predicate } => {
                assert!(
                    matches!(
                        input.as_ref(),
                        LogicalPlan::GinIndexScanMulti { pairs, match_all, .. }
                            if *match_all && pairs == &expected_pairs
                    ),
                    "Expected GIN prefilter for JSONB_CONTAINS, got {:?}",
                    input
                );

                let predicate_debug = format!("{:?}", predicate).to_lowercase();
                assert!(
                    predicate_debug.contains("jsonb_contains"),
                    "Expected JSONB_CONTAINS predicate to remain as post-filter, got {:?}",
                    predicate
                );
            }
            other => {
                panic!("Unexpected plan type: {:?}", other);
            }
        }
    }

    #[test]
    fn test_jsonb_contains_short_string_falls_back_to_path_prefilter() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "products",
            TableStats {
                row_count: 1000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new_gin(
                    "idx_metadata",
                    alloc::vec!["metadata".into()]
                ),],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        let predicate = Expr::Function {
            name: "JSONB_CONTAINS".into(),
            args: alloc::vec![
                Expr::column("products", "metadata", 1),
                Expr::literal(Value::String("$.tags".into())),
                Expr::literal(Value::String("tv".into())),
            ],
        };

        let plan = LogicalPlan::filter(LogicalPlan::scan("products"), predicate);
        let optimized = pass.optimize(plan);

        match &optimized {
            LogicalPlan::Filter { input, predicate } => {
                assert!(
                    matches!(
                        input.as_ref(),
                        LogicalPlan::GinIndexScan {
                            query_type,
                            path,
                            ..
                        } if query_type == "contains" && path == "tags"
                    ),
                    "Expected short JSONB_CONTAINS to keep path prefilter, got {:?}",
                    input
                );

                let predicate_debug = format!("{:?}", predicate).to_lowercase();
                assert!(
                    predicate_debug.contains("jsonb_contains"),
                    "Expected JSONB_CONTAINS predicate to remain as post-filter, got {:?}",
                    predicate
                );
            }
            other => {
                panic!("Unexpected plan type: {:?}", other);
            }
        }
    }

    /// Test case for bug: B-Tree index should work with AND predicates
    ///
    /// Query: price > 100 AND category = 'Electronics'
    ///
    /// Expected behcynos:
    /// - The indexed predicate (price > 100) should use IndexScan
    /// - The non-indexed predicate (category = 'Electronics') should be preserved as a Filter
    ///
    /// Bug: Currently the entire AND predicate fails to use any index because
    /// analyze_predicate only handles simple predicates, not AND combinations.
    #[test]
    fn test_btree_index_with_and_predicates_bug() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "products",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![
                    // B-Tree index on 'price' column
                    IndexInfo::new("idx_price", alloc::vec!["price".into()], false),
                ],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Build predicate: price > 100 AND category = 'Electronics'
        let predicate = Expr::and(
            // Indexed predicate: price > 100
            Expr::gt(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(100)),
            ),
            // Non-indexed predicate: category = 'Electronics'
            Expr::eq(
                Expr::column("products", "category", 2),
                Expr::literal(Value::String("Electronics".into())),
            ),
        );

        let plan = LogicalPlan::filter(LogicalPlan::scan("products"), predicate);

        let optimized = pass.optimize(plan);

        // The correct behcynos should be:
        // Filter(category = 'Electronics', IndexScan(price > 100))
        //
        // But currently it returns:
        // Filter(price > 100 AND category = 'Electronics', Scan(products))
        // because analyze_predicate returns None for AND expressions

        match &optimized {
            LogicalPlan::Filter {
                input,
                predicate: _,
            } => {
                match input.as_ref() {
                    LogicalPlan::IndexScan { index, .. } => {
                        // Good: we're using IndexScan
                        assert_eq!(index, "idx_price");
                    }
                    LogicalPlan::Scan { .. } => {
                        // BUG: Index is not being used!
                        panic!(
                            "BUG CONFIRMED: B-Tree index (idx_price) is not used for AND predicates! \
                             The query falls back to full table scan even though price > 100 \
                             could use the index."
                        );
                    }
                    other => {
                        panic!("Unexpected input plan type: {:?}", other);
                    }
                }
            }
            LogicalPlan::IndexScan { .. } => {
                // This would be acceptable too (if the other predicate is somehow handled)
            }
            other => {
                panic!("Unexpected plan type: {:?}", other);
            }
        }
    }

    /// Test that point lookups (IndexGet) are prioritized over range scans (IndexScan)
    /// when both are available in an AND predicate.
    #[test]
    fn test_btree_and_prioritizes_point_lookup() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "products",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![
                    // B-Tree index on 'price' column (for range scan)
                    IndexInfo::new("idx_price", alloc::vec!["price".into()], false),
                    // B-Tree index on 'id' column (for point lookup)
                    IndexInfo::new("idx_id", alloc::vec!["id".into()], true),
                ],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Build predicate: price > 100 AND id = 42
        // Both predicates are indexable, but id = 42 should be preferred (point lookup)
        let predicate = Expr::and(
            // Range predicate: price > 100
            Expr::gt(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(100)),
            ),
            // Point lookup predicate: id = 42
            Expr::eq(
                Expr::column("products", "id", 0),
                Expr::literal(Value::Int64(42)),
            ),
        );

        let plan = LogicalPlan::filter(LogicalPlan::scan("products"), predicate);

        let optimized = pass.optimize(plan);

        // Should use IndexGet for id = 42 (point lookup) and Filter for price > 100
        match &optimized {
            LogicalPlan::Filter { input, predicate } => {
                // Check that we're using IndexGet (point lookup)
                match input.as_ref() {
                    LogicalPlan::IndexGet { index, key, .. } => {
                        assert_eq!(index, "idx_id");
                        assert_eq!(*key, Value::Int64(42));
                    }
                    other => {
                        panic!("Expected IndexGet, got: {:?}", other);
                    }
                }
                // Check that the remaining predicate is price > 100
                if let Expr::BinaryOp { left, op, right } = predicate {
                    assert_eq!(*op, BinaryOp::Gt);
                    if let Expr::Column(col) = left.as_ref() {
                        assert_eq!(col.column, "price");
                    }
                    if let Expr::Literal(Value::Int64(v)) = right.as_ref() {
                        assert_eq!(*v, 100);
                    }
                }
            }
            LogicalPlan::IndexGet { .. } => {
                // Also acceptable if there's no remaining predicate
            }
            other => {
                panic!("Unexpected plan type: {:?}", other);
            }
        }
    }

    /// Test: Multiple range predicates on the same column should be merged.
    ///
    /// Query: price > 10 AND price < 150
    ///
    /// Expected: IndexScan with range (10, 150) - no Filter needed
    #[test]
    fn test_range_merge_same_column() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "stocks",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_price",
                    alloc::vec!["price".into()],
                    false
                )],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Build predicate: price > 10 AND price < 150
        let predicate = Expr::and(
            Expr::gt(
                Expr::column("stocks", "price", 1),
                Expr::literal(Value::Float64(10.0)),
            ),
            Expr::lt(
                Expr::column("stocks", "price", 1),
                Expr::literal(Value::Float64(150.0)),
            ),
        );

        let plan = LogicalPlan::filter(LogicalPlan::scan("stocks"), predicate);
        let optimized = pass.optimize(plan);

        // Should be IndexScan with merged range (10, 150) - no Filter
        match optimized {
            LogicalPlan::IndexScan { index, bounds, .. } => {
                assert_eq!(index, "idx_price");
                match bounds {
                    IndexBounds::Scalar(KeyRange::Bound {
                        lower,
                        upper,
                        lower_exclusive,
                        upper_exclusive,
                    }) => {
                        assert_eq!(lower, Value::Float64(10.0));
                        assert_eq!(upper, Value::Float64(150.0));
                        assert!(lower_exclusive, "Lower bound should be exclusive (>)");
                        assert!(upper_exclusive, "Upper bound should be exclusive (<)");
                    }
                    other => panic!("Expected scalar bound range, got {:?}", other),
                }
            }
            other => {
                panic!("Expected IndexScan with merged range, got: {:?}", other);
            }
        }
    }

    /// Test: Range merge with inclusive bounds (>= and <=)
    #[test]
    fn test_range_merge_inclusive_bounds() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "products",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_price",
                    alloc::vec!["price".into()],
                    false
                )],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Build predicate: price >= 100 AND price <= 500
        let predicate = Expr::and(
            Expr::ge(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(100)),
            ),
            Expr::le(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(500)),
            ),
        );

        let plan = LogicalPlan::filter(LogicalPlan::scan("products"), predicate);
        let optimized = pass.optimize(plan);

        match optimized {
            LogicalPlan::IndexScan { bounds, .. } => match bounds {
                IndexBounds::Scalar(KeyRange::Bound {
                    lower,
                    upper,
                    lower_exclusive,
                    upper_exclusive,
                }) => {
                    assert_eq!(lower, Value::Int64(100));
                    assert_eq!(upper, Value::Int64(500));
                    assert!(!lower_exclusive, "Lower bound should be inclusive (>=)");
                    assert!(!upper_exclusive, "Upper bound should be inclusive (<=)");
                }
                other => panic!("Expected bound range, got {:?}", other),
            },
            other => {
                panic!("Expected IndexScan, got: {:?}", other);
            }
        }
    }

    /// Test: Range merge takes the more restrictive bound when same value
    /// price > 10 AND price >= 10 should result in price > 10 (exclusive)
    #[test]
    fn test_range_merge_same_value_takes_exclusive() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "products",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_price",
                    alloc::vec!["price".into()],
                    false
                )],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Build predicate: price > 10 AND price >= 10
        let predicate = Expr::and(
            Expr::gt(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(10)),
            ),
            Expr::ge(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(10)),
            ),
        );

        let plan = LogicalPlan::filter(LogicalPlan::scan("products"), predicate);
        let optimized = pass.optimize(plan);

        match optimized {
            LogicalPlan::IndexScan { bounds, .. } => match bounds {
                IndexBounds::Scalar(KeyRange::LowerBound { value, exclusive }) => {
                    assert_eq!(value, Value::Int64(10));
                    assert!(exclusive, "Should take exclusive bound (>)");
                }
                other => panic!("Expected lower bound, got {:?}", other),
            },
            other => {
                panic!("Expected IndexScan, got: {:?}", other);
            }
        }
    }

    /// Test: Range merge takes the larger lower bound
    /// price > 10 AND price > 5 should result in price > 10
    #[test]
    fn test_range_merge_takes_larger_lower_bound() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "products",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_price",
                    alloc::vec!["price".into()],
                    false
                )],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Build predicate: price > 10 AND price > 5
        let predicate = Expr::and(
            Expr::gt(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(10)),
            ),
            Expr::gt(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(5)),
            ),
        );

        let plan = LogicalPlan::filter(LogicalPlan::scan("products"), predicate);
        let optimized = pass.optimize(plan);

        match optimized {
            LogicalPlan::IndexScan { bounds, .. } => match bounds {
                IndexBounds::Scalar(KeyRange::LowerBound { value, .. }) => {
                    assert_eq!(value, Value::Int64(10));
                }
                other => panic!("Expected lower bound, got {:?}", other),
            },
            other => {
                panic!("Expected IndexScan, got: {:?}", other);
            }
        }
    }

    /// Test: Range merge takes the smaller upper bound
    /// price < 150 AND price < 200 should result in price < 150
    #[test]
    fn test_range_merge_takes_smaller_upper_bound() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "products",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_price",
                    alloc::vec!["price".into()],
                    false
                )],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Build predicate: price < 150 AND price < 200
        let predicate = Expr::and(
            Expr::lt(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(150)),
            ),
            Expr::lt(
                Expr::column("products", "price", 1),
                Expr::literal(Value::Int64(200)),
            ),
        );

        let plan = LogicalPlan::filter(LogicalPlan::scan("products"), predicate);
        let optimized = pass.optimize(plan);

        match optimized {
            LogicalPlan::IndexScan { bounds, .. } => match bounds {
                IndexBounds::Scalar(KeyRange::UpperBound { value, .. }) => {
                    assert_eq!(value, Value::Int64(150));
                }
                other => panic!("Expected upper bound, got {:?}", other),
            },
            other => {
                panic!("Expected IndexScan, got: {:?}", other);
            }
        }
    }

    /// Test: Range merge with additional non-indexed predicate
    /// price > 10 AND price < 150 AND status = 'active'
    /// Should produce: Filter(status = 'active', IndexScan(10, 150))
    #[test]
    fn test_range_merge_with_non_indexed_predicate() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "stocks",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_price",
                    alloc::vec!["price".into()],
                    false
                )],
            },
        );

        let pass = IndexSelection::with_context(ctx);

        // Build predicate: price > 10 AND price < 150 AND status = 'active'
        let predicate = Expr::and(
            Expr::and(
                Expr::gt(
                    Expr::column("stocks", "price", 1),
                    Expr::literal(Value::Float64(10.0)),
                ),
                Expr::lt(
                    Expr::column("stocks", "price", 1),
                    Expr::literal(Value::Float64(150.0)),
                ),
            ),
            Expr::eq(
                Expr::column("stocks", "status", 2),
                Expr::literal(Value::String("active".into())),
            ),
        );

        let plan = LogicalPlan::filter(LogicalPlan::scan("stocks"), predicate);
        let optimized = pass.optimize(plan);

        // Should be Filter(status = 'active', IndexScan(10, 150))
        match optimized {
            LogicalPlan::Filter { input, predicate } => {
                // Check the Filter predicate is status = 'active'
                if let Expr::BinaryOp { left, op, .. } = &predicate {
                    assert_eq!(*op, BinaryOp::Eq);
                    if let Expr::Column(col) = left.as_ref() {
                        assert_eq!(col.column, "status");
                    }
                }

                // Check the input is IndexScan with merged range
                match input.as_ref() {
                    LogicalPlan::IndexScan { bounds, .. } => match bounds {
                        IndexBounds::Scalar(KeyRange::Bound {
                            lower,
                            upper,
                            lower_exclusive,
                            upper_exclusive,
                        }) => {
                            assert_eq!(*lower, Value::Float64(10.0));
                            assert_eq!(*upper, Value::Float64(150.0));
                            assert!(*lower_exclusive);
                            assert!(*upper_exclusive);
                        }
                        other => panic!("Expected scalar bound range, got {:?}", other),
                    },
                    other => {
                        panic!("Expected IndexScan inside Filter, got: {:?}", other);
                    }
                }
            }
            other => {
                panic!("Expected Filter wrapping IndexScan, got: {:?}", other);
            }
        }
    }

    #[test]
    fn test_composite_index_generates_tuple_bounds() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "scores",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_region_score",
                    alloc::vec!["region".into(), "score".into()],
                    false,
                )],
            },
        );

        let pass = IndexSelection::with_context(ctx);
        let predicate = Expr::and(
            Expr::eq(
                Expr::column("scores", "region", 0),
                Expr::literal(Value::String("apac".into())),
            ),
            Expr::and(
                Expr::ge(
                    Expr::column("scores", "score", 1),
                    Expr::literal(Value::Int64(10)),
                ),
                Expr::le(
                    Expr::column("scores", "score", 1),
                    Expr::literal(Value::Int64(20)),
                ),
            ),
        );

        let optimized = pass.optimize(LogicalPlan::filter(LogicalPlan::scan("scores"), predicate));
        match optimized {
            LogicalPlan::IndexScan { index, bounds, .. } => {
                assert_eq!(index, "idx_region_score");
                match bounds {
                    IndexBounds::Composite(KeyRange::Bound {
                        lower,
                        upper,
                        lower_exclusive,
                        upper_exclusive,
                    }) => {
                        assert_eq!(
                            lower,
                            alloc::vec![Value::String("apac".into()), Value::Int64(10)]
                        );
                        assert_eq!(
                            upper,
                            alloc::vec![Value::String("apac".into()), Value::Int64(20)]
                        );
                        assert!(!lower_exclusive);
                        assert!(!upper_exclusive);
                    }
                    other => panic!("Expected composite tuple bounds, got {:?}", other),
                }
            }
            other => panic!("Expected composite IndexScan, got {:?}", other),
        }
    }

    #[test]
    fn test_single_column_predicate_does_not_use_composite_prefix_index() {
        let mut ctx = ExecutionContext::new();
        ctx.register_table(
            "scores",
            TableStats {
                row_count: 10000,
                is_sorted: false,
                indexes: alloc::vec![IndexInfo::new(
                    "idx_region_score",
                    alloc::vec!["region".into(), "score".into()],
                    false,
                )],
            },
        );

        let pass = IndexSelection::with_context(ctx);
        let predicate = Expr::eq(
            Expr::column("scores", "region", 0),
            Expr::literal(Value::String("apac".into())),
        );

        let optimized = pass.optimize(LogicalPlan::filter(LogicalPlan::scan("scores"), predicate));
        assert!(
            matches!(optimized, LogicalPlan::Filter { .. }),
            "single-column predicates should not use a composite prefix as a scalar index lookup"
        );
    }
}
