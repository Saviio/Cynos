#[cfg(target_arch = "wasm32")]
use wasm_bindgen::JsCast;

#[derive(Clone, Debug, Default)]
pub(crate) struct DeltaFlushProfile {
    pub delta_table_count: usize,
    pub delta_query_count: usize,
    pub rows_query_count: usize,
    pub graphql_query_count: usize,
    pub delta_row_count: usize,
    pub clone_ms: f64,
    pub query_on_table_change_ms: f64,
    pub source_dispatch_ms: f64,
    pub unary_execute_ms: f64,
    pub join_execute_ms: f64,
    pub aggregate_execute_ms: f64,
    pub result_apply_ms: f64,
    pub graphql_view_update_ms: f64,
    pub graphql_invalidation_ms: f64,
    pub graphql_render_ms: f64,
    pub graphql_encode_ms: f64,
    pub graphql_emit_ms: f64,
    pub total_ms: f64,
}

#[cfg(feature = "benchmark")]
#[derive(Clone, Debug, Default)]
pub(crate) struct GraphqlDeltaProfile {
    pub view_update_ms: f64,
    pub invalidation_ms: f64,
    pub render_ms: f64,
    pub encode_ms: f64,
    pub emit_ms: f64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TraceInitProfile {
    pub compile_plan_ms: f64,
    pub compile_to_dataflow_ms: f64,
    pub compile_ivm_plan_ms: f64,
    pub compile_trace_program_ms: f64,
    pub initial_query_ms: f64,
    pub source_bootstrap_ms: f64,
    pub source_access_ms: f64,
    pub source_emit_ms: f64,
    pub source_visit_ms: f64,
    pub source_handle_wrap_ms: f64,
    pub bootstrap_scan_ms: f64,
    pub bootstrap_execute_ms: f64,
    pub bootstrap_runtime_node_ms: f64,
    pub bootstrap_slot_ms: f64,
    pub filter_bootstrap_ms: f64,
    pub project_bootstrap_ms: f64,
    pub map_bootstrap_ms: f64,
    pub join_build_ms: f64,
    pub join_finalize_ms: f64,
    pub join_emit_ms: f64,
    pub join_bootstrap_ms: f64,
    pub aggregate_bootstrap_ms: f64,
    pub root_sink_ms: f64,
    pub visible_store_init_ms: f64,
    pub materialized_view_init_ms: f64,
    pub total_ms: f64,
    pub source_table_count: usize,
    pub initial_row_count: usize,
}

#[cfg(feature = "benchmark")]
#[derive(Clone, Debug, Default)]
pub(crate) struct SnapshotInitProfile {
    pub logical_plan_ms: f64,
    pub describe_output_ms: f64,
    pub binary_layout_ms: f64,
    pub partial_refresh_plan_ms: f64,
    pub compile_main_plan_ms: f64,
    pub root_subset_plan_ms: f64,
    pub initial_query_ms: f64,
    pub dependency_bindings_ms: f64,
    pub initial_result_adapt_ms: f64,
    pub observable_init_ms: f64,
    pub total_ms: f64,
    pub initial_row_count: usize,
    pub visible_row_count: usize,
    pub partial_refresh_enabled: bool,
    pub root_subset_enabled: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SnapshotRefreshMode {
    FullRequery,
    CandidateWindowPartialRefresh,
    RootSubsetRefresh,
}

impl Default for SnapshotRefreshMode {
    fn default() -> Self {
        Self::FullRequery
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SnapshotQueryProfile {
    pub refresh_mode: SnapshotRefreshMode,
    pub reactive_patch_attempted: bool,
    pub reactive_patch_hit: bool,
    pub reactive_patch_ms: f64,
    pub partial_refresh_attempted: bool,
    pub partial_refresh_hit: bool,
    pub partial_refresh_collect_ms: f64,
    pub partial_refresh_requery_ms: f64,
    pub partial_refresh_apply_ms: f64,
    pub partial_refresh_compare_ms: f64,
    pub root_subset_attempted: bool,
    pub root_subset_hit: bool,
    pub root_subset_collect_ms: f64,
    pub root_subset_requery_ms: f64,
    pub root_subset_apply_ms: f64,
    pub root_subset_compare_ms: f64,
    pub full_requery_ms: f64,
    pub full_requery_compare_ms: f64,
    pub callback_ms: f64,
    pub total_ms: f64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SnapshotFlushProfile {
    pub changed_table_count: usize,
    pub rows_query_count: usize,
    pub graphql_query_count: usize,
    pub invalidation_merge_ms: f64,
    pub rows_query_on_change_ms: f64,
    pub graphql_query_on_change_ms: f64,
    pub graphql_root_refresh_ms: f64,
    pub graphql_batch_invalidation_ms: f64,
    pub graphql_render_ms: f64,
    pub graphql_encode_ms: f64,
    pub graphql_emit_ms: f64,
    pub reactive_patch_attempt_count: usize,
    pub reactive_patch_hit_count: usize,
    pub reactive_patch_ms: f64,
    pub partial_refresh_attempt_count: usize,
    pub partial_refresh_hit_count: usize,
    pub partial_refresh_collect_ms: f64,
    pub partial_refresh_requery_ms: f64,
    pub partial_refresh_apply_ms: f64,
    pub partial_refresh_compare_ms: f64,
    pub root_subset_attempt_count: usize,
    pub root_subset_hit_count: usize,
    pub root_subset_collect_ms: f64,
    pub root_subset_requery_ms: f64,
    pub root_subset_apply_ms: f64,
    pub root_subset_compare_ms: f64,
    pub full_requery_count: usize,
    pub full_requery_ms: f64,
    pub full_requery_compare_ms: f64,
    pub callback_ms: f64,
    pub total_ms: f64,
}

#[cfg(feature = "benchmark")]
#[derive(Clone, Debug, Default)]
pub(crate) struct GraphqlSnapshotQueryProfile {
    pub root_refresh_ms: f64,
    pub batch_invalidation_ms: f64,
    pub render_ms: f64,
    pub encode_ms: f64,
    pub emit_ms: f64,
}

#[cfg(feature = "benchmark")]
impl SnapshotFlushProfile {
    pub fn record_rows_query(&mut self, sample: &SnapshotQueryProfile) {
        self.rows_query_count += 1;
        self.callback_ms += sample.callback_ms;

        if sample.reactive_patch_attempted {
            self.reactive_patch_attempt_count += 1;
        }
        if sample.reactive_patch_hit {
            self.reactive_patch_hit_count += 1;
        }
        self.reactive_patch_ms += sample.reactive_patch_ms;

        if sample.partial_refresh_attempted {
            self.partial_refresh_attempt_count += 1;
        }
        if sample.partial_refresh_hit {
            self.partial_refresh_hit_count += 1;
        }
        self.partial_refresh_collect_ms += sample.partial_refresh_collect_ms;
        self.partial_refresh_requery_ms += sample.partial_refresh_requery_ms;
        self.partial_refresh_apply_ms += sample.partial_refresh_apply_ms;
        self.partial_refresh_compare_ms += sample.partial_refresh_compare_ms;

        if sample.root_subset_attempted {
            self.root_subset_attempt_count += 1;
        }
        if sample.root_subset_hit {
            self.root_subset_hit_count += 1;
        }
        self.root_subset_collect_ms += sample.root_subset_collect_ms;
        self.root_subset_requery_ms += sample.root_subset_requery_ms;
        self.root_subset_apply_ms += sample.root_subset_apply_ms;
        self.root_subset_compare_ms += sample.root_subset_compare_ms;

        if sample.full_requery_ms > 0.0 || sample.full_requery_compare_ms > 0.0 {
            self.full_requery_count += 1;
        }
        self.full_requery_ms += sample.full_requery_ms;
        self.full_requery_compare_ms += sample.full_requery_compare_ms;
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct IvmBridgeProfile {
    pub callback_count: usize,
    pub added_row_count: usize,
    pub removed_row_count: usize,
    pub serialize_added_ms: f64,
    pub serialize_removed_ms: f64,
    pub assemble_delta_ms: f64,
    pub callback_call_ms: f64,
    pub total_ms: f64,
}

#[derive(Default)]
pub(crate) struct IvmBridgeProfiler {
    #[cfg(feature = "benchmark")]
    current: IvmBridgeProfile,
    last: Option<IvmBridgeProfile>,
}

impl IvmBridgeProfiler {
    #[cfg(feature = "benchmark")]
    pub fn begin_flush(&mut self) {
        self.current = IvmBridgeProfile::default();
    }

    #[cfg(feature = "benchmark")]
    pub fn record_sample(&mut self, sample: &IvmBridgeProfile) {
        self.current.callback_count += sample.callback_count;
        self.current.added_row_count += sample.added_row_count;
        self.current.removed_row_count += sample.removed_row_count;
        self.current.serialize_added_ms += sample.serialize_added_ms;
        self.current.serialize_removed_ms += sample.serialize_removed_ms;
        self.current.assemble_delta_ms += sample.assemble_delta_ms;
        self.current.callback_call_ms += sample.callback_call_ms;
        self.current.total_ms += sample.total_ms;
    }

    #[cfg(feature = "benchmark")]
    pub fn end_flush(&mut self) {
        self.last = Some(self.current.clone());
    }

    pub fn take_last(&mut self) -> Option<IvmBridgeProfile> {
        self.last.take()
    }
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn now_ms() -> f64 {
    let global = js_sys::global();
    if let Ok(performance) =
        js_sys::Reflect::get(&global, &wasm_bindgen::JsValue::from_str("performance"))
    {
        if let Some(now_fn) =
            js_sys::Reflect::get(&performance, &wasm_bindgen::JsValue::from_str("now"))
                .ok()
                .and_then(|value| value.dyn_into::<js_sys::Function>().ok())
        {
            if let Ok(now) = now_fn.call0(&performance) {
                if let Some(value) = now.as_f64() {
                    return value;
                }
            }
        }
    }

    js_sys::Date::now()
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn now_ms() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64() * 1000.0)
        .unwrap_or(0.0)
}
