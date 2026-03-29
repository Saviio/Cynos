#[derive(Clone, Debug, Default)]
pub struct StorageGinInsertProfile {
    pub parse_json_ms: f64,
    pub path_lookup_ms: f64,
    pub scalar_emit_ms: f64,
    pub contains_stringify_ms: f64,
    pub contains_trigram_emit_ms: f64,
    pub parse_call_count: usize,
    pub selected_path_eval_count: usize,
    pub selected_path_hit_count: usize,
    pub path_key_emit_count: usize,
    pub scalar_value_count: usize,
    pub contains_value_count: usize,
    pub contains_trigram_count: usize,
}

#[derive(Clone, Debug, Default)]
pub struct StorageInsertProfile {
    pub row_count: usize,
    pub secondary_index_count: usize,
    pub gin_index_count: usize,
    pub validation_ms: f64,
    pub row_id_index_ms: f64,
    pub primary_index_ms: f64,
    pub secondary_index_ms: f64,
    pub gin_collect_ms: f64,
    pub gin_flush_ms: f64,
    pub row_slot_ms: f64,
    pub total_ms: f64,
    pub gin: StorageGinInsertProfile,
}
