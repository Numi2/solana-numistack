#[test]
fn aggregator_config_disables_stdout_json() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = std::path::Path::new(manifest_dir).join("configs/aggregator.json");
    let raw = std::fs::read_to_string(&path).expect("read aggregator.json");
    let v: serde_json::Value = serde_json::from_str(&raw).expect("parse json");
    let sj = v.get("stdout_json").and_then(|b| b.as_bool()).expect("stdout_json bool present");
    assert!(!sj, "stdout_json must be false in production configs");
}


