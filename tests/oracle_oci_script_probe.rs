use std::process::Command;

#[test]
#[ignore = "requires local Oracle XE at localhost:1521/FREE"]
fn oracle_oci_script_probe_executes_test_all_without_errors() {
    let probe = env!("CARGO_BIN_EXE_oracle_oci_script_probe");
    let output = Command::new(probe)
        .output()
        .expect("oracle oci script probe process must start");

    assert!(
        output.status.success(),
        "oracle oci script probe failed.\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
