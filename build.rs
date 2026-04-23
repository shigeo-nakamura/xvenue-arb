use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../dex-connector/.git/HEAD");
    println!("cargo:rerun-if-changed=../dex-connector/.git/refs/heads");

    let hash = Command::new("git")
        .args(["-C", "../dex-connector", "rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|out| {
            if out.status.success() {
                Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
            } else {
                None
            }
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=DEX_CONNECTOR_GIT_HASH={}", hash);
}
