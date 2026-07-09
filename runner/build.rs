fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `env!("TARGET")` isn't available to ordinary crate code — only build
    // scripts see the `TARGET` triple cargo is compiling for. Re-export it as
    // a compile-time env var so `runner/src/registry.rs` can bake in exactly
    // the triple this binary was built for (not a coarse `std::env::consts`
    // guess, which can't distinguish e.g. `gnu` vs `musl` libc, or `msvc` vs
    // `gnu` on Windows).
    println!(
        "cargo:rustc-env=RDFC_TARGET_TRIPLE={}",
        std::env::var("TARGET")?
    );

    tonic_build::configure()
        .build_server(true)
        .build_client(false) // client disabled: `rpc connect` collides with tonic's
                             // transport-level `connect(dst)` in the same impl block (E0592).
                             // We call the RPC manually via `tonic::client::Grpc::streaming()`.
        .compile_protos(
            &["proto/service.proto"],
            &["proto", "/usr/include"],
        )?;
    Ok(())
}
