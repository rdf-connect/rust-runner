fn main() -> Result<(), Box<dyn std::error::Error>> {
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
