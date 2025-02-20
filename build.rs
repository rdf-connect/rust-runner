use std::io::Result;
fn main() -> Result<()> {
    tonic_build::configure().compile_protos(&["./proto/index.proto"], &["./proto/"])?;
    Ok(())
}
