fn main() {
    if let Err(e) = inlet::cli::run() {
        eprintln!("inlet: {e}");
        std::process::exit(1);
    }
}
