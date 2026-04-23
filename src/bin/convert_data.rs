use std::env;
use std::process;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 || args.len() > 4 {
        eprintln!("Usage: convert-data <input.jsonl> <output.bin> [interval_secs]");
        eprintln!("  interval_secs: downsample interval (default: 5, 0 = keep all)");
        process::exit(1);
    }

    let input = &args[1];
    let output = &args[2];
    let interval_secs: u64 = args
        .get(3)
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);

    eprintln!("Converting {} -> {} (interval={}s) ...", input, output, interval_secs);
    match debot::ports::replay_dex::ReplayConnector::convert_jsonl_to_bincode_with_interval(
        input,
        output,
        interval_secs,
    ) {
        Ok(()) => eprintln!("Done."),
        Err(e) => {
            eprintln!("Error: {}", e);
            process::exit(1);
        }
    }
}
