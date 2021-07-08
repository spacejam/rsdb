use std::path::PathBuf;

const USAGE: &str = "Usage: rsdb [directory of db] [command]";

#[derive(Debug, Clone)]
struct Args {
    directory: PathBuf,
    command: String,
}

impl Default for Args {
    fn default() -> Args {
        Args {
            directory: "rsdb".into(),
            command: "show tables".into(),
        }
    }
}

fn parse<'a, I, T>(mut iter: I) -> T
where
    I: Iterator<Item = &'a str>,
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Debug,
{
    iter.next().expect(USAGE).parse().expect(USAGE)
}

impl Args {
    fn parse() -> Args {
        let mut args = Args::default();
        for raw_arg in std::env::args().skip(1) {
            let mut splits = raw_arg[2..].split('=');
            match splits.next().unwrap() {
                "directory" => args.directory = parse(&mut splits),
                "command" => args.command = parse(&mut splits),
                other => panic!("unknown option: {}, {}", other, USAGE),
            }
        }
        args
    }
}

fn main() {
    let _args = Args::parse();
}
