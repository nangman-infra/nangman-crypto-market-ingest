mod defaults;
mod help;
mod parse;
mod parse_cli;
#[cfg(test)]
mod tests;
mod types;
mod validation;

pub use parse_cli::parse_args;
pub use types::{InputRange, NormalizeArgs};

pub fn print_help() {
    help::print_help();
}
