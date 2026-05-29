mod defaults;
mod options;

use super::Args;
use super::validation::validate_and_normalize;
use std::error::Error;

use defaults::default_args;
use options::apply_option;

pub fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Option<Args>, Box<dyn Error>> {
    let mut parsed = default_args();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(None),
            _ => apply_option(arg.as_str(), &mut args, &mut parsed)?,
        }
    }
    validate_and_normalize(&mut parsed)?;
    Ok(Some(parsed))
}

fn required_arg(
    args: &mut impl Iterator<Item = String>,
    message: &'static str,
) -> Result<String, Box<dyn Error>> {
    args.next().ok_or_else(|| message.into())
}
