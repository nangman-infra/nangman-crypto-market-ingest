use super::types::BackfillArgs;
use super::validation::validate_completed_args;
use crate::backfill::BackfillError;

mod options;
mod values;

pub fn parse_args(
    mut args: impl Iterator<Item = String>,
) -> Result<Option<BackfillArgs>, BackfillError> {
    let mut parsed = BackfillArgs::with_defaults();

    while let Some(arg) = args.next() {
        if matches!(arg.as_str(), "-h" | "--help") {
            return Ok(None);
        }
        options::apply_arg(&mut parsed, &arg, &mut args)?;
    }

    validate_completed_args(&parsed)?;
    Ok(Some(parsed))
}
