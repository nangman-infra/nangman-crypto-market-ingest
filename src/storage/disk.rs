use std::ffi::CString;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

/// Return the disk used percent for the filesystem hosting `path`.
///
/// Uses POSIX `statvfs(3)` so the same code works on Linux (production) and
/// macOS (developer machines). Returns 0 if `path` does not exist.
pub fn disk_used_pct(path: &Path) -> io::Result<u8> {
    if !path.exists() {
        return Ok(0);
    }
    let cstr = CString::new(path.as_os_str().as_bytes())
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(cstr.as_ptr(), &mut stat) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    let total = u128::from(stat.f_blocks) * u128::from(stat.f_frsize);
    let free = u128::from(stat.f_bfree) * u128::from(stat.f_frsize);
    if total == 0 {
        return Ok(0);
    }
    let used = total.saturating_sub(free);
    let pct = (used * 100 / total).min(100) as u8;
    Ok(pct)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_zero_for_missing_path() {
        let path =
            std::env::temp_dir().join(format!("market-ingest-disk-missing-{}", std::process::id()));
        let pct = disk_used_pct(&path).unwrap();
        assert_eq!(pct, 0);
    }

    #[test]
    fn reads_a_real_filesystem_pct() {
        let path = std::env::temp_dir();
        let pct = disk_used_pct(&path).unwrap();
        assert!(pct <= 100);
    }
}
