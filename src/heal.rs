use std::fs::read_dir;
use std::path::Path;

use volume::Brick;
use super::GlusterError;

/// Find the self heal count for a given brick
pub fn get_self_heal_count(brick: Brick) -> Result<usize, GlusterError> {
    let brick_path = format!("{}/.glusterfs/indices/xattrop",
                             brick.path.to_string_lossy().into_owned());
    let heal_path = Path::new(&brick_path);

    // Count all files that don't start with xattrop.  Those are gfid's that need healing
    let entry_count = read_dir(heal_path)
        ?
        .filter_map(|entry| entry.ok())
        .filter(|entry| !entry.file_name().to_string_lossy().starts_with("xattrop"))
        .count();
    Ok(entry_count)
}
