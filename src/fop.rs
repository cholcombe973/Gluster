extern crate serde_json;

use std::collections::HashMap;
use std::str::FromStr;

use super::GlusterError;

/// A Gluster file operation sample
#[derive(Debug)]
pub struct GlusterFOPSample {
    pub time: String,
    pub fop: GlusterFOP,
    pub fop2: GlusterFOP,
    pub elapsed: String,
    pub xlator: String,
    pub instance_name: String,
    pub username: String,
    pub group: String,
    pub hostname: String,
    pub port: u16,
}

#[derive(Debug)]
pub enum GlusterFOP {
    GfFopOpen,
    GfFopStat,
    GfFopFstat,
    GfFopLookup,
    GfFopAccess,
    GfFopReadlink,
    GfFopOpendir,
    GfFopStatfs,
    GfFopReaddir,
    GfFopReaddirp,
    GfFopCreate,
    GfFopFlush,
    GfFopLk,
    GfFopInodelk,
    GfFopFinodelk,
    GfFopEntrylk,
    GfFopFentrylk,
    GfFopUnlink,
    GfFopSetAttr,
    GfFopFSetAttr,
    GfFopMknod,
    GfFopMkdir,
    GfFopRmdir,
    GfFopSymlink,
    GfFopRename,
    GfFopLink,
    GfFopSetXAttr,
    GfFopGetXAttr,
    GfFopFGetXattr,
    GfFopFSetXAttr,
    GfFopRemovexAttr,
    GfFopFRemovexAttr,
    GfFopIpc,
    GfFopRead,
    GfFopWrite,
    GfFopFsync,
    GfFopTruncate,
    GfFopFTruncate,
    GfFopFSyncDir,
    GfFopXAttrOp,
    GfFopFXAttrOp,
    GfFopRChecksum,
    GfFopZerofill,
    GfFopFallocate,
    GfFopNull,
    GfFopForget,
    GfFopRelease,
    GfFopReleaseDir,
    GfFopGetSpec,
    GfFopMaxValue,
    GfFopDiscard,
    Unknown,
}

impl FromStr for GlusterFOP {
    type Err = GlusterError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NULL" => Ok(GlusterFOP::GfFopNull),
            "STAT" => Ok(GlusterFOP::GfFopStat),
            "READLINK" => Ok(GlusterFOP::GfFopReadlink),
            "MKNOD" => Ok(GlusterFOP::GfFopMknod),
            "MKDIR" => Ok(GlusterFOP::GfFopMkdir),
            "UNLINK" => Ok(GlusterFOP::GfFopUnlink),
            "RMDIR" => Ok(GlusterFOP::GfFopRmdir),
            "SYMLINK" => Ok(GlusterFOP::GfFopSymlink),
            "RENAME" => Ok(GlusterFOP::GfFopRename),
            "LINK" => Ok(GlusterFOP::GfFopLink),
            "TRUNCATE" => Ok(GlusterFOP::GfFopTruncate),
            "OPEN" => Ok(GlusterFOP::GfFopOpen),
            "READ" => Ok(GlusterFOP::GfFopRead),
            "WRITE" => Ok(GlusterFOP::GfFopWrite),
            "STATFS" => Ok(GlusterFOP::GfFopStatfs),
            "FLUSH" => Ok(GlusterFOP::GfFopFlush),
            "FSYNC" => Ok(GlusterFOP::GfFopFsync),
            "SETXATTR" => Ok(GlusterFOP::GfFopSetXAttr),
            "GETXATTR" => Ok(GlusterFOP::GfFopGetXAttr),
            "REMOVEXATTR" => Ok(GlusterFOP::GfFopRemovexAttr),
            "OPENDIR" => Ok(GlusterFOP::GfFopOpendir),
            "FSYNCDIR" => Ok(GlusterFOP::GfFopFSyncDir),
            "ACCESS" => Ok(GlusterFOP::GfFopAccess),
            "CREATE" => Ok(GlusterFOP::GfFopCreate),
            "FTRUNCATE" => Ok(GlusterFOP::GfFopFTruncate),
            "FSTAT" => Ok(GlusterFOP::GfFopFstat),
            "LK" => Ok(GlusterFOP::GfFopLk),
            "LOOKUP" => Ok(GlusterFOP::GfFopLookup),
            "READDIR" => Ok(GlusterFOP::GfFopReaddir),
            "INODELK" => Ok(GlusterFOP::GfFopInodelk),
            "FINODELK" => Ok(GlusterFOP::GfFopFinodelk),
            "ENTRYLK" => Ok(GlusterFOP::GfFopEntrylk),
            "FENTRYLK" => Ok(GlusterFOP::GfFopFentrylk),
            "XATTROP" => Ok(GlusterFOP::GfFopXAttrOp),
            "FXATTROP" => Ok(GlusterFOP::GfFopFXAttrOp),
            "FGETXATTR" => Ok(GlusterFOP::GfFopFGetXattr),
            "FSETXATTR" => Ok(GlusterFOP::GfFopFSetXAttr),
            "RCHECKSUM" => Ok(GlusterFOP::GfFopRChecksum),
            "SETATTR" => Ok(GlusterFOP::GfFopSetAttr),
            "FSETATTR" => Ok(GlusterFOP::GfFopFSetAttr),
            "READDIRP" => Ok(GlusterFOP::GfFopReaddirp),
            "FORGET" => Ok(GlusterFOP::GfFopForget),
            "RELEASE" => Ok(GlusterFOP::GfFopRelease),
            "RELEASEDIR" => Ok(GlusterFOP::GfFopReleaseDir),
            "GETSPEC" => Ok(GlusterFOP::GfFopGetSpec),
            "FREMOVEXATTR" => Ok(GlusterFOP::GfFopFRemovexAttr),
            "FALLOCATE" => Ok(GlusterFOP::GfFopFallocate),
            "DISCARD" => Ok(GlusterFOP::GfFopDiscard),
            "ZEROFILL" => Ok(GlusterFOP::GfFopZerofill),
            "IPC" => Ok(GlusterFOP::GfFopIpc),
            "MAXVALUE" => Ok(GlusterFOP::GfFopMaxValue),
            "HIGH" => Ok(GlusterFOP::GfFopReaddirp),
            "LOW" => Ok(GlusterFOP::GfFopFallocate),
            "NORMAL" => Ok(GlusterFOP::GfFopIpc),
            "LEAST" => Ok(GlusterFOP::GfFopDiscard),
            _ => Err(GlusterError::new(format!("Unknown FOP: {}", s))),
        }
    }
}

impl GlusterFOP {
    fn to_string(&self) -> String {
        match *self {
            GlusterFOP::Unknown => "UNKNOWN".to_string(),
            GlusterFOP::GfFopNull => "NULL".to_string(),
            GlusterFOP::GfFopStat => "STAT".to_string(),
            GlusterFOP::GfFopReadlink => "READLINK".to_string(),
            GlusterFOP::GfFopMknod => "MKNOD".to_string(),
            GlusterFOP::GfFopMkdir => "MKDIR".to_string(),
            GlusterFOP::GfFopUnlink => "UNLINK".to_string(),
            GlusterFOP::GfFopRmdir => "RMDIR".to_string(),
            GlusterFOP::GfFopSymlink => "SYMLINK".to_string(),
            GlusterFOP::GfFopRename => "RENAME".to_string(),
            GlusterFOP::GfFopLink => "LINK".to_string(),
            GlusterFOP::GfFopTruncate => "TRUNCATE".to_string(),
            GlusterFOP::GfFopOpen => "OPEN".to_string(),
            GlusterFOP::GfFopRead => "READ".to_string(),
            GlusterFOP::GfFopWrite => "WRITE".to_string(),
            GlusterFOP::GfFopStatfs => "STATFS".to_string(),
            GlusterFOP::GfFopFlush => "FLUSH".to_string(),
            GlusterFOP::GfFopFsync => "FSYNC".to_string(),
            GlusterFOP::GfFopSetXAttr => "SETXATTR".to_string(),
            GlusterFOP::GfFopGetXAttr => "GETXATTR".to_string(),
            GlusterFOP::GfFopRemovexAttr => "REMOVEXATTR".to_string(),
            GlusterFOP::GfFopOpendir => "OPENDIR".to_string(),
            GlusterFOP::GfFopFSyncDir => "FSYNCDIR".to_string(),
            GlusterFOP::GfFopAccess => "ACCESS".to_string(),
            GlusterFOP::GfFopCreate => "CREATE".to_string(),
            GlusterFOP::GfFopFTruncate => "FTRUNCATE".to_string(),
            GlusterFOP::GfFopFstat => "FSTAT".to_string(),
            GlusterFOP::GfFopLk => "LK".to_string(),
            GlusterFOP::GfFopLookup => "LOOKUP".to_string(),
            GlusterFOP::GfFopReaddir => "READDIR".to_string(),
            GlusterFOP::GfFopInodelk => "INODELK".to_string(),
            GlusterFOP::GfFopFinodelk => "FINODELK".to_string(),
            GlusterFOP::GfFopEntrylk => "ENTRYLK".to_string(),
            GlusterFOP::GfFopFentrylk => "FENTRYLK".to_string(),
            GlusterFOP::GfFopXAttrOp => "XATTROP".to_string(),
            GlusterFOP::GfFopFXAttrOp => "FXATTROP".to_string(),
            GlusterFOP::GfFopFGetXattr => "FGETXATTR".to_string(),
            GlusterFOP::GfFopFSetXAttr => "FSETXATTR".to_string(),
            GlusterFOP::GfFopRChecksum => "RCHECKSUM".to_string(),
            GlusterFOP::GfFopSetAttr => "SETATTR".to_string(),
            GlusterFOP::GfFopFSetAttr => "FSETATTR".to_string(),
            GlusterFOP::GfFopReaddirp => "HIGH".to_string(),
            GlusterFOP::GfFopForget => "FORGET".to_string(),
            GlusterFOP::GfFopRelease => "RELEASE".to_string(),
            GlusterFOP::GfFopReleaseDir => "RELEASEDIR".to_string(),
            GlusterFOP::GfFopGetSpec => "GETSPEC".to_string(),
            GlusterFOP::GfFopFRemovexAttr => "FREMOVEXATTR".to_string(),
            GlusterFOP::GfFopFallocate => "LOW".to_string(),
            GlusterFOP::GfFopDiscard => "LEAST".to_string(),
            GlusterFOP::GfFopZerofill => "ZEROFILL".to_string(),
            GlusterFOP::GfFopIpc => "NORMAL".to_string(),
            GlusterFOP::GfFopMaxValue => "MAXVALUE".to_string(),
        }
    }
}

/// Read the stats from glusterd. This stat is typically located at: /var/lib/glusterd/stats
/// This function removes the mountpoint name from all the keys
pub fn read_aggr_fop(
    json_data: &str,
    filename: &str,
) -> Result<HashMap<String, String>, super::GlusterError> {
    // Remove the mountpoint name from all the keys
    let cleaned_data = json_data.replace(&format!("gluster.brick.{}.aggr.", filename), "");
    let deserialized: HashMap<String, String> = serde_json::from_str(&cleaned_data)?;
    Ok(deserialized)
}

/// Read the stats from glusterd. This stat is typically located at: /var/lib/glusterd/stats
/// This function removes the mountpoint name from all the keys
pub fn read_inter_fop(
    json_data: &str,
    filename: &str,
) -> Result<HashMap<String, String>, super::GlusterError> {
    let cleaned_data = json_data.replace(&format!("gluster.brick.{}.inter.", filename), "");
    let deserialized: HashMap<String, String> = serde_json::from_str(&cleaned_data)?;
    Ok(deserialized)
}

#[test]
fn test_parse_fop_sample() {
    let input = "1485970680.196223,HIGH,LOOKUP,191.0000,/mnt/xvdf,N/A,root,root,ip-172-31-32-197,\
                 49079";
    let result = parse_fop_sample(&input).unwrap();
    println!("fop_sample: {:?}", result);
}

fn parse_fop_sample(input: &str) -> Result<GlusterFOPSample, GlusterError> {
    let parts: Vec<&str> = input.split(',').collect();
    if parts.len() != 10 {
        return Err(GlusterError::new(format!(
            "Invalid FOP sample: {}.  Unable to parse",
            input
        )));
    }
    Ok(GlusterFOPSample {
        time: parts[0].to_string(),
        fop: parts[1].parse().unwrap(),
        fop2: parts[2].parse().unwrap(),
        elapsed: parts[3].to_string(),
        xlator: parts[4].to_string(),
        instance_name: parts[5].to_string(),
        username: parts[6].to_string(),
        group: parts[7].to_string(),
        hostname: parts[8].to_string(),
        port: parts[9].parse().unwrap(),
    })
}
