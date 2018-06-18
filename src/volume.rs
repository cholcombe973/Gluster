extern crate serde;
extern crate serde_xml_rs;

use std::ascii::AsciiExt;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io::Cursor;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use self::serde_xml_rs::deserialize;
use super::{process_output, resolve_to_ip, run_command, translate_to_bytes, BitrotOption,
            BrickStatus, GlusterError, GlusterOption, Quota};
use byteorder::{BigEndian, ReadBytesExt};
use peer::{get_peer, Peer, State};
use regex::Regex;
use rpc;
use rpc::{Pack, UnPack};
use unix_socket::UnixStream;
use uuid::Uuid;

/// A Gluster Brick consists of a Peer and a path to the mount point
#[derive(Clone, Eq, PartialEq)]
pub struct Brick {
    pub peer: Peer,
    pub path: PathBuf,
}

impl Brick {
    /// Returns a String representation of the selected enum variant.
    pub fn to_string(&self) -> String {
        format!(
            "{}:{}",
            self.peer.hostname.clone(),
            self.path.to_string_lossy()
        )
    }
}

impl fmt::Debug for Brick {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}:{:?}", self.peer.hostname, self.path.to_str())
    }
}

/// An enum to select the transport method Gluster should use for the Volume
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Transport {
    Tcp,
    Rdma,
    TcpAndRdma,
}

impl Transport {
    /// Create a new Transport from a str.
    fn new(name: &str) -> Transport {
        match name.trim().to_ascii_lowercase().as_ref() {
            "tcp" => Transport::Tcp,
            "tcp,rdma" => Transport::TcpAndRdma,
            "rdma" => Transport::Rdma,
            _ => Transport::Tcp,
        }
    }
    /// Returns a String representation of the selected enum variant.
    fn to_string(self) -> String {
        match self {
            Transport::Rdma => "rdma".to_string(),
            Transport::Tcp => "tcp".to_string(),
            Transport::TcpAndRdma => "tcp,rdma".to_string(),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum VolumeTranslator {
    Arbiter,
    Disperse,
    Replica,
    Redundancy,
    Stripe,
}

impl VolumeTranslator {
    /// Returns a String representation of the selected enum variant.
    fn to_string(self) -> String {
        match self {
            VolumeTranslator::Arbiter => "arbiter".to_string(),
            VolumeTranslator::Disperse => "disperse".to_string(),
            VolumeTranslator::Replica => "replica".to_string(),
            VolumeTranslator::Redundancy => "redundancy".to_string(),
            VolumeTranslator::Stripe => "stripe".to_string(),
        }
    }
}

/// These are all the different Volume types that are possible in Gluster
/// Note: Tier is not represented here because I'm waiting for it to become
/// more stable
/// For more information about these types see: [Gluster Volume]
/// (https://gluster.readthedocs.
/// org/en/latest/Administrator%20Guide/Setting%20Up%20Volumes/)
#[derive(Debug, Deserialize, Eq, PartialEq)]
pub enum VolumeType {
    #[serde(rename = "Arbiter")]
    Arbiter,
    #[serde(rename = "Distribute")]
    Distribute,
    #[serde(rename = "Stripe")]
    Stripe,
    #[serde(rename = "Replicate")]
    Replicate,
    #[serde(rename = "Striped-Replicate")]
    StripedAndReplicate,
    #[serde(rename = "Disperse")]
    Disperse,
    // Tier,
    #[serde(rename = "Distributed-Stripe")]
    DistributedAndStripe,
    #[serde(rename = "Distributed-Replicate")]
    DistributedAndReplicate,
    #[serde(rename = "Distributed-Striped-Replicate")]
    DistributedAndStripedAndReplicate,
    #[serde(rename = "Distributed-Disperse")]
    DistributedAndDisperse,
}

impl VolumeType {
    /// Constructs a new VolumeType from a &str
    pub fn new(name: &str) -> VolumeType {
        match name.trim().to_ascii_lowercase().as_ref() {
            "arbiter" => VolumeType::Arbiter,
            "distribute" => VolumeType::Distribute,
            "stripe" => VolumeType::Stripe,
            "replicate" => VolumeType::Replicate,
            "striped-replicate" => VolumeType::StripedAndReplicate,
            "disperse" => VolumeType::Disperse,
            // "Tier" => VolumeType::Tier, //TODO: Waiting for this to become stable
            "distributed-stripe" => VolumeType::DistributedAndStripe,
            "distributed-replicate" => VolumeType::DistributedAndReplicate,
            "distributed-striped-replicate" => VolumeType::DistributedAndStripedAndReplicate,
            "distributed-disperse" => VolumeType::DistributedAndDisperse,
            _ => VolumeType::Replicate,
        }
    }

    /// Returns a enum variant of the given String.
    pub fn from_str(vol_type: &str) -> VolumeType {
        match vol_type {
            "Arbiter" => VolumeType::Arbiter,
            "Distribute" => VolumeType::Distribute,
            "Stripe" => VolumeType::Stripe,
            "Replicate" => VolumeType::Replicate,
            "Striped-Replicate" => VolumeType::StripedAndReplicate,
            "Disperse" => VolumeType::Disperse,
            // VolumeType::Tier => "Tier".to_string(), //TODO: Waiting for this to become stable
            "Distributed-Stripe" => VolumeType::DistributedAndStripe,
            "Distributed-Replicate" => VolumeType::DistributedAndReplicate,
            "Distributed-Striped-Replicate" => VolumeType::DistributedAndStripedAndReplicate,
            "Distributed-Disperse" => VolumeType::DistributedAndDisperse,
            _ => VolumeType::Replicate,
        }
    }

    /// Returns a String representation of the selected enum variant.
    pub fn to_string(self) -> String {
        match self {
            VolumeType::Arbiter => "Replicate".to_string(),
            VolumeType::Distribute => "Distribute".to_string(),
            VolumeType::Stripe => "Stripe".to_string(),
            VolumeType::Replicate => "Replicate".to_string(),
            VolumeType::StripedAndReplicate => "Striped-Replicate".to_string(),
            VolumeType::Disperse => "Disperse".to_string(),
            // VolumeType::Tier => "Tier".to_string(), //TODO: Waiting for this to become stable
            VolumeType::DistributedAndStripe => "Distributed-Stripe".to_string(),
            VolumeType::DistributedAndReplicate => "Distributed-Replicate".to_string(),
            VolumeType::DistributedAndStripedAndReplicate => {
                "Distributed-Striped-Replicate".to_string()
            }
            VolumeType::DistributedAndDisperse => "Distributed-Disperse".to_string(),
        }
    }
}

/// A volume is a logical collection of bricks. Most of the gluster management
/// operations
/// happen on the volume.
#[derive(Debug, Eq, PartialEq)]
pub struct Volume {
    /// The name of the volume
    pub name: String,
    /// The type of the volume
    pub vol_type: VolumeType,
    /// The unique id of the volume
    pub id: Uuid,
    pub status: String,
    /// The underlying Transport mechanism
    pub transport: Transport,
    /// A Vec containing all the Brick's that are in the Volume
    pub bricks: Vec<Brick>,
    /// A Vec containing a tuple of options that are configured on this Volume
    pub options: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub struct BrickXml {
    pub name: String,
    #[serde(rename = "hostUuid")]
    pub host_uuid: Uuid,
    #[serde(rename = "isArbiter")]
    pub is_arbiter: String,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub struct VolumeCliXml {
    #[serde(rename = "opRet")]
    pub ret: i32,
    #[serde(rename = "opErrno")]
    pub errno: i32,
    #[serde(rename = "opErrStr")]
    pub err_str: Option<String>,
    #[serde(rename = "volInfo")]
    pub volumes: XmlVolumes,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub struct XmlVolumes {
    pub volumes: Blah,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub struct Blah {
    pub volume: Vec<VolumeXml>,
    pub count: u64,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub struct VolumeXml {
    pub name: String,
    pub id: Uuid,
    pub status: String,
    #[serde(rename = "statusStr")]
    pub status_str: String,
    #[serde(rename = "snapshotCount")]
    pub snapshot_count: String,
    #[serde(rename = "brickCount")]
    pub brick_count: String,
    #[serde(rename = "distCount")]
    pub dist_count: String,
    #[serde(rename = "stripeCount")]
    pub stripe_count: String,
    #[serde(rename = "replicaCount")]
    pub replica_count: String,
    #[serde(rename = "arbiterCount")]
    pub arbiter_count: String,
    #[serde(rename = "disperseCount")]
    pub disperse_count: String,
    #[serde(rename = "redundancyCount")]
    redundancy_count: String,
    #[serde(rename = "type")]
    pub vol_type: String,
    #[serde(rename = "typeStr")]
    pub type_str: VolumeType,
    pub transport: String,
    pub xlators: Option<String>,
    pub bricks: Vec<String>,
    #[serde(rename = "optCount")]
    pub option_count: String,
}

#[test]
fn test_parse_volume_info_xml() {
    use std::fs::File;
    use std::io::Read;

    /*
    let test_data = {
        let mut f = File::open("tests/volume_info.xml").unwrap();
        let mut s = String::new();
        f.read_to_string(&mut s).unwrap();
        s
    };
    let result: VolumeCliXml = deserialize(test_data.as_bytes()).unwrap();
    println!("vol_info_xml: {:?}", result);
    */
    /*
    let vol_info = VolumeXml {
        name: "test".to_string(),
        vol_type: VolumeType::Replicate,
        id: Uuid::parse_str("cae6868d-b080-4ea3-927b-93b5f1e3fe69").unwrap(),
        status: "Started".to_string(),
        transport: Transport::Tcp,
        bricks: vec![
            Brick {
                peer: Peer {
                    uuid: Uuid::parse_str("78f68270-201a-4d8a-bad3-7cded6e6b7d8").unwrap(),
                    hostname: "test_ip".to_string(),
                    status: State::Connected,
                },
                path: PathBuf::from("/mnt/xvdf"),
            },
        ],
        options: options_map,
    };
    */
    //println!("vol_info_xml: {:?}", result);
    //assert_eq!(vol_info, result);
}

// Volume Name: test
// Type: Replicate
// Volume ID: cae6868d-b080-4ea3-927b-93b5f1e3fe69
// Status: Started
// Number of Bricks: 1 x 2 = 2
// Transport-type: tcp
// Bricks:
// Brick1: 172.31.41.135:/mnt/xvdf
// Brick2: 172.31.26.65:/mnt/xvdf
// Options Reconfigured:
// features.inode-quota: off
// features.quota: off
// transport.address-family: inet
// performance.readdir-ahead: on
// nfs.disable: on
//
enum ParseState {
    Root,
    Bricks,
    Options,
}

/// Lists all available volume names.
/// # Failures
/// Will return None if the Volume list command failed or if volume could not
/// be transformed
/// into a String from utf8
pub fn volume_list() -> Option<Vec<String>> {
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("list".to_string());
    let output = run_command("gluster", &arg_list, true, false);
    let status = output.status;

    if !status.success() {
        debug!("Volume list get command failed");
        return None;
    }
    let output_str: String = match String::from_utf8(output.stdout) {
        Ok(n) => n,
        Err(_) => {
            debug!("Volume list output transformation to utf8 failed");
            return None;
        }
    };
    let mut volume_names: Vec<String> = Vec::new();
    for line in output_str.lines() {
        if line.is_empty() {
            // Skip any blank lines in the output
            continue;
        }
        volume_names.push(line.trim().to_string());
    }
    return Some(volume_names);
}

#[test]
fn test_parse_volume_info() {
    let test_data = r#"

Volume Name: test
Type: Replicate
Volume ID: cae6868d-b080-4ea3-927b-93b5f1e3fe69
Status: Started
Number of Bricks: 1 x 2 = 2
Transport-type: tcp
Bricks:
Brick1: 172.31.41.135:/mnt/xvdf
Options Reconfigured:
features.inode-quota: off
features.quota: off
transport.address-family: inet
performance.readdir-ahead: on
nfs.disable: on
"#;
    let result = parse_volume_info("test", test_data.to_string()).unwrap();
    let mut options_map: BTreeMap<String, String> = BTreeMap::new();
    options_map.insert("features.inode-quota".to_string(), "off".to_string());
    options_map.insert("features.quota".to_string(), "off".to_string());
    options_map.insert("transport.address-family".to_string(), "inet".to_string());
    options_map.insert("performance.readdir-ahead".to_string(), "on".to_string());
    options_map.insert("nfs.disable".to_string(), "on".to_string());

    let vol_info = Volume {
        name: "test".to_string(),
        vol_type: VolumeType::Replicate,
        id: Uuid::parse_str("cae6868d-b080-4ea3-927b-93b5f1e3fe69").unwrap(),
        status: "Started".to_string(),
        transport: Transport::Tcp,
        bricks: vec![Brick {
            peer: Peer {
                uuid: Uuid::parse_str("78f68270-201a-4d8a-bad3-7cded6e6b7d8").unwrap(),
                hostname: "test_ip".to_string(),
                status: State::Connected,
            },
            path: PathBuf::from("/mnt/xvdf"),
        }],
        options: options_map,
    };
    println!("vol_info: {:?}", vol_info);
    assert_eq!(vol_info, result);
}

fn parse_volume_info(volume: &str, output_str: String) -> Result<Volume, GlusterError> {
    // Variables we will return in a struct
    let mut transport_type = String::new();
    let mut volume_type = String::new();
    let mut volume_name = String::new();
    let mut volume_options: BTreeMap<String, String> = BTreeMap::new();
    let mut status = String::new();
    let mut bricks: Vec<Brick> = Vec::new();
    let mut id = Uuid::nil();

    if output_str.trim() == "No volumes present" {
        debug!("No volumes present");
        println!("No volumes present");
        return Err(GlusterError::NoVolumesPresent);
    }

    if output_str.trim() == format!("Volume {} does not exist", volume) {
        debug!("Volume {} does not exist", volume);
        println!("Volume {} does not exist", volume);
        return Err(GlusterError::new(format!(
            "Volume: {} does not exist",
            volume
        )));
    }

    let mut parser_state = ParseState::Root;

    for line in output_str.lines() {
        if line.is_empty() {
            // Skip the first blank line in the output
            continue;
        }
        match line {
            "Bricks:" => {
                parser_state = ParseState::Bricks;
                continue;
            }
            "Options Reconfigured:" => {
                parser_state = ParseState::Options;
                continue;
            }
            _ => {}
        };
        match parser_state {
            ParseState::Root => {
                let parts: Vec<String> = line.split(": ").map(|e| e.to_string()).collect();
                if parts.len() < 2 {
                    // We don't know what this is
                    continue;
                }
                let ref name = parts[0];
                let ref value = parts[1];

                if name == "Volume Name" {
                    volume_name = value.to_owned();
                }
                if name == "Type" {
                    volume_type = value.to_owned();
                }
                if name == "Volume ID" {
                    id = try!(Uuid::parse_str(&value));
                }
                if name == "Status" {
                    status = value.to_owned();
                }
                if name == "Transport-Type" {
                    transport_type = value.to_owned();
                }
                if name == "Number of Bricks" {}
            }
            ParseState::Bricks => {
                let parts: Vec<String> = line.split(": ").map(|e| e.to_string()).collect();
                if parts.len() < 2 {
                    // We don't know what this is
                    continue;
                }
                let ref value = parts[1];

                // let brick_str = value;
                let brick_parts: Vec<&str> = value.split(":").collect();
                assert!(
                    brick_parts.len() == 2,
                    "Failed to parse bricks from gluster vol info"
                );

                let mut hostname = brick_parts[0].trim().to_string();

                // Translate back into an IP address if needed
                let check_for_ip = hostname.parse::<IpAddr>();

                if check_for_ip.is_err() {
                    // It's a hostname so lets resolve it
                    hostname = match resolve_to_ip(&hostname) {
                        Ok(ip_addr) => ip_addr,
                        Err(e) => {
                            return Err(GlusterError::new(format!(
                                "Failed to resolve hostname: \
                                 {}. Error: {}",
                                &hostname, e
                            )));
                        }
                    };
                }

                let peer: Peer = try!(get_peer(&hostname.to_string()));
                debug!("get_peer_by_ipaddr result: Peer: {:?}", peer);
                let brick = Brick {
                    // Should this panic if it doesn't work?
                    peer: peer,
                    path: PathBuf::from(brick_parts[1].to_string()),
                };
                bricks.push(brick);
            }
            ParseState::Options => {
                // Parse the options
                let parts: Vec<String> = line.split(": ").map(|e| e.to_string()).collect();
                if parts.len() < 2 {
                    // We don't know what this is
                    continue;
                }
                volume_options.insert(parts[0].clone(), parts[1].clone());
            }
        }
    }

    let transport = Transport::new(&transport_type);
    let vol_type = VolumeType::new(&volume_type);
    let vol_info = Volume {
        name: volume_name,
        vol_type: vol_type,
        id: id,
        status: status,
        transport: transport,
        bricks: bricks,
        options: volume_options,
    };
    return Ok(vol_info);
}

/// Returns a Volume with all available information on the volume
/// # Failures
/// Will return GlusterError if the command failed to run.
pub fn volume_info(volume: &str) -> Result<Volume, GlusterError> {
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("info".to_string());
    arg_list.push(volume.to_string());
    let output = run_command("gluster", &arg_list, true, false);
    let status = output.status;

    if !status.success() {
        debug!("Volume info get command failed");
        println!(
            "Volume info get command failed with error: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        // TODO: What is the appropriate error to report here?
        // The client is using this to figure out if it should make a volume
        return Err(GlusterError::NoVolumesPresent);
    }
    let output_str: String = try!(String::from_utf8(output.stdout));

    return parse_volume_info(&volume, output_str);
}

/// Returns a u64 representing the bytes used on the volume.
/// Note: This uses my brand new RPC library.  Some bugs may exist so use
/// caution.  This does not
/// shell out and therefore should be significantly faster.  It also suffers
/// far less hang conditions
/// than the CLI version.
/// # Failures
/// Will return GlusterError if the RPC fails
pub fn get_quota_usage(volume: &str) -> Result<u64, GlusterError> {
    let xid = 1; //Transaction ID number.
    let prog = rpc::GLUSTER_QUOTA_PROGRAM_NUMBER;
    let vers = 1; //RPC version == 1

    let verf = rpc::GlusterAuth {
        flavor: rpc::AuthFlavor::AuthNull,
        stuff: vec![0, 0, 0, 0],
    };
    let verf_bytes = try!(verf.pack());

    let creds = rpc::GlusterCred {
        flavor: rpc::GLUSTER_V2_CRED_FLAVOR,
        pid: 0,
        uid: 0,
        gid: 0,
        groups: "".to_string(),
        lock_owner: vec![0, 0, 0, 0],
    };
    let cred_bytes = try!(creds.pack());

    let mut call_bytes = try!(rpc::pack_quota_callheader(
        xid,
        prog,
        vers,
        rpc::GlusterAggregatorCommand::GlusterAggregatorGetlimit,
        cred_bytes,
        verf_bytes,
    ));

    let mut dict: HashMap<String, Vec<u8>> = HashMap::with_capacity(4);

    // TODO: Make a Gluster wd RPC call and parse this from the quota.conf file
    // This is crap
    let mut gfid = "00000000-0000-0000-0000-000000000001"
        .to_string()
        .into_bytes();
    gfid.push(0); //Null Terminate
    let mut name = volume.to_string().into_bytes();
    name.push(0); //Null Terminate
    let mut version = "1.20000005".to_string().into_bytes();
    version.push(0); //Null Terminate
                     //No idea what vol_type == 5 means to Gluster
    let mut vol_type = "5".to_string().into_bytes();
    vol_type.push(0); //Null Terminate

    dict.insert("gfid".to_string(), gfid);
    dict.insert("type".to_string(), vol_type);
    dict.insert("volume-uuid".to_string(), name);
    dict.insert("version".to_string(), version);
    let quota_request = rpc::GlusterCliRequest { dict: dict };
    let quota_bytes = try!(quota_request.pack());
    for byte in quota_bytes {
        call_bytes.push(byte);
    }

    // Ok.. we need to hunt down the quota socket file ..crap..
    let addr = Path::new("/var/run/gluster/quotad.socket");
    let mut sock = try!(UnixStream::connect(&addr));

    let send_bytes = try!(rpc::sendrecord(&mut sock, &call_bytes));
    let mut reply_bytes = try!(rpc::recvrecord(&mut sock));

    let mut cursor = Cursor::new(&mut reply_bytes[..]);

    // Check for success
    try!(rpc::unpack_replyheader(&mut cursor));

    let mut cli_response = try!(rpc::GlusterCliResponse::unpack(&mut cursor));
    // The raw bytes
    let mut quota_size_bytes = match cli_response.dict.get_mut("trusted.glusterfs.quota.size") {
        Some(s) => s,
        None => {
            return Err(GlusterError::new(
                "trusted.glusterfs.quota.size was not returned from \
                 quotad"
                    .to_string(),
            ));
        }
    };
    // Gluster is crazy and encodes a ton of data in this vector.  We're just going
    // to
    // read the first value and throw away the rest.  Why they didn't just use a
    // struct and
    // XDR is beyond me
    let mut size_cursor = Cursor::new(&mut quota_size_bytes[..]);
    let usage = try!(size_cursor.read_u64::<BigEndian>());
    return Ok(usage);
}

/// Return a list of quotas on the volume if any
/// # Failures
/// Will return GlusterError if the command failed to run.
pub fn quota_list(volume: &str) -> Result<Vec<Quota>, GlusterError> {
    let mut args_list: Vec<String> = Vec::new();
    args_list.push("volume".to_string());
    args_list.push("quota".to_string());
    args_list.push(volume.to_string());
    args_list.push("list".to_string());

    let output = run_command("gluster", &args_list, true, false);
    let status = output.status;

    if !status.success() {
        debug!(
            "Volume quota list command failed with error: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        return Err(GlusterError::new(
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }
    let output_str: String = try!(String::from_utf8(output.stdout));
    let quota_list = parse_quota_list(volume, output_str);

    Ok(quota_list)
}

#[test]
fn test_quota_list() {
    let test_data = r#"
    Path Hard-limit  Soft-limit      Used  Available  Soft-limit exceeded? Hard-limit exceeded?
----------------------------------------------------------------------------------------------
/ 1.0KB  80%(819Bytes)   0Bytes   1.0KB              No                   No
"#;
    let result = parse_quota_list("test", test_data.to_string());
    let quotas = vec![Quota {
        path: PathBuf::from("/"),
        limit: 1024,
        used: 0,
    }];
    println!("quota_list: {:?}", result);
    assert_eq!(quotas, result);
}

/// Return a list of quotas on the volume if any
// ThinkPad-T410s:~# gluster vol quota test list
// Path                   Hard-limit Soft-limit   Used  Available  Soft-limit
// exceeded? Hard-limit exceeded?
// ---------------------------------------------------------------------------
// /                                        100.0MB       80%      0Bytes
// 100.0MB              No                   No
//
// There are 2 ways to get quota information
// 1. List the quota's with the quota list command.  This command has been
// known in the past to hang
// in certain situations.
// 2. Issue an RPC directly to Gluster
//
fn parse_quota_list(volume: &str, output_str: String) -> Vec<Quota> {
    // ThinkPad-T410s:~# gluster vol quota test list
    // Path                   Hard-limit Soft-limit   Used  Available  Soft-limit
    // exceeded? Hard-limit exceeded?
    // --------------------------------------------------------------------------
    // /                                        100.0MB       80%      0Bytes
    // 100.0MB              No                   No
    //
    // There are 2 ways to get quota information
    // 1. List the quota's with the quota list command.  This command has been
    // known in the past to hang
    // in certain situations.
    // 2. Go to the backend brick and getfattr -d -e hex -m . dir_name/ on the
    // directory directly:
    // /mnt/x1# getfattr -d -e hex -m . quota/
    // # file: quota/
    // trusted.gfid=0xdb2443e4742e4aaf844eee40405ad7ae
    // trusted.glusterfs.dht=0x000000010000000000000000ffffffff
    // trusted.glusterfs.quota.00000000-0000-0000-0000-000000000001.
    // contri=0x0000000000000000
    // trusted.glusterfs.quota.dirty=0x3000
    // trusted.glusterfs.quota.limit-set=0x0000000006400000ffffffffffffffff
    // trusted.glusterfs.quota.size=0x0000000000000000
    // TODO: link to the c xattr library #include <sys/xattr.h> and implement
    // method 2
    //
    let mut quota_list = Vec::new();

    if output_str.trim() == format!("quota: No quota configured on volume {}", volume) {
        return quota_list;
    }
    for line in output_str.lines() {
        if line.is_empty() {
            // Skip the first blank line in the output
            continue;
        }
        if line.starts_with(" ") {
            continue;
        }
        if line.starts_with("-") {
            continue;
        }
        // Ok now that we've eliminated the garbage
        let parts: Vec<&str> = line.split(" ")
            .filter(|s| !s.is_empty())
            .collect::<Vec<&str>>();
        // Output should match: ["/", "100.0MB", "80%", "0Bytes", "100.0MB", "No", "No"]
        if parts.len() > 3 {
            let limit: f64 = match translate_to_bytes(parts[1]) {
                Some(v) => v,
                None => 0.0,
            };
            let used: f64 = match translate_to_bytes(parts[3]) {
                Some(v) => v,
                None => 0.0,
            };
            let quota = Quota {
                path: PathBuf::from(parts[0].to_string()),
                limit: limit as u64,
                used: used as u64,
            };
            quota_list.push(quota);
        }
        // else?
    }
    return quota_list;
}

/// Enable bitrot detection and remediation on the volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_enable_bitrot(volume: &str) -> Result<i32, GlusterError> {
    let arg_list: Vec<&str> = vec!["volume", "bitrot", volume, "enable"];
    return process_output(run_command("gluster", &arg_list, true, false));
}

/// Disable bitrot detection and remediation on the volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_disable_bitrot(volume: &str) -> Result<i32, GlusterError> {
    let arg_list: Vec<&str> = vec!["volume", "bitrot", volume, "disable"];
    return process_output(run_command("gluster", &arg_list, true, false));
}

/// Set a bitrot option on the volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_set_bitrot_option(volume: &str, setting: BitrotOption) -> Result<i32, GlusterError> {
    let arg_list: Vec<String> = vec![
        "volume".to_string(),
        "bitrot".to_string(),
        volume.to_string(),
        setting.to_string(),
        setting.value(),
    ];
    return process_output(run_command("gluster", &arg_list, true, true));
}

/// Enable quotas on the volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_enable_quotas(volume: &str) -> Result<i32, GlusterError> {
    let arg_list: Vec<&str> = vec!["volume", "quota", volume, "enable"];
    return process_output(run_command("gluster", &arg_list, true, false));
}

/// Check if quotas are already enabled on a volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_quotas_enabled(volume: &str) -> Result<bool, GlusterError> {
    let vol_info = try!(volume_info(volume));
    let quota = vol_info.options.get("features.quota");
    match quota {
        Some(v) => {
            if v == "off" {
                return Ok(false);
            } else if v == "on" {
                return Ok(true);
            } else {
                // No idea what this is
                return Ok(false);
            }
        }
        None => Ok(false),
    }
}

/// Disable quotas on the volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_disable_quotas(volume: &str) -> Result<i32, GlusterError> {
    let arg_list: Vec<&str> = vec!["volume", "quota", volume, "disable"];
    return process_output(run_command("gluster", &arg_list, true, false));
}

/// Removes a size quota to the volume and path.
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_remove_quota(volume: &str, path: &Path) -> Result<i32, GlusterError> {
    let path_str = format!("{}", path.display());
    let arg_list: Vec<&str> = vec!["volume", "quota", volume, "remove", &path_str];
    return process_output(run_command("gluster", &arg_list, true, false));
}

/// Adds a size quota to the volume and path.
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_add_quota(volume: &str, path: &Path, size: u64) -> Result<i32, GlusterError> {
    let path_str = format!("{}", path.display());
    let size_string = size.to_string();
    let arg_list: Vec<&str> = vec![
        "volume",
        "quota",
        volume,
        "limit-usage",
        &path_str,
        &size_string,
    ];

    return process_output(run_command("gluster", &arg_list, true, false));
}
#[test]
fn test_parse_volume_status() {
    let test_data = r#"
    Gluster process                             TCP Port  RDMA Port  Online  Pid
    ------------------------------------------------------------------------------
    Brick 172.31.46.33:/mnt/xvdf                49152     0          Y       14228
    Brick 172.31.19.130:/mnt/xvdf               49152     0          Y       14446
    Self-heal Daemon on localhost               N/A       N/A        Y       14248
    Self-heal Daemon on ip-172-31-19-130.us-wes
    t-2.compute.internal                        N/A       N/A        Y       14466

    Task Status of Volume test
    ------------------------------------------------------------------------------
    There are no active volume tasks

"#;
    let result = parse_volume_status(test_data.to_string()).unwrap();
    println!("status: {:?}", result);
    // Have to inspect these manually because the UUID is randomly generated by the parser.
    // It's either that or it has to be set to some fixed UUID.  Neither solution seems good
    assert_eq!(result[0].brick.peer.hostname, "172.31.46.33".to_string());
    assert_eq!(result[0].tcp_port, 49152);
    assert_eq!(result[0].rdma_port, 0);
    assert_eq!(result[0].online, true);
    assert_eq!(result[0].pid, 14228);

    assert_eq!(result[1].brick.peer.hostname, "172.31.19.130".to_string());
    assert_eq!(result[1].tcp_port, 49152);
    assert_eq!(result[1].rdma_port, 0);
    assert_eq!(result[1].online, true);
    assert_eq!(result[1].pid, 14446);
}

/// Based on the replicas or erasure bits that are still available in the
/// volume this will return
/// True or False as to whether you can remove a Brick. This should be called
/// before volume_remove_brick()
pub fn ok_to_remove(volume: &str, brick: &Brick) -> Result<bool, GlusterError> {
    // TODO: switch over to native RPC call to eliminate String regex parsing
    let arg_list: Vec<&str> = vec!["vol", "status", volume];

    let output = run_command("gluster", &arg_list, true, false);
    if !output.status.success() {
        let stderr = try!(String::from_utf8(output.stderr));
        return Err(GlusterError::new(stderr));
    }

    let output_str = try!(String::from_utf8(output.stdout));
    let bricks = try!(parse_volume_status(output_str));
    // The redudancy requirement is needed here.  The code needs to understand what
    // volume type
    // it's operating on.
    return Ok(true);
}

// pub fn volume_shrink_replicated(volume: &str,
// replica_count: usize,
// bricks: Vec<Brick>,
// force: bool) -> Result<i32,String> {
// volume remove-brick <VOLNAME> [replica <COUNT>] <BRICK> ...
// <start|stop|status|c
// ommit|force> - remove brick from volume <VOLNAME>
// }
//
fn parse_volume_status(output_str: String) -> Result<Vec<BrickStatus>, GlusterError> {
    // Sample output
    // Status of volume: test
    // Gluster process                             TCP Port  RDMA Port  Online  Pid
    // ------------------------------------------------------------------------------
    // Brick 192.168.1.6:/mnt/brick2               49154     0          Y
    // 14940
    // Brick 192.168.1.6:/mnt/brick3               49155     0          Y
    // 14947
    //
    let mut bricks: Vec<BrickStatus> = Vec::new();
    for line in output_str.lines() {
        // Skip the header crap
        if line.starts_with("Status") {
            continue;
        }
        if line.starts_with("Gluster") {
            continue;
        }
        if line.starts_with("-") {
            continue;
        }
        let regex_str = r#"Brick\s+(?P<hostname>[a-zA-Z0-9.]+)
:(?P<path>[/a-zA-z0-9]+)
\s+(?P<tcp>[0-9]+)\s+(?P<rdma>[0-9]+)\s+(?P<online>[Y,N])\s+(?P<pid>[0-9]+)"#;
        let brick_regex = try!(Regex::new(&regex_str.replace("\n", "")));
        match brick_regex.captures(&line) {
            Some(result) => {
                let tcp_port = match result.name("tcp") {
                    Some(port) => port,
                    None => {
                        return Err(GlusterError::new(
                            "Unable to find tcp port in gluster vol \
                             status output"
                                .to_string(),
                        ));
                    }
                };

                let peer = Peer {
                    uuid: Uuid::new_v4(),
                    hostname: result.name("hostname").unwrap().to_string(),
                    status: State::Unknown,
                };

                let brick = Brick {
                    peer: peer,
                    path: PathBuf::from(result.name("path").unwrap()),
                };

                let online = match result.name("online").unwrap() {
                    "Y" => true,
                    "N" => false,
                    _ => false,
                };

                let status = BrickStatus {
                    brick: brick,
                    tcp_port: try!(u16::from_str(result.name("tcp").unwrap())),
                    rdma_port: try!(u16::from_str(result.name("rdma").unwrap())),
                    online: online,
                    pid: try!(u16::from_str(result.name("pid").unwrap())),
                };
                bricks.push(status);
            }
            None => {}
        }
    }
    return Ok(bricks);
}

/// Query the status of the volume given.
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_status(volume: &str) -> Result<Vec<BrickStatus>, GlusterError> {
    let arg_list: Vec<&str> = vec!["vol", "status", volume];

    let output = run_command("gluster", &arg_list, true, false);
    if !output.status.success() {
        let stderr = try!(String::from_utf8(output.stderr));
        return Err(GlusterError::new(stderr));
    }

    let output_str = try!(String::from_utf8(output.stdout));
    let bricks = try!(parse_volume_status(output_str));

    Ok(bricks)
}
// pub fn volume_shrink_replicated(volume: &str,
// replica_count: usize,
// bricks: Vec<Brick>,
// force: bool) -> Result<i32,String> {
// volume remove-brick <VOLNAME> [replica <COUNT>] <BRICK> ...
// <start|stop|status|c
// ommit|force> - remove brick from volume <VOLNAME>
// }
//

/// This will remove a brick from the volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_remove_brick(
    volume: &str,
    bricks: Vec<Brick>,
    force: bool,
) -> Result<i32, GlusterError> {
    if bricks.is_empty() {
        return Err(GlusterError::new(
            "The brick list is empty. Not shrinking volume".to_string(),
        ));
    }

    for brick in bricks {
        let ok = try!(ok_to_remove(&volume, &brick));
        if ok {
            let mut arg_list: Vec<&str> = vec!["volume", "remove-brick", volume];

            if force {
                arg_list.push("force");
            }
            arg_list.push("start");

            let status = process_output(run_command("gluster", &arg_list, true, true));
        } else {
            return Err(GlusterError::new(
                "Unable to remove brick due to redundancy failure".to_string(),
            ));
        }
    }
    return Ok(0);
}

// volume add-brick <VOLNAME> [<stripe|replica> <COUNT>]
// <NEW-BRICK> ... [force] - add brick to volume <VOLNAME>
/// This adds a new brick to the volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_add_brick(
    volume: &str,
    bricks: Vec<Brick>,
    force: bool,
) -> Result<i32, GlusterError> {
    if bricks.is_empty() {
        return Err(GlusterError::new(
            "The brick list is empty. Not expanding volume".to_string(),
        ));
    }

    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("add-brick".to_string());
    arg_list.push(volume.to_string());

    for brick in bricks.iter() {
        arg_list.push(brick.to_string());
    }
    if force {
        arg_list.push("force".to_string());
    }
    return process_output(run_command("gluster", &arg_list, true, true));
}

/// Once a volume is created it needs to be started.  This starts the volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_start(volume: &str, force: bool) -> Result<i32, GlusterError> {
    // Should I check the volume exists first?
    let mut arg_list: Vec<&str> = vec!["volume", "start", volume];

    if force {
        arg_list.push("force");
    }
    return process_output(run_command("gluster", &arg_list, true, true));
}

/// This stops a running volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_stop(volume: &str, force: bool) -> Result<i32, GlusterError> {
    let mut arg_list: Vec<&str> = vec!["volume", "stop", volume];

    if force {
        arg_list.push("force");
    }
    return process_output(run_command("gluster", &arg_list, true, true));
}

/// This deletes a stopped volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_delete(volume: &str) -> Result<i32, GlusterError> {
    let arg_list: Vec<&str> = vec!["volume", "delete", volume];

    return process_output(run_command("gluster", &arg_list, true, true));
}

/// This function doesn't do anything yet.  It is a place holder because
/// volume_rebalance
/// is a long running command and I haven't decided how to poll for completion
/// yet
pub fn volume_rebalance(volume: &str) {
    // Usage: volume rebalance <VOLNAME> {{fix-layout start} | {start
    // [force]|stop|status}}
}

fn volume_create<T: ToString>(
    volume: &str,
    options: HashMap<VolumeTranslator, T>,
    transport: &Transport,
    bricks: Vec<Brick>,
    force: bool,
) -> Result<i32, GlusterError> {
    if bricks.is_empty() {
        return Err(GlusterError::new(
            "The brick list is empty. Not creating volume".to_string(),
        ));
    }

    // TODO: figure out how to check each VolumeTranslator type
    // if (bricks.len() % replica_count) != 0 {
    // return Err("The brick list and replica count are not multiples. Not creating
    // volume".to_string());
    // }
    //

    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("create".to_string());
    arg_list.push(volume.to_string());

    for (key, value) in options.iter() {
        arg_list.push(key.clone().to_string());
        arg_list.push(value.to_string());
    }

    arg_list.push("transport".to_string());
    arg_list.push(transport.clone().to_string());

    for brick in bricks.iter() {
        arg_list.push(brick.to_string());
    }
    if force {
        arg_list.push("force".to_string());
    }
    return process_output(run_command("gluster", &arg_list, true, true));
}

fn vol_set(volume: &str, option: &GlusterOption) -> Result<i32, GlusterError> {
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("set".to_string());
    arg_list.push(volume.to_string());

    arg_list.push(option.to_string());
    arg_list.push(option.value());

    return process_output(run_command("gluster", &arg_list, true, true));
}

/// Set an option on the volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_set_options(volume: &str, settings: Vec<GlusterOption>) -> Result<i32, GlusterError> {
    let results: Vec<Result<i32, GlusterError>> = settings
        .iter()
        .map(|gluster_opt| vol_set(volume, gluster_opt))
        .collect();

    let mut error_list: Vec<String> = Vec::new();
    for result in results {
        match result {
            Ok(_) => {}
            Err(e) => error_list.push(e.to_string()),
        }
    }
    if error_list.len() > 0 {
        return Err(GlusterError::new(error_list.join("\n")));
    }

    return Ok(0);
}

/// This creates a new replicated volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_create_replicated(
    volume: &str,
    replica_count: usize,
    transport: Transport,
    bricks: Vec<Brick>,
    force: bool,
) -> Result<i32, GlusterError> {
    let mut volume_translators: HashMap<VolumeTranslator, usize> = HashMap::new();
    volume_translators.insert(VolumeTranslator::Replica, replica_count);

    return volume_create(volume, volume_translators, &transport, bricks, force);
}

/// The arbiter volume is special subset of replica volumes that is aimed at preventing
/// split-brains and providing the same consistency guarantees as a normal replica 3 volume
/// without consuming 3x space.
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_create_arbiter(
    volume: &str,
    replica_count: usize,
    arbiter_count: usize,
    transport: Transport,
    bricks: Vec<Brick>,
    force: bool,
) -> Result<i32, GlusterError> {
    let mut volume_translators: HashMap<VolumeTranslator, usize> = HashMap::new();
    volume_translators.insert(VolumeTranslator::Replica, replica_count);
    volume_translators.insert(VolumeTranslator::Arbiter, arbiter_count);

    return volume_create(volume, volume_translators, &transport, bricks, force);
}

/// This creates a new striped volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_create_striped(
    volume: &str,
    stripe: usize,
    transport: Transport,
    bricks: Vec<Brick>,
    force: bool,
) -> Result<i32, GlusterError> {
    let mut volume_translators: HashMap<VolumeTranslator, usize> = HashMap::new();
    volume_translators.insert(VolumeTranslator::Stripe, stripe);

    return volume_create(volume, volume_translators, &transport, bricks, force);
}

/// This creates a new striped and replicated volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_create_striped_replicated(
    volume: &str,
    stripe: usize,
    replica: usize,
    transport: Transport,
    bricks: Vec<Brick>,
    force: bool,
) -> Result<i32, GlusterError> {
    let mut volume_translators: HashMap<VolumeTranslator, usize> = HashMap::new();
    volume_translators.insert(VolumeTranslator::Stripe, stripe);
    volume_translators.insert(VolumeTranslator::Replica, replica);

    return volume_create(volume, volume_translators, &transport, bricks, force);
}

/// This creates a new distributed volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_create_distributed(
    volume: &str,
    transport: Transport,
    bricks: Vec<Brick>,
    force: bool,
) -> Result<i32, GlusterError> {
    let volume_translators: HashMap<VolumeTranslator, String> = HashMap::new();

    return volume_create(volume, volume_translators, &transport, bricks, force);
}

/// This creates a new erasure coded volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_create_erasure(
    volume: &str,
    disperse: usize,
    redundancy: usize,
    transport: Transport,
    bricks: Vec<Brick>,
    force: bool,
) -> Result<i32, GlusterError> {
    let mut volume_translators: HashMap<VolumeTranslator, usize> = HashMap::new();
    volume_translators.insert(VolumeTranslator::Disperse, disperse);
    volume_translators.insert(VolumeTranslator::Redundancy, redundancy);

    return volume_create(volume, volume_translators, &transport, bricks, force);
}
