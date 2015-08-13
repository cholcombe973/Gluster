extern crate byteorder;
extern crate regex;
extern crate unix_socket;
extern crate uuid;
#[macro_use]
extern crate log;
extern crate xdr;
use regex::Regex;
use uuid::Uuid;

use std::ascii::AsciiExt;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io;
use std::io::Cursor;
use std::io::prelude::*;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use unix_socket::UnixStream;
use xdr::xdr::{XdrWriter};

const RPC_VERSION: u32 = 2;

const CALL: i32 = 0;
const REPLY: i32 = 1;
const MSG_ACCEPTED: i32 = 0;
const MSG_DENIED: i32 = 1;

const SUCCESS: i32 = 0;                            // RPC executed successfully
const PROG_UNAVAIL: i32  = 1;                      // remote hasn't exported program
const PROG_MISMATCH: i32 = 2;                      // remote can't support version #
const PROC_UNAVAIL: i32  = 3;                      // program can't support procedure
const GARBAGE_ARGS: i32  = 4;                      // procedure can't decode params

const RPC_MISMATCH: i32 = 0;                       // RPC version number != 2
const AUTH_ERROR: i32 = 1;                         // remote can't authenticate caller

#[cfg(test)]
mod tests{
    extern crate unix_socket;
    use std::io::Cursor;
    use std::path::Path;
    use unix_socket::UnixStream;
    use uuid::Uuid;
    use super::Pack;
    use super::UnPack;

    #[test]
    fn test_parse_peer_status() {
        let test_result = vec![
            super::Peer{
               uuid: Uuid::parse_str("afbd338e-881b-4557-8764-52e259885ca3").unwrap(),
               hostname: "10.0.3.207".to_string(),
               status: super::State::PeerInCluster,
            },
            super::Peer{
               uuid: Uuid::parse_str("fa3b031a-c4ef-43c5-892d-4b909bc5cd5d").unwrap(),
               hostname: "10.0.3.208".to_string(),
               status: super::State::PeerInCluster,
            },
            super::Peer{
               uuid: Uuid::parse_str("5f45e89a-23c1-41dd-b0cd-fd9cf37f1520").unwrap(),
               hostname: "10.0.3.209".to_string(),
               status: super::State::PeerInCluster,
            }];
        let test_line = "Number of Peers: 3 \
                                            \
                        Hostname: 10.0.3.207 \
                        Uuid: afbd338e-881b-4557-8764-52e259885ca3 \
                        State: Peer in Cluster (Connected) \

                        Hostname: 10.0.3.208 \
                        Uuid: fa3b031a-c4ef-43c5-892d-4b909bc5cd5d \
                        State: Peer in Cluster (Connected) \

                        Hostname: 10.0.3.209 \
                        Uuid: 5f45e89a-23c1-41dd-b0cd-fd9cf37f1520 \
                        State: Peer in Cluster (Connected)".to_string();

        //Expect a 3 peer result
        let result = super::parse_peer_status(&test_line);
        println!("Result: {:?}", result);
        assert!(result.is_ok());

        let result_unwrapped = result.unwrap();
        assert_eq!(test_result, result_unwrapped);
    }

    #[test]
    //Needs a running Gluster server to test against. Probably needs to be moved to function testing
    fn list_peers(){
        let mut xid = 1;
        let prog = 1238463;
        let vers = 2;
        let verf = super::GlusterAuth{
            flavor: 0,
            stuff: "".to_string(),
        };
        let verf_bytes = verf.pack();
        let cred_flavor = 390039;
        let creds = super::pack_gluster_v2_cred(cred_flavor);

        let mut call_bytes = super::pack_callheader(
            xid, prog, vers, super::GlusterCliCommand::GlusterCliListFriends, creds, verf_bytes);

        let peer_request = super::GlusterCliPeerListRequest{
                flags: 2,
                dict: "".to_string()
        };

        let peer_bytes = peer_request.pack();
        for byte in peer_bytes{
            call_bytes.push(byte);
        }

        let addr = Path::new("/var/run/glusterd.socket");
        println!("Connecting to /var/run/glusterd.socket");
        let mut sock = UnixStream::connect(&addr).unwrap();

        let result = super::sendrecord(&mut sock, &call_bytes);
        println!("Result: {:?}", result);

        let mut reply_bytes = super::recvrecord(&mut sock).unwrap();
        let mut cursor = Cursor::new(&mut reply_bytes[..]);
        let reply = super::unpack_replyheader(&mut cursor).unwrap();
        println!("Reply header parsed result: {:?}", reply);
        let peer_list = super::GlusterCliPeerListResponse::unpack(&mut cursor).unwrap();
        println!("Peer list: {:?}", peer_list);
    }
}
//#define CLI_DEFAULT_CMD_TIMEOUT              120  // 2 minutes
pub trait Pack{
    fn pack(&self) -> Vec<u8>;
}

pub trait UnPack{
    fn unpack<T: Read>(&mut T) -> Result<Self, GlusterError>;
}

fn unpack_string<T: Read>(data: &mut T, size: u32)->Result<String,GlusterError>{
    let mut buffer: Vec<u8> = Vec::with_capacity(size as usize);
    for _ in 0..size {
        let b = try!(data.read_u8());
        buffer.push(b);
    }
    let s = try!(String::from_utf8(buffer));
    return Ok(s);
}

fn pack_string(s: &String)->Vec<u8>{
    let mut buffer: Vec<u8> = Vec::new();

    let bytes = s.clone().into_bytes();
    let bytes_len = bytes.len();
    let pad = bytes_len % 4;

    buffer.write_u32::<BigEndian>(bytes_len as u32).unwrap();

    for byte in bytes{
        buffer.write_u8(byte).unwrap();
    }

    //Padding
    for _ in 0..pad{
        buffer.write_u8(0).unwrap();
    }

    return buffer;
}

#[derive(Debug,Clone)]
enum AuthFlavor{
    AuthNull = 0,
    AuthUnix = 1,
    AuthShort = 2,
    AuthDes = 3,
}

impl AuthFlavor{
    pub fn new(flavor: i32)->AuthFlavor{
        match flavor{
            0 => AuthFlavor::AuthNull,
            1 => AuthFlavor::AuthUnix,
            2 => AuthFlavor::AuthShort,
            3 => AuthFlavor::AuthDes,
            _ => AuthFlavor::AuthNull,
        }
    }
}

#[derive(Debug)]
struct GlusterCliRequest{
    dict: String
}

impl Pack for GlusterCliRequest{
    fn pack(&self)->Vec<u8>{
        let mut wr = XdrWriter::new();
        wr.pack(self.dict.clone());
        return wr.into_buffer();
    }
}

impl UnPack for GlusterCliRequest{
    //Expects a cursor so calls can be chained
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliRequest, GlusterError>{
        let size = try!(data.read_u32::<BigEndian>());
        let dict = try!(unpack_string(data, size));

        return Ok(GlusterCliRequest{
            dict: dict
        })
    }
}

#[derive(Debug)]
struct GlusterCliResponse{
    op_ret: i32,
    op_errno: i32,
    op_errstr: String,
    dict: String
}

impl Pack for GlusterCliResponse{
    fn pack(&self)->Vec<u8>{
        let mut wr = XdrWriter::new();
        wr.pack(self.op_ret);
        wr.pack(self.op_errno);
        wr.pack(self.op_errstr.clone());
        wr.pack(self.dict.clone());
        return wr.into_buffer();
    }
}

impl UnPack for GlusterCliResponse{
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliResponse, GlusterError>{
        let op_ret = try!(data.read_i32::<BigEndian>());
        let op_errno = try!(data.read_i32::<BigEndian>());

        let size = try!(data.read_u32::<BigEndian>());
        let op_errstr = try!(unpack_string(data, size));

        let size = try!(data.read_u32::<BigEndian>());
        let dict = try!(unpack_string(data, size));

        return Ok(GlusterCliResponse{
            op_ret: op_ret,
            op_errno: op_errno,
            op_errstr: op_errstr,
            dict: dict
        })
    }
}

#[derive(Debug)]
struct GlusterCliPeerListRequest{
    flags: i32,
    dict: String,
}



impl Pack for GlusterCliPeerListRequest{
    fn pack(&self)->Vec<u8>{
        //XDRlib has a bug where it doesn't pad Strings correctly that are size 0
        let mut buffer: Vec<u8> = Vec::new();
        buffer.write_i32::<BigEndian>(self.flags).unwrap();
        let string_buffer = pack_string(&self.dict);
        for byte in string_buffer{
            buffer.push(byte);
        }
        return buffer;
    }
}

impl UnPack for GlusterCliPeerListRequest{
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliPeerListRequest, GlusterError>{
        let flags = try!(data.read_i32::<BigEndian>());

        let size = try!(data.read_u32::<BigEndian>());
        let dict = try!(unpack_string(data, size));

        return Ok(GlusterCliPeerListRequest{
            flags: flags,
            dict: dict,
        })
    }
}

#[derive(Debug)]
struct GlusterCliPeerListResponse{
    op_ret: i32,
    op_errno: i32,
    friends: String,
}

impl Pack for GlusterCliPeerListResponse{
    fn pack(&self)->Vec<u8>{
        let mut wr = XdrWriter::new();

        wr.pack(self.op_ret);
        wr.pack(self.op_errno);
        wr.pack(self.friends.clone());

        return wr.into_buffer();
    }
}

impl UnPack for GlusterCliPeerListResponse{
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliPeerListResponse, GlusterError>{
        let op_ret = try!(data.read_i32::<BigEndian>());
        let op_errno = try!(data.read_i32::<BigEndian>());

        let size = try!(data.read_u32::<BigEndian>());
        println!("Peer list size: {}", size);
        let friends = try!(unpack_string(data, size));

        return Ok(GlusterCliPeerListResponse{
            op_ret: op_ret,
            op_errno: op_errno,
            friends: friends,
        })
    }
}

#[derive(Debug)]
struct GlusterCliFsmLogRequest{
    name: String,
}

impl Pack for GlusterCliFsmLogRequest{
    fn pack(&self)->Vec<u8>{
        let mut wr = XdrWriter::new();
        wr.pack(self.name.clone());
        return wr.into_buffer();
    }
}

impl UnPack for GlusterCliFsmLogRequest{
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliFsmLogRequest, GlusterError>{
        let size = try!(data.read_u32::<BigEndian>());
        let name = try!(unpack_string(data, size));

        return Ok(GlusterCliFsmLogRequest{
            name: name
        })
    }
}

#[derive(Debug)]
struct GlusterCliFsmLogReponse{
    op_ret: i32,
    op_errno: i32,
    op_errstr: String,
    fsm_log: String,
}

impl Pack for GlusterCliFsmLogReponse{
    fn pack(&self)->Vec<u8>{
        let mut wr = XdrWriter::new();
        wr.pack(self.op_ret);
        wr.pack(self.op_errno);
        wr.pack(self.op_errstr.clone());
        wr.pack(self.fsm_log.clone());
        return wr.into_buffer();
    }
}
impl UnPack for GlusterCliFsmLogReponse{
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliFsmLogReponse, GlusterError>{
        let op_ret = try!(data.read_i32::<BigEndian>());
        let op_errno = try!(data.read_i32::<BigEndian>());

        let size = try!(data.read_u32::<BigEndian>());
        let op_errstr = try!(unpack_string(data, size));

        let size = try!(data.read_u32::<BigEndian>());
        let fsm_log = try!(unpack_string(data, size));

        return Ok(GlusterCliFsmLogReponse{
            op_ret: op_ret,
            op_errno: op_errno,
            op_errstr: op_errstr,
            fsm_log: fsm_log,
        })
    }
}


#[derive(Debug)]
struct GlusterCliGetwdRequest{
    unused:i32,
}

impl Pack for GlusterCliGetwdRequest{
    fn pack(&self)->Vec<u8>{
        let mut wr = XdrWriter::new();
        wr.pack(self.unused);
        return wr.into_buffer();
    }
}

impl UnPack for GlusterCliGetwdRequest{
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliGetwdRequest, GlusterError>{
        let unused = try!(data.read_i32::<BigEndian>());
        return Ok(GlusterCliGetwdRequest{
            unused: unused
        })
    }
}

#[derive(Debug)]
struct GlusterCliGetwdResponse{
    op_ret: i32,
    op_errno: i32,
    wd: String
}

impl Pack for GlusterCliGetwdResponse{
    fn pack(&self)->Vec<u8>{
        let mut wr = XdrWriter::new();
        wr.pack(self.op_ret);
        wr.pack(self.op_errno);
        wr.pack(self.wd.clone());
        return wr.into_buffer();
    }
}

impl UnPack for GlusterCliGetwdResponse{
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliGetwdResponse, GlusterError>{
        let op_ret = try!(data.read_i32::<BigEndian>());
        let op_errno = try!(data.read_i32::<BigEndian>());

        let size = try!(data.read_u32::<BigEndian>());
        let wd = try!(unpack_string(data, size));

        return Ok(GlusterCliGetwdResponse{
            op_ret: op_ret,
            op_errno: op_errno,
            wd: wd,
        })
    }
}

#[derive(Debug)]
struct GlusterCliMountRequest{
    label: String,
    dict: String,
}

impl Pack for GlusterCliMountRequest{
    fn pack(&self)->Vec<u8>{
        let mut wr = XdrWriter::new();
        wr.pack(self.label.clone());
        wr.pack(self.dict.clone());
        return wr.into_buffer();
    }
}

impl UnPack for GlusterCliMountRequest{
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliMountRequest, GlusterError>{
        let size = try!(data.read_u32::<BigEndian>());
        let label = try!(unpack_string(data, size));

        let size = try!(data.read_u32::<BigEndian>());
        let dict = try!(unpack_string(data, size));

        return Ok(GlusterCliMountRequest{
            label: label,
            dict: dict,
        })
    }
}

#[derive(Debug)]
struct GlusterCliMountResponse{
    op_ret: i32,
    op_errno: i32,
    path: String,
}

impl Pack for GlusterCliMountResponse{
    fn pack(&self)->Vec<u8>{
        let mut wr = XdrWriter::new();
        wr.pack(self.op_ret);
        wr.pack(self.op_errno);
        wr.pack(self.path.clone());
        return wr.into_buffer();
    }
}

impl UnPack for GlusterCliMountResponse{
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliMountResponse, GlusterError>{
        let op_ret = try!(data.read_i32::<BigEndian>());
        let op_errno = try!(data.read_i32::<BigEndian>());

        let size = try!(data.read_u32::<BigEndian>());
        let path = try!(unpack_string(data, size));

        return Ok(GlusterCliMountResponse{
            op_ret: op_ret,
            op_errno: op_errno,
            path: path,
        })
    }
}

#[derive(Debug)]
struct GlusterCliUmountRequest{
    lazy: i32,
    path: String,
}

impl Pack for GlusterCliUmountRequest{
    fn pack(&self)->Vec<u8>{
        let mut wr = XdrWriter::new();
        wr.pack(self.lazy);
        wr.pack(self.path.clone());
        return wr.into_buffer();
    }
}

impl UnPack for GlusterCliUmountRequest{
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliUmountRequest, GlusterError>{
        let lazy = try!(data.read_i32::<BigEndian>());

        let size = try!(data.read_u32::<BigEndian>());
        let path = try!(unpack_string(data, size));

        return Ok(GlusterCliUmountRequest{
            lazy: lazy,
            path: path
        })
    }
}

#[derive(Debug)]
struct GlusterCliUmountResponse{
    op_ret:i32,
    op_errno: i32,
}

impl Pack for GlusterCliUmountResponse{
    fn pack(&self)->Vec<u8>{
        let mut wr = XdrWriter::new();
        wr.pack(self.op_ret);
        wr.pack(self.op_errno);
        return wr.into_buffer();
    }
}

impl UnPack for GlusterCliUmountResponse{
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliUmountResponse, GlusterError>{
        let op_ret = try!(data.read_i32::<BigEndian>());
        let op_errno = try!(data.read_i32::<BigEndian>());

        return Ok(GlusterCliUmountResponse{
            op_ret: op_ret,
            op_errno: op_errno,
        })
    }
}

#[derive(Debug)]
struct GlusterAuth{
    flavor: i32,
    stuff: String
}

#[derive(Debug)]
struct GlusterCred{
    flavor: i32,
}

impl Pack for GlusterAuth{
    fn pack(&self)->Vec<u8>{
        let mut wr = XdrWriter::new();
        wr.pack(self.flavor);
        wr.pack(self.stuff.clone());
        return wr.into_buffer();
    }
}

fn pack_gluster_v2_cred(flavor: i32)->Vec<u8>{
    let mut wr = XdrWriter::new();
    //let flavor: i32 = 390039
    wr.pack(flavor);
    wr.pack(24); // Length = 24
    wr.pack(0);  // Padding?
    wr.pack(0);
    wr.pack(0);
    wr.pack(0);
    wr.pack(4);
    return wr.into_buffer();
}

fn pack_callheader(xid: u32, prog: i32, vers: u32, proc_num: GlusterCliCommand, cred_flavor: Vec<u8>, verf: Vec<u8>)->Vec<u8>{
    let mut wr = XdrWriter::new();
    let call: i32 = 0;
    let rpc_version: u32 = 2;

    wr.pack(xid);
    wr.pack(call); //CALL=0
    wr.pack(rpc_version);
    wr.pack(prog); // 1238463
    wr.pack(vers); // 2
    wr.pack(proc_num as u32); // 3
    let mut buffer = wr.into_buffer();

    for byte in cred_flavor{
        buffer.push(byte);
    }
    for byte in verf{
        buffer.push(byte);
    }
    // Caller must add procedure-specific part of call
    return buffer;
}

//Takes a generic which will most likely be a Cursor
//That way the next call can also use the last cursor position
fn unpack_replyheader<T: Read>(data: &mut T)->Result<(u32, GlusterAuth), String>{
    let xid = data.read_u32::<BigEndian>().unwrap();
    println!("reply xid {}", xid);
    let msg_type = data.read_i32::<BigEndian>().unwrap();
    println!("reply msg_type {}", xid);

    if msg_type != REPLY{
        //Invalid REPLY
        return Err(format!("Invalid reply with msg_type: {}", msg_type));
    }

    let stat = data.read_i32::<BigEndian>().unwrap();
    println!("reply stat {}", xid);
    if stat == MSG_DENIED {
        let reason = data.read_i32::<BigEndian>().unwrap();
        if reason == RPC_MISMATCH{
            let low = data.read_u32::<BigEndian>().unwrap();
            let high = data.read_u32::<BigEndian>().unwrap();
            return Err(format!("MSG_DENIED: RPC_MISMATCH low: {} high: {}", low, high));
        }
        if reason == AUTH_ERROR {
            let err = data.read_u32::<BigEndian>().unwrap();
            return Err(format!("MSG_DENIED: AUTH_ERROR {}", err));
        }
        return Err(format!("MSG_DENIED: {}", reason));
    }
    if stat == MSG_ACCEPTED{
        let auth_flavor = data.read_i32::<BigEndian>().unwrap();

        let size = data.read_u32::<BigEndian>().unwrap();
        let stuff = unpack_string(data, size).unwrap();

        let accept_message = data.read_i32::<BigEndian>().unwrap();
        //Parse auth_flavor into the enum
        let rpc_auth = GlusterAuth{
            flavor: auth_flavor,
            stuff: stuff,
        };
        match accept_message{
            PROG_UNAVAIL => {
                return Err("call failed PROG_UNAVAIL".to_string());
            },
            PROG_MISMATCH => {
                let low = data.read_u32::<BigEndian>().unwrap();
                let high = data.read_u32::<BigEndian>().unwrap();
                return Err(format!("Call failed: PROG_MISMATCH low: {} high: {}", low, high));
            }
            PROC_UNAVAIL => {
                return Err("call failed PROC_UNAVAIL".to_string());
            },
            GARBAGE_ARGS => {
                return Err("call failed GARBAGE_ARGS".to_string());
            },
            SUCCESS => {
                return Ok((xid, rpc_auth));
            }
            _ => {
                return Err(format!("Call failed: {}", accept_message));
            }
        }
    }else{
        return Err(format!("MSG neither denied or accepted: {}", stat));
    }
}


/*
fn create_quota_dict(gfid, volume_uuid, default_soft_limit, quota_command){
    data = "gfid\x00" + gfid + "\x00volume-uuid\x00" + volume_uuid + \
           "\x00default-soft-limit\x00" + default_soft_limit + "\x00" + \
           default_soft_limit + "%\x00type\x00" + quota_command
    return data
}
*/

#[derive(Debug)]
pub enum GlusterCliCommand {
    GlusterCliNull = 0,
    GlusterCliProbe = 1,
    GlusterCliDeprobe = 2,
    GlusterCliListFriends = 3,
    GlusterCliCreateVolume = 4,
    GlusterCliGetVolume = 5,
    GlusterCliGetNextVolume = 6,
    GlusterCliDeleteVolume = 7,
    GlusterCliStartVolume = 8,
    GlusterCliStopVolume = 9,
    GlusterCliRenameVolume = 10,
    GlusterCliDefragVolume = 11,
    GlusterCliSetVolume = 12,
    GlusterCliAddBrick = 13,
    GlusterCliRemoveBrick = 14,
    GlusterCliReplaceBrick = 15,
    GlusterCliLogRotate = 16,
    GlusterCliGetspec = 17,
    GlusterCliPmapPortbybrick = 18,
    GlusterCliSyncVolume = 19,
    GlusterCliResetVolume = 20,
    GlusterCliFsmLog = 21,
    GlusterCliGsyncSet = 22,
    GlusterCliProfileVolume = 23,
    GlusterCliQuota = 24,
    GlusterCliTopVolume = 25,
    GlusterCliGetwd = 26,
    GlusterCliStatusVolume = 27,
    GlusterCliStatusAll = 28,
    GlusterCliMount = 29,
    GlusterCliUmount = 30,
    GlusterCliHealVolume = 31,
    GlusterCliStatedumpVolume = 32,
    GlusterCliListVolume = 33,
    GlusterCliClrlocksVolume = 34,
    GlusterCliUuidReset = 35,
    GlusterCliUuidGet = 36,
    GlusterCliCopyFile = 37,
    GlusterCliSysExec = 38,
    GlusterCliSnap = 39,
    GlusterCliBarrierVolume = 40,
    GlusterCliGetVolOpt = 41,
    GlusterCliGanesha = 42,
    GlusterCliBitrot = 43,
    GlusterCliAttachTier = 44,
    GlusterCliDetachTier = 45,
    GlusterCliMaxvalue = 46,
}

//Custom error handling for the library
#[derive(Debug)]
pub enum GlusterError{
    IoError(io::Error),
    FromUtf8Error(std::string::FromUtf8Error),
    ParseError(uuid::ParseError),
    AddrParseError(String),
    ByteOrder(byteorder::Error),
    NoVolumesPresent,
}

impl GlusterError{
    fn new(err: String) -> GlusterError {
        GlusterError::IoError(
            io::Error::new(std::io::ErrorKind::Other, err)
        )
    }

    pub fn to_string(&self) -> String{
        match *self {
            GlusterError::IoError(ref err) => err.description().to_string(),
            GlusterError::FromUtf8Error(ref err) => err.description().to_string(),
            //TODO fix this
            GlusterError::ParseError(_) => "Parse error".to_string(),
            GlusterError::AddrParseError(_) => "IP Address parsing error".to_string(),
            GlusterError::ByteOrder(ref err) => err.description().to_string(),
            GlusterError::NoVolumesPresent => "No volumes present".to_string(),
        }
    }
}

impl From<io::Error> for GlusterError {
    fn from(err: io::Error) -> GlusterError {
        GlusterError::IoError(err)
    }
}

impl From<std::string::FromUtf8Error> for GlusterError {
    fn from(err: std::string::FromUtf8Error) -> GlusterError {
        GlusterError::FromUtf8Error(err)
    }
}

impl From<uuid::ParseError> for GlusterError {
    fn from(err: uuid::ParseError) -> GlusterError {
        GlusterError::ParseError(err)
    }
}

impl From<std::net::AddrParseError> for GlusterError {
    fn from(_: std::net::AddrParseError) -> GlusterError {
        GlusterError::AddrParseError("IP Address parsing error".to_string())
    }
}

impl From<byteorder::Error> for GlusterError {
    fn from(err: byteorder::Error) -> GlusterError {
        GlusterError::ByteOrder(err)
    }
}

pub struct Brick {
    pub peer: Peer,
    pub path: PathBuf,
}

impl Brick{
    pub fn to_string(&self) -> String{
        format!("{}:{}", self.peer.hostname.clone(), self.path.to_str().unwrap())
    }
}

impl fmt::Debug for Brick {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}:{:?}", self.peer.hostname, self.path.to_str())
    }
}

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub enum State {
    Connected,
    Disconnected,
    Unknown,

    EstablishingConnection,
    ProbeSentToPeer,
    ProbeReceivedFromPeer,
    PeerInCluster,
    AcceptedPeerRequest,
    SentAndReceivedPeerRequest,
    PeerRejected,
    PeerDetachInProgress,
    ConnectedToPeer,
    PeerIsConnectedAndAccepted,
    InvalidState,
}

impl State {
    pub fn new(name: &str)->State{
        match name.trim().to_ascii_lowercase().as_ref() {
            "connected" => State::Connected,
            "disconnected" => State::Disconnected,
            "establishing connection" => State::EstablishingConnection,
            "probe sent to peer" => State::ProbeSentToPeer,
            "probe received from peer" => State::ProbeReceivedFromPeer,
            "peer in cluster" => State::PeerInCluster,
            "accepted peer request" => State::AcceptedPeerRequest,
            "sent and received peer request" => State::SentAndReceivedPeerRequest,
            "peer rejected" => State::PeerRejected,
            "peer detach in progress" => State::PeerDetachInProgress,
            "connected to peer" => State::ConnectedToPeer,
            "peer is connected and accepted" => State::PeerIsConnectedAndAccepted,
            "invalid state" => State::InvalidState,
            _ => State::Unknown,
        }
    }
    pub fn to_string(self) -> String {
        match self {
            State::Connected => "Connected".to_string(),
            State::Disconnected => "Disconnected".to_string(),
            State::Unknown => "Unknown".to_string(),
            State::EstablishingConnection => "establishing connection".to_string(),
            State::ProbeSentToPeer =>"probe sent to peer".to_string(),
            State::ProbeReceivedFromPeer =>"probe received from peer".to_string(),
            State::PeerInCluster =>"peer in cluster".to_string(),
            State::AcceptedPeerRequest =>"accepted peer request".to_string(),
            State::SentAndReceivedPeerRequest =>"sent and received peer request".to_string(),
            State::PeerRejected =>"peer rejected".to_string(),
            State::PeerDetachInProgress =>"peer detach in progress".to_string(),
            State::ConnectedToPeer =>"connected to peer".to_string(),
            State::PeerIsConnectedAndAccepted =>"peer is connected and accepted".to_string(),
            State::InvalidState => "invalid state".to_string(),
        }
    }
}
#[derive(Debug)]
pub struct Quota{
    pub path: PathBuf,
    pub limit: u64,
    pub used: u64,
}

#[derive(Clone, Eq, PartialEq)]
pub struct Peer {
   pub uuid: Uuid,
   //TODO: Lets stay with ip addresses.
   pub hostname: String,
   pub status: State,
}

impl fmt::Debug for Peer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UUID: {} Hostname: {} Status: {}",
            self.uuid.to_hyphenated_string(),
            self.hostname,
            self.status.to_string())
    }
}

#[derive(Debug, Clone)]
pub enum Transport {
    Tcp,
    Rdma,
    TcpAndRdma,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum VolumeTranslator{
    Stripe,
    Replica,
    Disperse,
    Redundancy,
}

impl VolumeTranslator{
    fn to_string(self) -> String {
        match self {
            VolumeTranslator::Stripe => "stripe".to_string(),
            VolumeTranslator::Replica => "replica".to_string(),
            VolumeTranslator::Disperse => "disperse".to_string(),
            VolumeTranslator::Redundancy => "redundancy".to_string(),
        }
    }
}

#[derive(Debug)]
pub enum VolumeType {
    Distribute,
    Stripe,
    Replicate,
    StripedAndReplicate,
    Disperse,
    //Tier,
    DistributedAndStripe,
    DistributedAndReplicate,
    DistributedAndStripedAndReplicate,
    DistributedAndDisperse,
}

impl VolumeType {
    pub fn new(name: &str)->VolumeType{
        match name.trim().to_ascii_lowercase().as_ref() {
            "distribute" => VolumeType::Distribute,
            "stripe" => VolumeType::Stripe,
            "replicate" => VolumeType::Replicate,
            "striped-replicate" => VolumeType::StripedAndReplicate,
            "disperse" => VolumeType::Disperse,
            //"Tier" => VolumeType::Tier, //TODO: Waiting for this to become stable
            "distributed-stripe" => VolumeType::DistributedAndStripe,
            "distributed-replicate" => VolumeType::DistributedAndReplicate,
            "distributed-striped-replicate" => VolumeType::DistributedAndStripedAndReplicate,
            "distributed-disperse" => VolumeType::DistributedAndDisperse,
            _ => VolumeType::Replicate,
        }
    }
    pub fn to_string(self) -> String {
        match self {
            VolumeType::Distribute => "Distribute".to_string(),
            VolumeType::Stripe => "Stripe".to_string(),
            VolumeType::Replicate => "Replicate".to_string(),
            VolumeType::StripedAndReplicate => "Striped-Replicate".to_string(),
            VolumeType::Disperse => "Disperse".to_string(),
            //VolumeType::Tier => "Tier".to_string(), //TODO: Waiting for this to become stable
            VolumeType::DistributedAndStripe => "Distributed-Stripe".to_string(),
            VolumeType::DistributedAndReplicate => "Distributed-Replicate".to_string(),
            VolumeType::DistributedAndStripedAndReplicate => "Distributed-Striped-Replicate".to_string(),
            VolumeType::DistributedAndDisperse => "Distributed-Disperse".to_string(),
        }
    }
}

#[derive(Debug)]
pub struct Volume {
    pub name: String,
    pub vol_type: VolumeType,
    pub id: Uuid,
    pub status: String,
    pub transport: Transport,
    pub bricks: Vec<Brick>,
}

impl Transport {
    fn new(name: &str)->Transport{
        match name.trim().to_ascii_lowercase().as_ref() {
            "tcp" => Transport::Tcp,
            "tcp,rdma" => Transport::TcpAndRdma,
            "rdma" => Transport::Rdma,
            _ => Transport::Tcp,
        }
    }
    fn to_string(self) -> String {
        match self {
            Transport::Rdma => "rdma".to_string(),
            Transport::Tcp => "tcp".to_string(),
            Transport::TcpAndRdma => "tcp,rdma".to_string(),
        }
    }
}

fn process_output(output: std::process::Output)->Result<i32, GlusterError>{
    let status = output.status;

    if status.success(){
        return Ok(0);
    }else{
        return Err(
            GlusterError::new(
                try!(String::from_utf8(output.stderr))
            )
        );
    }
}

//TODO: Change me to Result<std::process::Output, String>
fn run_command(command: &str, arg_list: &Vec<String>, as_root: bool, script_mode: bool) -> std::process::Output{
    if as_root{
        let mut cmd = std::process::Command::new("sudo");
        cmd.arg(command);
        if script_mode{
            cmd.arg("--mode=script");
        }
        for arg in arg_list{
            cmd.arg(&arg);
        }
        debug!("About to run command: {:?}", cmd);
        let output = cmd.output().unwrap_or_else(|e| { panic!("failed to execute process: {} ", e)});
        return output;
    }else{
       let mut cmd = std::process::Command::new(command);
        if script_mode{
            cmd.arg("--mode=script");
        }
        for arg in arg_list{
            cmd.arg(&arg);
        }
        debug!("About to run command: {:?}", cmd);
        let output = cmd.output().unwrap_or_else(|e| { panic!("failed to execute process: {} ", e)});
        return output;
    }
}

#[cfg(target_endian="little")]
fn htonl(num: u32)->u32{
    return 0;
}

#[cfg(target_endian="big")]
fn htonl(num: u32)->u32{
    return 0;
}

fn send_fragment<T: Write>(socket: &mut T, last: bool, fragment: &Vec<u8>)->Result<usize,String>{
    let mut header_buffer: Vec<u8> = Vec::new();
    let length: u32 = fragment.len() as u32;

    let mut header = length & 0x7fffffff;

    if last {
        header = length | 0x80000000;
        println!("length: {}", length);
    }
    //This assumes we're on a little endian machine.  Needs to be endian generic
    //let header = (x>>24 & 0xff) + (x>>16 & 0xff) +
    //          (x>>8 & 0xff) + (x & 0xff);
    //Might be a better way to do this like writing to the socket directly
    //fragment.insert(0, header as u8);
    header_buffer.write_u32::<BigEndian>(header as u32).unwrap();

    println!("Sending header");
    print_fragment(&header_buffer);

    let mut bytes_written = try!(socket.write(&header_buffer).map_err(|e| e.to_string()));

    println!("Fragment length: {}", fragment.len());
    println!("Sending fragment");

    print_fragment(&fragment);

    bytes_written += try!(socket.write(fragment).map_err(|e| e.to_string()));
    socket.flush().unwrap();
    return Ok(bytes_written);
}

fn sendrecord(sock: &mut UnixStream, record: &Vec<u8>)->Result<usize,String>{
    let send_size = try!(send_fragment(sock, true, &record));
    return Ok(send_size);
}

fn print_fragment(frag: &Vec<u8>){
    for chunk in frag.chunks(4){
        for c in chunk{
            print!("{:02x}:", c);
        }
        print!(" ");
    }
    println!("");
}

/*
    Uses a generic trait so that this function can be unit tested
    by replaying captured data.
 */
fn recv_fragment<T: Read>(socket: &mut T)-> Result<(bool, Vec<u8>), String>{
    //Read at most 4 bytes
    let mut buffer: Vec<u8> = Vec::new();

    try!(socket.by_ref().take(4).read_to_end(&mut buffer).map_err(|e| e.to_string()));

    if buffer.len() < 4{
        return Err("EOF Error".to_string());
    }
    let mut buf = Cursor::new(&buffer[..]);
    let header = buf.read_u32::<BigEndian>().unwrap();

    let last = (header & 0x80000000) != 0;
    println!("Last Fragment: {}", last);

    let mut n = header & 0x7fffffff;
    println!("Fragment length: {}", n);
    let mut fragment: Vec<u8> = Vec::new();
    while n > 0{
        //Might need to introduce a local buffer here.  I'm not sure yet
        let mut handle = socket.by_ref().take(n as u64);
        try!(handle.read_to_end(&mut fragment).map_err(|e| e.to_string()));
        n = n - n;
    }
    print_fragment(&fragment);
    return Ok((last, fragment));
}

fn recvrecord(sock: &mut UnixStream)->Result<Vec<u8>, String>{
    let mut record:Vec<u8> = Vec::new();
    let mut last = false;
    while !last{
        let (last_frag, frag) = try!(recv_fragment(sock));
        last = last_frag;
        for byte in frag{
            record.push(byte);
        }
    }
    return Ok(record);
}

//TODO: figure out a better way to do this.  This seems hacky
pub fn get_local_ip()->Result<Ipv4Addr, GlusterError>{
    let mut default_route: Vec<String>  = Vec::new();
    default_route.push("route".to_string());
    default_route.push("show".to_string());
    default_route.push("0.0.0.0/0".to_string());

    let cmd_output = run_command("ip", &default_route, false, false);
    let default_route_stdout: String = try!(String::from_utf8(cmd_output.stdout));

    //default via 192.168.1.1 dev wlan0  proto static
    let addr_regex = Regex::new(r"(?P<addr>via \S+)").unwrap();
    let x = addr_regex.captures(&default_route_stdout).unwrap().name("addr");

    //Skip "via" in the capture
    let addr: Vec<&str> = x.unwrap().split(" ").skip(1).collect();

    let mut arg_list: Vec<String>  = Vec::new();
    arg_list.push("route".to_string());
    arg_list.push("get".to_string());
    arg_list.push(addr[0].to_string());

    let src_address_output = run_command("ip", &arg_list, false, false);
    //192.168.1.1 dev wlan0  src 192.168.1.7
    let local_address_stdout = try!(String::from_utf8(src_address_output.stdout));
    let src_regex = Regex::new(r"(?P<src>src \S+)").unwrap();
    let capture_output = src_regex.captures(&local_address_stdout).unwrap().name("src");

    //Skip src in the capture
    let local_ip: Vec<&str> = capture_output.unwrap().split(" ").skip(1).collect();
    let ip_addr = try!(local_ip[0].trim().parse::<Ipv4Addr>());

    return Ok(ip_addr);
}

pub fn resolve_to_ip(address: &str)->Result<String, String>{
    if address == "localhost"{
        let local_ip = try!(get_local_ip().map_err(|e| e.to_string()));
        debug!("hostname is localhost.  Resolving to local ip {}", &local_ip.to_string());
        return Ok(local_ip.to_string());
    }

    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("+short".to_string());
    //arg_list.push("-x".to_string());
    arg_list.push(address.trim().to_string());
    let output = run_command("dig", &arg_list, false, false);

    let status = output.status;

    if status.success(){
        let output_str = try!(String::from_utf8(output.stdout).map_err(|e| e.to_string()));
        //Remove the trailing . and newline
        let trimmed = output_str.trim().trim_right_matches(".");
        return Ok(trimmed.to_string());
    }else{
        return Err(try!(String::from_utf8(output.stderr).map_err(|e| e.to_string())));
    }
}

pub fn get_local_hostname()->Result<String, GlusterError>{
    let mut f = try!(File::open("/etc/hostname"));
    let mut s = String::new();
    try!(f.read_to_string(&mut s));
    return Ok(s.trim().to_string());
}

pub fn get_peer(hostname: &String) ->Result<Peer, GlusterError>{
    let peer_list = try!(peer_list());

    for peer in peer_list{
        if peer.hostname == *hostname {
            debug!("Found peer: {:?}", peer);
            return Ok(peer.clone());
        }
    }
    return Err(GlusterError::new(format!("Unable to find peer by hostname: {}", hostname)));
}

fn parse_peer_status(line: &String)-> Result<Vec<Peer>, GlusterError>{
    let mut peers: Vec<Peer> = Vec::new();

    //TODO: It's either this or some kinda crazy looping or batching
    let peer_regex = Regex::new(r"Hostname:\s+(?P<hostname>[a-zA-Z0-9.]+)\s+Uuid:\s+(?P<uuid>\w+-\w+-\w+-\w+-\w+)\s+State:\s+(?P<state_detail>[a-zA-z ]+)\s+\((?P<state>\w+)\)").unwrap();
    for cap in peer_regex.captures_iter(line){
        let hostname = try!(cap.name("hostname").ok_or(
            GlusterError::new(format!("Invalid hostname for peer: {}", line)))
        );

        let uuid = try!(cap.name("uuid").ok_or(
            GlusterError::new(format!("Invalid uuid for peer: {}", line)))
        );
        let uuid_parsed = try!(Uuid::parse_str(uuid));
        let state_details = try!(cap.name("state_detail").ok_or(
            GlusterError::new(format!("Invalid state for peer: {}", line)))
        );

        //Translate back into an IP address if needed
        let check_for_ip = hostname.parse::<Ipv4Addr>();

        if check_for_ip.is_err(){
            //It's a hostname so lets resolve it
            match resolve_to_ip(&hostname){
                Ok(ip_addr) => {
                    peers.push(Peer{
                        uuid: uuid_parsed,
                        hostname: ip_addr,
                        status: State::new(state_details),
                    });
                    continue;
                }
                Err(e) => {
                    return Err(GlusterError::new(e.to_string()));
                }
            };
        }else{
            //It's an IP address so lets use it
            peers.push(Peer{
                uuid: uuid_parsed,
                hostname: hostname.to_string(),
                status: State::new(state_details),
            });
        }
    }
    return Ok(peers);
}

pub fn peer_status() ->Result<Vec<Peer>, GlusterError>{
    let mut arg_list: Vec<String>  = Vec::new();
    arg_list.push("peer".to_string());
    arg_list.push("status".to_string());

    let output = run_command("gluster", &arg_list, true, false);
    let output_str = try!(String::from_utf8(output.stdout));
    /*
    Number of Peers: 1
    Hostname: 10.0.3.207
    Uuid: afbd338e-881b-4557-8764-52e259885ca3
    State: Peer in Cluster (Connected)
    */

    return parse_peer_status(&output_str);
}

//List all peers including localhost
pub fn peer_list() ->Result<Vec<Peer>, GlusterError>{
    let mut peers: Vec<Peer> = Vec::new();
    let mut arg_list: Vec<String>  = Vec::new();
    arg_list.push("pool".to_string());
    arg_list.push("list".to_string());

    let output = run_command("gluster", &arg_list, true, false);
    let output_str = try!(String::from_utf8(output.stdout));

    for line in output_str.lines(){
        if line.contains("State"){
            continue;
        }else{
            let v: Vec<&str> = line.split('\t').collect();
            let uuid = try!(Uuid::parse_str(v[0]));
            let mut hostname = v[1].trim().to_string();

            //Translate back into an IP address if needed
            let check_for_ip = hostname.parse::<Ipv4Addr>();

            if check_for_ip.is_err(){
                //It's a hostname so lets resolve it
                hostname = match resolve_to_ip(&hostname){
                    Ok(ip_addr) => ip_addr,
                    Err(e) => {
                        return Err(GlusterError::new(e.to_string()));
                    }
                };
            }
            debug!("hostname from peer list command is {:?}", &hostname);

            peers.push(Peer{
                uuid: uuid,
                hostname: hostname,
                status: State::new(v[2]),
            });
    }
}
return Ok(peers);
}

//Probe a peer and prevent double probing
pub fn peer_probe(hostname: &String)->Result<i32, GlusterError>{
    let current_peers = try!(peer_list());
    for peer in current_peers{
        if peer.hostname == *hostname{
            //Bail instead of double probing
            //return Err(format!("hostname: {} is already part of the cluster", hostname));
            return Ok(0); //Does it make sense to say this is ok?
        }
    }
    let mut arg_list: Vec<String>  = Vec::new();
    arg_list.push("peer".to_string());
    arg_list.push("probe".to_string());
    arg_list.push(hostname.to_string());

    return process_output(run_command("gluster", &arg_list, true, false));
}

pub fn peer_remove(hostname: &String, force: bool)->Result<i32, GlusterError>{
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("peer".to_string());
    arg_list.push("detach".to_string());
    arg_list.push(hostname.to_string());

    if force{
        arg_list.push("force".to_string());
    }

    return process_output(run_command("gluster", &arg_list, true, false));
}

fn split_and_return_field(field_number: usize, string: String) -> String{
    let x: Vec<&str> = string.split(" ").collect();
    if x.len() == (field_number + 1){
        return x[field_number].to_string();
    }else{
        //Failed
        return "".to_string();
    }
}

//Note this will panic on failure to parse u64
pub fn translate_to_bytes(value: &str) -> Option<u64> {
    if value.ends_with("PB"){
        match value.trim_right_matches("PB").parse::<u64>(){
            Ok(n) => return Some(n  * 1024 * 1024 * 1024 * 1024 * 1024),
            Err(_) => return None,
        };
    }else if value.ends_with("TB"){
        match value.trim_right_matches("TB").parse::<u64>(){
            Ok(n) => return Some(n  * 1024 * 1024 * 1024 * 1024),
            Err(_) => return None,
        };
    }else if value.ends_with("GB"){
        match value.trim_right_matches("GB").parse::<u64>(){
            Ok(n) => return Some(n  * 1024 * 1024 * 1024),
            Err(_) => return None,
        };
    } else if value.ends_with("MB"){
        match value.trim_right_matches("MB").parse::<u64>(){
            Ok(n) => return Some(n  * 1024 * 1024),
            Err(_) => return None,
        };
    }else if value.ends_with("KB"){
        match value.trim_right_matches("KB").parse::<u64>(){
            Ok(n) => return Some(n  * 1024),
            Err(_) => return None,
        };
    }else if value.ends_with("Bytes"){
        match value.trim_right_matches("Bytes").parse::<u64>(){
            Ok(n) => return Some(n),
            Err(_) => return None,
        };
    }else {
        return None;
    }
}

//Lists all available volume names
pub fn volume_list()->Option<Vec<String>>{
    let mut arg_list: Vec<String>  = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("list".to_string());
    let output = run_command("gluster", &arg_list, true, false);
    let status = output.status;

    if !status.success(){
        debug!("Volume list get command failed");
        return None;
    }
    let output_str:String = match String::from_utf8(output.stdout){
        Ok(n) => n,
        Err(_) => {
            debug!("Volume list output transformation to utf8 failed");
            return None
        },
    };
    let mut volume_names: Vec<String> = Vec::new();
    for line in output_str.lines(){
        if line.is_empty(){
            //Skip any blank lines in the output
            continue;
        }
        volume_names.push(line.trim().to_string());
    }
    return Some(volume_names);
}

//TODO: now we can unit test this :) woo!
fn parse_volume_info(volume: &str, output_str: String)->Result<Volume, GlusterError>{
    //Variables we will return in a struct
    let mut transport_type = String::new();
    let mut volume_type = String::new();
    let mut name = String::new();
    let mut status = String::new();
    let mut bricks: Vec<Brick> = Vec::new();
    let mut id = Uuid::nil();

    if output_str.trim() == "No volumes present"{
        debug!("No volumes present");
        println!("No volumes present");
        return Err(GlusterError::NoVolumesPresent);
    }

    if output_str.trim() == format!("Volume {} does not exist", volume){
        debug!("Volume {} does not exist", volume);
        println!("Volume {} does not exist", volume);
        return Err(
            GlusterError::new(format!("Volume: {} does not exist", volume))
        );
    }

    for line in output_str.lines(){
        if line.is_empty(){
            //Skip the first blank line in the output
            continue;
        }
        if line.starts_with("Volume Name"){
            name = split_and_return_field(2, line.to_string());
        }
        if line.starts_with("Type"){
            volume_type = split_and_return_field(1, line.to_string());
        }
        if line.starts_with("Volume ID"){
            let x = split_and_return_field(2, line.to_string());
            id = try!(Uuid::parse_str(&x));
        }
        if line.starts_with("Status"){
            status = split_and_return_field(1, line.to_string());
        }
        if line.starts_with("Transport-Type"){
            transport_type = split_and_return_field(1, line.to_string());
        }
        if line.starts_with("Number of Bricks"){

        }
        if line.starts_with("Brick"){
            //Decend into parsing the brick list
            //need a regex here :(
            let re = Regex::new(r"Brick\d+").unwrap();
            if re.is_match(line){
                let brick_str = split_and_return_field(1, line.to_string());
                let brick_parts: Vec<&str> = brick_str.split(":").collect();
                assert!(brick_parts.len() == 2, "Failed to parse bricks from gluster vol info");

                let mut hostname = brick_parts[0].trim().to_string();

                //Translate back into an IP address if needed
                let check_for_ip = hostname.parse::<Ipv4Addr>();

                if check_for_ip.is_err(){
                    //It's a hostname so lets resolve it
                    hostname = match resolve_to_ip(&hostname){
                        Ok(ip_addr) => ip_addr,
                        Err(e) => {
                            return Err(
                                GlusterError::new(format!(
                                    "Failed to resolve hostname: {}. Error: {}", &hostname, e))
                            );
                        }
                    };
                }

                let peer: Peer = try!(get_peer(&hostname.to_string()));
                debug!("get_peer_by_ipaddr result: Peer: {:?}", peer);
                let brick = Brick{
                    //Should this panic if it doesn't work?
                    peer: peer,
                    path: PathBuf::from(brick_parts[1].to_string()),
                };
                bricks.push(brick);
            }
        }
    }
    let transport = Transport::new(&transport_type);
    let vol_type = VolumeType::new(&volume_type);
    let vol_info = Volume{name: name,
        vol_type: vol_type,
        id: id,
        status: status,
        transport: transport,
        bricks: bricks};
    return Ok(vol_info);
}

pub fn volume_info(volume: &str) -> Result<Volume, GlusterError> {
    let mut arg_list: Vec<String>  = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("info".to_string());
    arg_list.push(volume.to_string());
    let output = run_command("gluster", &arg_list, true, false);
    let status = output.status;

    if !status.success(){
        debug!("Volume info get command failed");
        println!("Volume info get command failed with error: {}",
            String::from_utf8(output.stdout).unwrap());

        //TODO: What is the appropriate error to report here?
        //The client is using this to figure out if it should make a volume
        return Err(GlusterError::NoVolumesPresent);
    }
    let output_str:String = try!(String::from_utf8(output.stdout));

    return parse_volume_info(&volume, output_str);
}


//Return a list of quotas on the volume if any
pub fn quota_list(volume: &str)->Option<Vec<Quota>>{
/*
  ThinkPad-T410s:~# gluster vol quota test list
                    Path                   Hard-limit Soft-limit   Used  Available  Soft-limit exceeded? Hard-limit exceeded?
  ---------------------------------------------------------------------------------------------------------------------------
  /                                        100.0MB       80%      0Bytes 100.0MB              No                   No

  There are 2 ways to get quota information
  1. List the quota's with the quota list command.  This command has been known in the past to hang
  in certain situations.
  2. Go to the backend brick and getfattr -d -e hex -m . dir_name/ on the directory directly:
    /mnt/x1# getfattr -d -e hex -m . quota/
    # file: quota/
    trusted.gfid=0xdb2443e4742e4aaf844eee40405ad7ae
    trusted.glusterfs.dht=0x000000010000000000000000ffffffff
    trusted.glusterfs.quota.00000000-0000-0000-0000-000000000001.contri=0x0000000000000000
    trusted.glusterfs.quota.dirty=0x3000
    trusted.glusterfs.quota.limit-set=0x0000000006400000ffffffffffffffff
    trusted.glusterfs.quota.size=0x0000000000000000
  TODO: link to the c xattr library #include <sys/xattr.h> and implement method 2
*/
    let mut quota_list: Vec<Quota> = Vec::new();
    let mut args_list: Vec<String> = Vec::new();
    args_list.push("gluster".to_string());
    args_list.push("volume".to_string());
    args_list.push("quota".to_string());
    args_list.push(volume.to_string());
    args_list.push("list".to_string());

    let output = run_command("gluster", &args_list, true, false);
    let status = output.status;

    //Rule out case of quota's being disabled on the volume
    if !status.success(){
        return None;
    }

    let output_str = match String::from_utf8(output.stdout){
        Ok(s) => s,
        //TODO: We're eating the error here
        Err(_) => return None,
    };

    if output_str.trim() == format!("quota: No quota configured on volume {}", volume){
        return None;
    }
    for line in output_str.lines(){
        if line.is_empty(){
            //Skip the first blank line in the output
            continue;
        }
        if line.starts_with(" "){
            continue;
        }
        if line.starts_with("-"){
            continue;
        }
        //Ok now that we've eliminated the garbage
        let parts: Vec<&str> = line.split(" ").filter(|s| !s.is_empty()).collect::<Vec<&str>>();
        //Output should match: ["/", "100.0MB", "80%", "0Bytes", "100.0MB", "No", "No"]
        if parts.len() > 3{
            let limit = match translate_to_bytes(parts[1]){
                Some(v) => v,
                //TODO:  is this sane?
                None => 0,
            };
            let used = match translate_to_bytes(parts[3]){
                Some(v) => v,
                //TODO:  is this sane?
                None => 0,
            };
            let quota = Quota{
                path: PathBuf::from(parts[0].to_string()),
                limit: limit,
                used: used,
            };
            quota_list.push(quota);
        }
        //else?
    }
    return Some(quota_list);
}

pub fn volume_enable_quotas(volume: &str)->Result<i32, GlusterError>{
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("quota".to_string());
    arg_list.push(volume.to_string());
    arg_list.push("enable".to_string());

    return process_output(run_command("gluster", &arg_list, true, false));
}

pub fn volume_disable_quotas(volume: &str)->Result<i32, GlusterError>{
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("quota".to_string());
    arg_list.push(volume.to_string());
    arg_list.push("disable".to_string());

    return process_output(run_command("gluster", &arg_list, true, false));
}

pub fn volume_add_quota(
    volume: &str,
    path: PathBuf,
    size: u64)->Result<i32,GlusterError>{

    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("quota".to_string());
    arg_list.push(volume.to_string());
    arg_list.push("limit-usage".to_string());
    arg_list.push(path.to_str().unwrap().to_string());
    arg_list.push(size.to_string());

    return process_output(run_command("gluster", &arg_list, true, false));
}
/*
pub fn volume_shrink_replicated(volume: &str,
    replica_count: usize,
    bricks: Vec<Brick>,
    force: bool) -> Result<i32,String> {
    //volume remove-brick <VOLNAME> [replica <COUNT>] <BRICK> ... <start|stop|status|c
    //ommit|force> - remove brick from volume <VOLNAME>
}
*/
fn ok_to_remove()->bool{
    return true;
}

pub fn volume_remove_brick(volume: &str,
    bricks: Vec<Brick>,
    force: bool) -> Result<i32, GlusterError>{

    if bricks.is_empty(){
        return Err(GlusterError::new("The brick list is empty. Not shrinking volume".to_string()));
    }

    if ok_to_remove(){
        let mut arg_list: Vec<String> = Vec::new();
        arg_list.push("volume".to_string());
        arg_list.push("remove-brick".to_string());
        arg_list.push(volume.to_string());

        if force{
            arg_list.push("force".to_string());
        }
        arg_list.push("start".to_string());

        return process_output(run_command("gluster", &arg_list, true, true));
    }else{
        return Err(GlusterError::new("Unable to remove brick due to redundancy failure".to_string()));
    }
}

//volume add-brick <VOLNAME> [<stripe|replica> <COUNT>]
//<NEW-BRICK> ... [force] - add brick to volume <VOLNAME>
pub fn volume_add_brick(volume: &str,
    bricks: Vec<Brick>,
    force: bool) -> Result<i32,GlusterError> {

    if bricks.is_empty(){
        return Err(GlusterError::new("The brick list is empty. Not expanding volume".to_string()));
    }

    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("add-brick".to_string());
    arg_list.push(volume.to_string());

    for brick in bricks.iter(){
        arg_list.push(brick.to_string());
    }
    if force{
        arg_list.push("force".to_string());
    }
    return process_output(run_command("gluster", &arg_list, true, true));
}

pub fn volume_start(volume: &str, force: bool) -> Result<i32, GlusterError>{
    //Should I check the volume exists first?
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("start".to_string());
    arg_list.push(volume.to_string());

    if force {
        arg_list.push("force".to_string());
    }
    return process_output(run_command("gluster", &arg_list, true, true));
}

pub fn volume_stop(volume: &str, force: bool) -> Result<i32, GlusterError>{
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("stop".to_string());
    arg_list.push(volume.to_string());

    if force {
        arg_list.push("force".to_string());
    }
    return process_output(run_command("gluster", &arg_list, true, true));
}

pub fn volume_delete(volume: &str) -> Result<i32, GlusterError>{
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("delete".to_string());
    arg_list.push(volume.to_string());

    return process_output(run_command("gluster", &arg_list, true, true));
}

pub fn volume_rebalance(volume: &str){
    //Usage: volume rebalance <VOLNAME> {{fix-layout start} | {start [force]|stop|status}}
}

fn volume_create<T: ToString>(volume: &str,
    options: HashMap<VolumeTranslator,T>,
    transport: &Transport,
    bricks: Vec<Brick>,
    force: bool) ->Result<i32, GlusterError>{

    if bricks.is_empty(){
        return Err(GlusterError::new("The brick list is empty. Not creating volume".to_string()));
    }

    /*
    //TODO: figure out how to check each VolumeTranslator type
    if (bricks.len() % replica_count) != 0 {
        return Err("The brick list and replica count are not multiples. Not creating volume".to_string());
    }
    */

    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("create".to_string());
    arg_list.push(volume.to_string());

    for (key, value) in options.iter(){
        arg_list.push(key.clone().to_string());
        arg_list.push(value.to_string());
    }

    arg_list.push("transport".to_string());
    arg_list.push(transport.clone().to_string());

    for brick in bricks.iter(){
        arg_list.push(brick.to_string());
    }
    if force{
        arg_list.push("force".to_string());
    }
    return process_output(run_command("gluster", &arg_list, true, true));
}


pub fn volume_create_replicated(volume: &str,
    replica_count: usize,
    transport: Transport,
    bricks: Vec<Brick>,
    force: bool) ->Result<i32, GlusterError>{

    let mut volume_translators: HashMap<VolumeTranslator, String> = HashMap::new();
    volume_translators.insert(VolumeTranslator::Replica, replica_count.to_string());

    return volume_create(volume, volume_translators, &transport, bricks, force);
}

pub fn volume_create_striped(volume: &str,
    stripe: usize,
    transport: Transport,
    bricks: Vec<Brick>,
    force: bool)->Result<i32, GlusterError>{

    let mut volume_translators: HashMap<VolumeTranslator, String> = HashMap::new();
    volume_translators.insert(VolumeTranslator::Stripe, stripe.to_string());

    return volume_create(volume, volume_translators, &transport, bricks, force);
}

pub fn volume_create_striped_replicated(volume: &str,
    stripe: usize,
    replica: usize,
    transport: Transport,
    bricks: Vec<Brick>,
    force: bool)->Result<i32, GlusterError>{

    let mut volume_translators: HashMap<VolumeTranslator, String> = HashMap::new();
    volume_translators.insert(VolumeTranslator::Stripe, stripe.to_string());
    volume_translators.insert(VolumeTranslator::Replica, replica.to_string());

    return volume_create(volume, volume_translators, &transport, bricks, force);
}

pub fn volume_create_distributed(volume: &str,
    transport: Transport,
    bricks: Vec<Brick>,
    force: bool)->Result<i32, GlusterError>{

    let volume_translators: HashMap<VolumeTranslator, String> = HashMap::new();

    return volume_create(volume, volume_translators, &transport, bricks, force);

}

pub fn volume_create_erasure(volume: &str,
    disperse: usize,
    redundancy: usize,
    transport: Transport,
    bricks: Vec<Brick>,
    force: bool)->Result<i32, GlusterError>{

    let mut volume_translators: HashMap<VolumeTranslator, String> = HashMap::new();
    volume_translators.insert(VolumeTranslator::Disperse, disperse.to_string());
    volume_translators.insert(VolumeTranslator::Redundancy, redundancy.to_string());

    return volume_create(volume, volume_translators, &transport, bricks, force);

}
