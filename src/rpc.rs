extern crate byteorder;
extern crate unix_socket;

use std::io::Cursor;
use self::byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::prelude::*;
use std::collections::HashMap;
use self::unix_socket::UnixStream;

pub const GLUSTER_CLI_PROGRAM_NUMBER: i32 = 1238463;
pub const GLUSTER_V2_CRED_FLAVOR: i32 = 390039;
pub const GLUSTER_QUOTA_PROGRAM_NUMBER: i32 = 29852134;
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
    extern crate byteorder;
    extern crate unix_socket;
    use std::io::Cursor;
    use std::path::Path;
    use self::unix_socket::UnixStream;
    use self::byteorder::{BigEndian, ReadBytesExt};
    use super::Pack;
    use super::UnPack;
    use std::collections::HashMap;

    #[test]
    fn list_peers(){
        //These will be used to verify that call and unpack did the correct thing
        //We're mocking the call and response

        //This is what the call bytes should look like after being packed
        //XDR says every 4 bytes is a value so I've arranged this vertically to help visualize that.
        let packed_call_result_bytes: Vec<u8> = vec![
            0x00,0x00,0x00,0x01, //msg_type
            0x00,0x00,0x00,0x00, //union?
            0x00,0x00,0x00,0x02, //union?
            0x00,0x12,0xe5,0xbf, //prog_num
            0x00,0x00,0x00,0x02, //prog_vers
            0x00,0x00,0x00,0x03, //proc_num
            0x00,0x05,0xf3,0x97, //cred
            0x00,0x00,0x00,0x18, //verf

            0x00,0x00,0x00,0x00,
            0x00,0x00,0x00,0x00,
            0x00,0x00,0x00,0x00,
            0x00,0x00,0x00,0x00,
            0x00,0x00,0x00,0x04,
            0x00,0x00,0x00,0x00,
            0x00,0x00,0x00,0x00,
            0x00,0x00,0x00,0x00,

            //RPC request struct
            0x00,0x00,0x00,0x02,
            0x00,0x00,0x00,0x00];

        let mut reply_bytes = vec![
            0x00,0x00,0x00,0x01, //msg_type
            0x00,0x00,0x00,0x01, //msg_type
            0x00,0x00,0x00,0x00,
            0x00,0x00,0x00,0x00,
            0x00,0x00,0x00,0x00,
            0x00,0x00,0x00,0x00,
            0x00,0x00,0x00,0x00,
            0x00,0x00,0x00,0x00,
            0x00,0x00,0x00,0x8d,
            0x00,0x00,0x00,0x04,
            0x00,0x00,0x00,0x05,
            0x00,0x00,0x00,0x02,
            0x63,0x6f,0x75,0x6e,
            0x74,0x00,0x31,0x00,
            0x00,0x00,0x00,0x11,
            0x00,0x00,0x00,0x02,
            0x66,0x72,0x69,0x65,
            0x6e,0x64,0x36,0x2e,
            0x63,0x6f,0x6e,0x6e,
            0x65,0x63,0x74,0x65,
            0x64,0x00,0x31,0x00,
            0x00,0x00,0x00,0x10,
            0x00,0x00,0x00,0x0a,
            0x66,0x72,0x69,0x65,
            0x6e,0x64,0x31,0x2e,
            0x68,0x6f,0x73,0x74,
            0x6e,0x61,0x6d,0x65,
            0x00,0x6c,0x6f,0x63,
            0x61,0x6c,0x68,0x6f,
            0x73,0x74,0x00,0x00,
            0x00,0x00,0x0c,0x00,
            0x00,0x00,0x25,0x66,
            0x72,0x69,0x65,0x6e,
            0x64,0x31,0x2e,0x75,
            0x75,0x69,0x64,0x00,
            0x34,0x30,0x37,0x32,
            0x36,0x62,0x38,0x30,
            0x2d,0x62,0x63,0x30,
            0x35,0x2d,0x34,0x31,
            0x66,0x33,0x2d,0x62,
            0x61,0x39,0x37,0x2d,
            0x35,0x30,0x34,0x63,
            0x65,0x33,0x33,0x30,
            0x31,0x65,0x63,0x65,
            0x00,0x00,0x00,0x00,
        ];

        let xid = 1;
        let prog = super::GLUSTER_CLI_PROGRAM_NUMBER;
        let vers = super::RPC_VERSION;
        let verf = super::GlusterAuth{
            flavor: super::AuthFlavor::AuthNull,
            stuff: vec![0,0,0,0]
        };
        let verf_bytes = verf.pack().unwrap();
        let creds = super::GlusterCred{
            flavor: super::GLUSTER_V2_CRED_FLAVOR,
            pid: 0,
            uid: 0,
            gid: 0,
            groups: "".to_string(),
            lock_owner: vec![0,0,0,0]
        };
        let cred_bytes = creds.pack().unwrap();

        let mut call_bytes = super::pack_cli_callheader(
            xid, prog, vers,
            super::GlusterCliCommand::GlusterCliListFriends, cred_bytes, verf_bytes).unwrap();

        let peer_request = super::GlusterCliPeerListRequest{
                flags: 2,
                dict: HashMap::new(), //This works now. Matches Gluster's strace output bytes
        };

        let peer_bytes = peer_request.pack().unwrap();
        println!("Peer bytes: {:?}", peer_bytes);
        for byte in peer_bytes{
            call_bytes.push(byte);
        }
        assert_eq!(call_bytes, packed_call_result_bytes);

        //Functional tests
        /*let addr = Path::new("/var/run/glusterd.socket");
        println!("Connecting to /var/run/glusterd.socket");
        let mut sock = UnixStream::connect(&addr).unwrap();

        let result = super::sendrecord(&mut sock, &call_bytes);
        println!("Result: {:?}", result);
        let mut reply_bytes = super::recvrecord(&mut sock).unwrap();
        println!("Reply bytes: ");
        super::print_fragment(&reply_bytes);
        */

        let mut cursor = Cursor::new(&mut reply_bytes[..]);
        let reply = super::unpack_replyheader(&mut cursor).unwrap();
        println!("Reply header parsed result: {:?}", reply);
        let peer_list = super::GlusterCliPeerListResponse::unpack(&mut cursor).unwrap();
        println!("Peer list: {:?}", peer_list);
    }
    #[test]
    fn list_quota(){

        let mut reply_bytes: Vec<u8> = vec![
            0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 117, 0, 0, 0, 0, 0, 0, 0, 228, 0, 0, 0, 6, 0, 0, 0, 4, 0, 0, 0, 2, 116, 121, 112, 101,
            0, 53, 0, 0, 0, 0, 28, 0, 0, 0, 24, 116, 114, 117, 115, 116, 101, 100, 46, 103, 108, 117,
            115, 116, 101, 114, 102, 115, 46, 113, 117, 111, 116, 97, 46, 115, 105, 122, 101, 0, 0, 0,
            0, 1, 58, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 23, 0, 0, 0, 2,
            103, 108, 117, 115, 116, 101, 114, 102, 115, 46, 97, 110, 99, 101, 115, 116, 114, 121, 46,
            112, 97, 116, 104, 0, 47, 0, 0, 0, 0, 21, 0, 0, 0, 16, 116, 114, 117, 115, 116, 101, 100, 46,
            103, 108, 117, 115, 116, 101, 114, 102, 115, 46, 100, 104, 116, 0, 0, 0, 0, 1, 0, 0, 0, 0,
            0, 0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 23, 0, 0, 0, 2, 103, 108, 117, 115, 116, 101, 114,
            102, 115, 46, 101, 110, 116, 114, 121, 108, 107, 45, 99, 111, 117, 110, 116, 0, 48, 0, 0,
            0, 0, 23, 0, 0, 0, 2, 103, 108, 117, 115, 116, 101, 114, 102, 115, 46, 105, 110, 111, 100,
            101, 108, 107, 45, 99, 111, 117, 110, 116, 0, 48, 0];

        let xid = 1;
        let prog = super::GLUSTER_QUOTA_PROGRAM_NUMBER;
        let vers = 1;

        let verf = super::GlusterAuth{
            flavor: super::AuthFlavor::AuthNull,
            stuff: vec![0,0,0,0]
        };
        let verf_bytes = verf.pack().unwrap();

        let creds = super::GlusterCred{
            flavor: super::GLUSTER_V2_CRED_FLAVOR,
            pid: 0,
            uid: 0,
            gid: 0,
            groups: "".to_string(),
            lock_owner: vec![0,0,0,0],
        };
        let cred_bytes = creds.pack().unwrap();

        let mut call_bytes = super::pack_quota_callheader(
            xid, prog, vers,
            super::GlusterAggregatorCommand::GlusterAggregatorGetlimit,
            cred_bytes, verf_bytes).unwrap();

        let mut dict: HashMap<String,Vec<u8>> = HashMap::new();

        let mut gfid = "00000000-0000-0000-0000-000000000001".to_string().into_bytes();
        gfid.push(0); //Null Terminate
        let mut name = "test".to_string().into_bytes();
        name.push(0); //Null Terminate
        let mut version = "1.20000005".to_string().into_bytes();
        version.push(0); //Null Terminate
        let mut vol_type  = "5".to_string().into_bytes();
        vol_type.push(0); //Null Terminate

        dict.insert("gfid".to_string(), gfid);
        dict.insert("type".to_string(), vol_type);
        dict.insert("volume-uuid".to_string(), name);
        dict.insert("version".to_string(), version);
        let quota_request = super::GlusterCliRequest{
                dict: dict,
        };
        let quota_bytes = quota_request.pack().unwrap();
        for byte in quota_bytes{
            call_bytes.push(byte);
        }

        /*
        let addr = Path::new("/var/run/gluster/quotad.socket");
        println!("Connecting to /var/run/gluster/quotad.socket");
        let mut sock = UnixStream::connect(&addr).unwrap();

        let result = super::sendrecord(&mut sock, &call_bytes);
        println!("Quota Result: {:?}", result);
        let mut reply_bytes = super::recvrecord(&mut sock).unwrap();
        */

        //println!("Quota Reply len: {}", reply_bytes.len());
        //println!("Quota Reply bytes: {:?}", &reply_bytes);
        let mut cursor = Cursor::new(&mut reply_bytes[..]);
        let reply_header = super::unpack_replyheader(&mut cursor).unwrap();

        let mut cli_response = super::GlusterCliResponse::unpack(&mut cursor).unwrap();
        println!("Quota reply: {:?}", &cli_response);
        //The raw bytes
        let mut quota_size_bytes = cli_response.dict.get_mut("trusted.glusterfs.quota.size").unwrap();

        let mut size_cursor = Cursor::new(&mut quota_size_bytes[..]);
        //Read u64 off the byte vector and get the decoded value
        let usage = size_cursor.read_u64::<BigEndian>().unwrap();
        println!("Quota usage: {}", usage);
    }

    #[test]
    fn serialize_dictionary_test(){
        let mut hm: HashMap<String,Vec<u8>> = HashMap::new();

        hm.insert("gfid".to_string(), "00000000-0000-0000-0000-000000000001".to_string().into_bytes());
        hm.insert("volume-uuid".to_string(), "test".to_string().into_bytes());
        hm.insert("default-soft-limit".to_string(), "80%".to_string().into_bytes());
        hm.insert("type".to_string(), "5".to_string().into_bytes());

        //let mut buffer:Vec<u8> = Vec::new();
        let mut dict_bytes = super::serialize_dict(&hm).unwrap().into_bytes();

        //Deserialize
        let mut cursor = Cursor::new(&mut dict_bytes[..]);
        let result_map = super::deserialize_dict(&mut cursor).unwrap();

        assert_eq!(hm, result_map);
    }
}

pub trait Pack{
    fn pack(&self) -> Result<Vec<u8>,super::GlusterError>;
}

pub trait UnPack{
    fn unpack<T: Read>(&mut T) -> Result<Self, super::GlusterError>;
}

fn unpack_string<T: Read>(data: &mut T, size: u32)->Result<String,super::GlusterError>{
    let mut buffer: Vec<u8> = Vec::with_capacity(size as usize);
    for _ in 0..size {
        let b = try!(data.read_u8());
        buffer.push(b);
    }
    let s = try!(String::from_utf8(buffer));
    return Ok(s);
}

fn unpack_dict_bytes<T: Read>(data: &mut T, size: u32)->Result<Vec<u8>,super::GlusterError>{
    let mut buffer: Vec<u8> = Vec::with_capacity(size as usize);
    for _ in 0..size {
        let b = try!(data.read_u8());
        buffer.push(b);
    }
    return Ok(buffer);
}

fn pack_string<T: Write>(s: &String, buffer: &mut T){
    let bytes = s.clone().into_bytes();
    let bytes_len = bytes.len();
    let pad = (4- (bytes_len % 4)) % 4;

    buffer.write_u32::<BigEndian>(bytes_len as u32).unwrap();

    for byte in bytes{
        buffer.write_u8(byte).unwrap();
    }

    //Padding
    for _ in 0..pad{
        buffer.write_u8(0).unwrap();
    }
}

#[derive(Debug,Clone)]
pub enum AuthFlavor{
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
pub struct GlusterCliRequest{
    pub dict: HashMap<String,Vec<u8>>
}

impl Pack for GlusterCliRequest{
    fn pack(&self)->Result<Vec<u8>,super::GlusterError>{
        let mut buffer:Vec<u8> = Vec::new();
        let dict_string: String = try!(serialize_dict(&self.dict));
        pack_string(&dict_string, &mut buffer);
        return Ok(buffer);
    }
}

impl UnPack for GlusterCliRequest{
    //Expects a cursor so calls can be chained
    fn unpack<R: Read>(data: &mut R)->Result<GlusterCliRequest, super::GlusterError>{
        let size = try!(data.read_u32::<BigEndian>());
        let mut s = try!(unpack_string(data, size)).into_bytes();
        let mut cursor = Cursor::new(&mut s[..]);
        let dict = try!(deserialize_dict(&mut cursor));

        return Ok(GlusterCliRequest{
            dict: dict
        })
    }
}

#[derive(Debug)]
pub struct GlusterCliResponse{
    pub op_ret: i32,
    pub op_errno: i32,
    pub op_errstr: String,
    pub dict: HashMap<String,Vec<u8>>
}

impl Pack for GlusterCliResponse{
    fn pack(&self)->Result<Vec<u8>,super::GlusterError>{
        let mut buffer: Vec<u8> = Vec::new();
        buffer.write_i32::<BigEndian>(self.op_ret).unwrap();
        buffer.write_i32::<BigEndian>(self.op_errno).unwrap();
        pack_string(&self.op_errstr, &mut buffer);
        let dict_string = try!(serialize_dict(&self.dict));
        pack_string(&dict_string, &mut buffer);
        return Ok(buffer);
    }
}

impl UnPack for GlusterCliResponse{
    fn unpack<R: Read>(data: &mut R)->Result<GlusterCliResponse, super::GlusterError>{
        let op_ret = try!(data.read_i32::<BigEndian>());
        let op_errno = try!(data.read_i32::<BigEndian>());

        let size = try!(data.read_u32::<BigEndian>());
        let op_errstr = try!(unpack_string(data, size));

        //Unpack the opaque string and then deserialize it into a dict
        let size = try!(data.read_u32::<BigEndian>());
        let mut s = try!(unpack_dict_bytes(data, size));
        let mut cursor = Cursor::new(&mut s[..]);
        let dict = try!(deserialize_dict(&mut cursor));

        return Ok(GlusterCliResponse{
            op_ret: op_ret,
            op_errno: op_errno,
            op_errstr: op_errstr,
            dict: dict
        })
    }
}

#[derive(Debug)]
pub struct GlusterCliPeerListRequest{
    pub flags: i32,
    pub dict: HashMap<String,Vec<u8>>
}

impl Pack for GlusterCliPeerListRequest{
    fn pack(&self)->Result<Vec<u8>,super::GlusterError>{
        //XDRlib has a bug where it doesn't pad Strings correctly that are size 0
        let mut buffer: Vec<u8> = Vec::new();
        try!(buffer.write_i32::<BigEndian>(self.flags));
        let dict_string = try!(serialize_dict(&self.dict));
        pack_string(&dict_string, &mut buffer);
        return Ok(buffer);
    }
}

impl UnPack for GlusterCliPeerListRequest{
    fn unpack<R: Read>(data: &mut R)->Result<GlusterCliPeerListRequest, super::GlusterError>{
        let flags = try!(data.read_i32::<BigEndian>());

        let size = try!(data.read_u32::<BigEndian>());
        let mut s = try!(unpack_dict_bytes(data, size));
        let mut cursor = Cursor::new(&mut s[..]);
        let dict = try!(deserialize_dict(&mut cursor));

        return Ok(GlusterCliPeerListRequest{
            flags: flags,
            dict: dict,
        })
    }
}

#[derive(Debug)]
pub struct GlusterCliPeerListResponse{
    pub op_ret: i32,
    pub op_errno: i32,
    //Replace all dict instances with the correct parameters
    //For example this should be a Vec<Friend> ?
    //That way dict can deserialize the opaque crap
    //and then we can deserialize into the correct type at unpack time
    pub friends: HashMap<String,Vec<u8>>,
}

impl Pack for GlusterCliPeerListResponse{
    fn pack(&self)->Result<Vec<u8>,super::GlusterError>{
        let mut buffer:Vec<u8> = Vec::new();
        try!(buffer.write_i32::<BigEndian>(self.op_ret));
        try!(buffer.write_i32::<BigEndian>(self.op_errno));
        let dict_string = try!(serialize_dict(&self.friends));
        pack_string(&dict_string, &mut buffer);
        return Ok(buffer);
    }
}

impl UnPack for GlusterCliPeerListResponse{
    fn unpack<R: Read>(data: &mut R)->Result<GlusterCliPeerListResponse, super::GlusterError>{
        let op_ret = try!(data.read_i32::<BigEndian>());
        let op_errno = try!(data.read_i32::<BigEndian>());

        let size = try!(data.read_u32::<BigEndian>());
        let mut s = try!(unpack_dict_bytes(data, size));
        let mut cursor = Cursor::new(&mut s[..]);
        let friends = try!(deserialize_dict(&mut cursor));

        return Ok(GlusterCliPeerListResponse{
            op_ret: op_ret,
            op_errno: op_errno,
            friends: friends,
        })
    }
}

#[derive(Debug)]
pub struct GlusterCliFsmLogRequest{
    pub name: String,
}

impl Pack for GlusterCliFsmLogRequest{
    fn pack(&self)->Result<Vec<u8>,super::GlusterError>{
        let mut buffer:Vec<u8> = Vec::new();
        pack_string(&self.name, &mut buffer);
        return Ok(buffer);
    }
}

impl UnPack for GlusterCliFsmLogRequest{
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliFsmLogRequest, super::GlusterError>{
        let size = try!(data.read_u32::<BigEndian>());
        let name = try!(unpack_string(data, size));

        return Ok(GlusterCliFsmLogRequest{
            name: name
        })
    }
}

#[derive(Debug)]
pub struct GlusterCliFsmLogReponse{
    pub op_ret: i32,
    pub op_errno: i32,
    pub op_errstr: String,
    pub fsm_log: String,
}

impl Pack for GlusterCliFsmLogReponse{
    fn pack(&self)->Result<Vec<u8>,super::GlusterError>{
        let mut buffer:Vec<u8> = Vec::new();
        try!(buffer.write_i32::<BigEndian>(self.op_ret));
        try!(buffer.write_i32::<BigEndian>(self.op_errno));
        pack_string(&self.op_errstr, &mut buffer);
        pack_string(&self.fsm_log, &mut buffer);
        return Ok(buffer);
    }
}
impl UnPack for GlusterCliFsmLogReponse{
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliFsmLogReponse, super::GlusterError>{
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
pub struct GlusterCliGetwdRequest{
    pub unused:i32,
}

impl Pack for GlusterCliGetwdRequest{
    fn pack(&self)->Result<Vec<u8>,super::GlusterError>{
        let mut buffer:Vec<u8> = Vec::new();
        try!(buffer.write_i32::<BigEndian>(self.unused));
        return Ok(buffer);
    }
}

impl UnPack for GlusterCliGetwdRequest{
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliGetwdRequest, super::GlusterError>{
        let unused = try!(data.read_i32::<BigEndian>());
        return Ok(GlusterCliGetwdRequest{
            unused: unused
        })
    }
}

#[derive(Debug)]
pub struct GlusterCliGetwdResponse{
    pub op_ret: i32,
    pub op_errno: i32,
    pub wd: String
}

impl Pack for GlusterCliGetwdResponse{
    fn pack(&self)->Result<Vec<u8>,super::GlusterError>{
        let mut buffer:Vec<u8> = Vec::new();
        try!(buffer.write_i32::<BigEndian>(self.op_ret));
        try!(buffer.write_i32::<BigEndian>(self.op_errno));
        pack_string(&self.wd, &mut buffer);
        return Ok(buffer);
    }
}

impl UnPack for GlusterCliGetwdResponse{
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliGetwdResponse, super::GlusterError>{
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
pub struct GlusterCliMountRequest{
    pub label: String,
    pub dict: HashMap<String,Vec<u8>>,
}

impl Pack for GlusterCliMountRequest{
    fn pack(&self)->Result<Vec<u8>,super::GlusterError>{
        let mut buffer:Vec<u8> = Vec::new();
        pack_string(&self.label, &mut buffer);
        let dict_string = try!(serialize_dict(&self.dict));
        pack_string(&dict_string, &mut buffer);
        return Ok(buffer);
    }
}

impl UnPack for GlusterCliMountRequest{
    fn unpack<R: Read>(data: &mut R)->Result<GlusterCliMountRequest, super::GlusterError>{
        let size = try!(data.read_u32::<BigEndian>());
        let label = try!(unpack_string(data, size));

        let size = try!(data.read_u32::<BigEndian>());
        let mut s = try!(unpack_string(data, size)).into_bytes();
        let mut cursor = Cursor::new(&mut s[..]);
        let dict = try!(deserialize_dict(&mut cursor));

        return Ok(GlusterCliMountRequest{
            label: label,
            dict: dict,
        })
    }
}

#[derive(Debug)]
pub struct GlusterCliMountResponse{
    pub op_ret: i32,
    pub op_errno: i32,
    pub path: String,
}

impl Pack for GlusterCliMountResponse{
    fn pack(&self)->Result<Vec<u8>,super::GlusterError>{
        let mut buffer:Vec<u8> = Vec::new();
        try!(buffer.write_i32::<BigEndian>(self.op_ret));
        try!(buffer.write_i32::<BigEndian>(self.op_errno));
        pack_string(&self.path, &mut buffer);
        return Ok(buffer);
    }
}

impl UnPack for GlusterCliMountResponse{
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliMountResponse, super::GlusterError>{
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
pub struct GlusterCliUmountRequest{
    pub lazy: i32,
    pub path: String,
}

impl Pack for GlusterCliUmountRequest{
    fn pack(&self)->Result<Vec<u8>,super::GlusterError>{
        let mut buffer:Vec<u8> = Vec::new();
        try!(buffer.write_i32::<BigEndian>(self.lazy));
        pack_string(&self.path, &mut buffer);
        return Ok(buffer);
    }
}

impl UnPack for GlusterCliUmountRequest{
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliUmountRequest, super::GlusterError>{
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
pub struct GlusterCliUmountResponse{
    pub op_ret:i32,
    pub op_errno: i32,
}

impl Pack for GlusterCliUmountResponse{
    fn pack(&self)->Result<Vec<u8>,super::GlusterError>{
        let mut buffer:Vec<u8> = Vec::new();
        try!(buffer.write_i32::<BigEndian>(self.op_ret));
        try!(buffer.write_i32::<BigEndian>(self.op_errno));
        return Ok(buffer);
    }
}

impl UnPack for GlusterCliUmountResponse{
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliUmountResponse, super::GlusterError>{
        let op_ret = try!(data.read_i32::<BigEndian>());
        let op_errno = try!(data.read_i32::<BigEndian>());

        return Ok(GlusterCliUmountResponse{
            op_ret: op_ret,
            op_errno: op_errno,
        })
    }
}

#[derive(Debug)]
pub struct GlusterAuth{
    pub flavor: AuthFlavor,//i32,
    pub stuff: Vec<u8>,
    //I think I'm missing a field here
    //$29 = {pid = 0, uid = 0, gid = 0, groups = {groups_len = 0, groups_val = 0x0},
    //lk_owner = {lk_owner_len = 4,
    //lk_owner_val = 0x7ffff4119b50 ""}}
}

#[derive(Debug)]
pub struct GlusterCred{
    pub flavor: i32,

    //Experimental
    //I think this is supposed to be a string?
    pub pid: u32,
    pub uid: u32,
    pub gid: u32,
    pub groups: String,
    pub lock_owner: Vec<u8>, // I wish I knew what this was
    /*
    //Maybe that's what all this crap is
    owner[0] = (char)(au.pid & 0xff);
    owner[1] = (char)((au.pid >> 8) & 0xff);
    owner[2] = (char)((au.pid >> 16) & 0xff);
    owner[3] = (char)((au.pid >> 24) & 0xff);
    1361
    au.lk_owner.lk_owner_val = owner;
    au.lk_owner.lk_owner_len = 4;
     */
}

impl Pack for GlusterAuth{
    fn pack(&self)->Result<Vec<u8>,super::GlusterError>{
        let mut buffer:Vec<u8> = Vec::new();
        try!(buffer.write_i32::<BigEndian>(self.flavor.clone() as i32));

        for b in &self.stuff{
            buffer.push(b.clone());
        }

        return Ok(buffer);
    }
}

impl Pack for GlusterCred{
    fn pack(&self) -> Result<Vec<u8>, super::GlusterError>{
        let mut buffer:Vec<u8> = Vec::new();

        try!(buffer.write_i32::<BigEndian>(self.flavor));

        //Write the size of the next chunk
        try!(buffer.write_u32::<BigEndian>(24));

        try!(buffer.write_u32::<BigEndian>(self.pid)); //4
        try!(buffer.write_u32::<BigEndian>(self.uid)); //8
        try!(buffer.write_u32::<BigEndian>(self.gid)); //12
        pack_string(&self.groups, &mut buffer); //16?

        //lock_owner length
        try!(buffer.write_u32::<BigEndian>(4)); //12
        for b in &self.lock_owner{
            buffer.push(b.clone());
        }
        //pack_string(&self.lock_owner, &mut buffer); //20-24?

        println!("Credential Bytes: {:?}", &buffer);

        return Ok(buffer);
    }
}

/*pub fn pack_gluster_v2_cred(flavor: i32)->Result<Vec<u8>, super::GlusterError>{
    let mut buffer:Vec<u8> = Vec::new();
    try!(buffer.write_i32::<BigEndian>(flavor));

    //Experimental
    try!(buffer.write_u32::<BigEndian>(self.pid));
    try!(buffer.write_u32::<BigEndian>(self.uid));
    try!(buffer.write_u32::<BigEndian>(self.gid));
    pack_string(self.groups, &mut buffer);
    pack_string(self.lock_owner, &mut buffer);
    /*
    try!(buffer.write_i32::<BigEndian>(24));
    try!(buffer.write_i32::<BigEndian>(0));
    try!(buffer.write_i32::<BigEndian>(0));
    try!(buffer.write_i32::<BigEndian>(0));
    try!(buffer.write_i32::<BigEndian>(0));
    try!(buffer.write_i32::<BigEndian>(4));

    //Test
    //TODO: That works.  What are these two fields supposed to be??
    try!(buffer.write_u32::<BigEndian>(0));
    try!(buffer.write_u32::<BigEndian>(0));
    */
    return Ok(buffer);
}
*/
pub fn pack_quota_callheader(xid: u32, prog: i32, vers: u32, proc_num: GlusterAggregatorCommand,
    cred_flavor: Vec<u8>, verf: Vec<u8>)->Result<Vec<u8>, super::GlusterError>{
        return pack_callheader(xid, prog, vers, proc_num as u32, cred_flavor, verf);
}

pub fn pack_cli_callheader(xid: u32, prog: i32, vers: u32, proc_num: GlusterCliCommand,
    cred_flavor: Vec<u8>, verf: Vec<u8>)->Result<Vec<u8>, super::GlusterError>{
        return pack_callheader(xid, prog, vers, proc_num as u32, cred_flavor, verf);
}

fn pack_callheader(xid: u32, prog: i32, vers: u32, proc_num: u32,
    cred_flavor: Vec<u8>, verf: Vec<u8>)->Result<Vec<u8>, super::GlusterError>{
    let mut buffer:Vec<u8> = Vec::new();

    try!(buffer.write_u32::<BigEndian>(xid));
    try!(buffer.write_i32::<BigEndian>(CALL));
    try!(buffer.write_u32::<BigEndian>(RPC_VERSION));
    try!(buffer.write_i32::<BigEndian>(prog));
    try!(buffer.write_u32::<BigEndian>(vers));
    try!(buffer.write_u32::<BigEndian>(proc_num));

    for byte in cred_flavor{
        buffer.push(byte);
    }

    for byte in verf{
        buffer.push(byte);
    }
    // Caller must add procedure-specific part of call
    return Ok(buffer);
}

#[cfg(target_endian="little")]
fn htonl(num: u32)->u32{
    return 0;
}

#[cfg(target_endian="big")]
fn htonl(num: u32)->u32{
    return 0;
}

//Takes a generic which will most likely be a Cursor
//That way the next call can also use the last cursor position
pub fn unpack_replyheader<T: Read>(data: &mut T)->Result<(u32, GlusterAuth), super::GlusterError>{
    let xid = try!(data.read_u32::<BigEndian>());
    println!("reply xid {}", xid);
    let msg_type = try!(data.read_i32::<BigEndian>());
    println!("reply msg_type {}", xid);

    if msg_type != REPLY{
        //Invalid REPLY
        return Err(super::GlusterError::new(format!("Invalid reply with msg_type: {}", msg_type)));
    }

    let stat = try!(data.read_i32::<BigEndian>());
    println!("reply stat {}", xid);
    if stat == MSG_DENIED {
        let reason = try!(data.read_i32::<BigEndian>());
        if reason == RPC_MISMATCH{
            let low = try!(data.read_u32::<BigEndian>());
            let high = try!(data.read_u32::<BigEndian>());
            return Err(super::GlusterError::new(format!("MSG_DENIED: RPC_MISMATCH low: {} high: {}", low, high)));
        }
        if reason == AUTH_ERROR {
            let err = try!(data.read_u32::<BigEndian>());
            return Err(super::GlusterError::new(format!("MSG_DENIED: AUTH_ERROR {}", err)));
        }
        return Err(super::GlusterError::new(format!("MSG_DENIED: {}", reason)));
    }
    if stat == MSG_ACCEPTED{
        let auth_flavor = try!(data.read_i32::<BigEndian>());

        let size = try!(data.read_u32::<BigEndian>());
        let stuff = try!(unpack_string(data, size));

        let accept_message = try!(data.read_i32::<BigEndian>());
        //Parse auth_flavor into the enum
        let rpc_auth = GlusterAuth{
            flavor: AuthFlavor::new(auth_flavor),
            stuff: stuff.into_bytes(),
        };
        match accept_message{
            PROG_UNAVAIL => {
                return Err(super::GlusterError::new("call failed PROG_UNAVAIL".to_string()));
            },
            PROG_MISMATCH => {
                let low = try!(data.read_u32::<BigEndian>());
                let high = try!(data.read_u32::<BigEndian>());
                return Err(super::GlusterError::new(format!("Call failed: PROG_MISMATCH low: {} high: {}", low, high)));
            }
            PROC_UNAVAIL => {
                return Err(super::GlusterError::new("call failed PROC_UNAVAIL".to_string()));
            },
            GARBAGE_ARGS => {
                return Err(super::GlusterError::new("call failed GARBAGE_ARGS".to_string()));
            },
            SUCCESS => {
                return Ok((xid, rpc_auth));
            }
            _ => {
                return Err(super::GlusterError::new(format!("Call failed: {}", accept_message)));
            }
        }
    }else{
        return Err(super::GlusterError::new(format!("MSG neither denied or accepted: {}", stat)));
    }
}

pub fn send_fragment<T: Write>(socket: &mut T, last: bool, fragment: &Vec<u8>)->Result<usize,super::GlusterError>{
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

    let mut bytes_written = try!(socket.write(&header_buffer));

    println!("Fragment length: {}", fragment.len());
    println!("Sending fragment");

    print_fragment(&fragment);

    bytes_written += try!(socket.write(fragment));
    socket.flush().unwrap();
    return Ok(bytes_written);
}

pub fn sendrecord(sock: &mut UnixStream, record: &Vec<u8>)->Result<usize,super::GlusterError>{
    let send_size = try!(send_fragment(sock, true, &record));
    return Ok(send_size);
}

pub fn print_fragment(frag: &Vec<u8>){
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
pub fn recv_fragment<T: Read>(socket: &mut T)-> Result<(bool, Vec<u8>), super::GlusterError>{
    //Read at most 4 bytes
    let mut buffer: Vec<u8> = Vec::new();

    try!(socket.by_ref().take(4).read_to_end(&mut buffer));

    if buffer.len() < 4{
        return Err(super::GlusterError::new("EOF Error".to_string()));
    }
    let mut buf = Cursor::new(&buffer[..]);
    let header = try!(buf.read_u32::<BigEndian>());

    let last = (header & 0x80000000) != 0;
    println!("Last Fragment: {}", last);

    let mut n = header & 0x7fffffff;
    println!("Fragment length: {}", n);
    let mut fragment: Vec<u8> = Vec::new();
    while n > 0{
        //Might need to introduce a local buffer here.  I'm not sure yet
        let mut handle = socket.by_ref().take(n as u64);
        try!(handle.read_to_end(&mut fragment));
        n = n - n;
    }
    print_fragment(&fragment);
    return Ok((last, fragment));
}

pub fn recvrecord(sock: &mut UnixStream)->Result<Vec<u8>, super::GlusterError>{
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

//Used to get the Quota Information from the quotad server
#[derive(Debug)]
pub enum GlusterAggregatorCommand{
    GlusterAggregatorNull = 0,
    GlusterAggregatorLookup = 1,
    GlusterAggregatorGetlimit = 2,
    GlusterAggregatorMaxvalue = 3,
}

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
    GlusterCliBrick = 15,
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

fn unpack_key<T: Read>(data: &mut T, size: u32)->Result<String,super::GlusterError>{
    let v = try!(unpack_value(data, size, true));
    let s = try!(String::from_utf8(v));
    return Ok(s.trim_matches('\0').to_string());
}

fn unpack_value<T: Read>(data: &mut T, size: u32, skip_null: bool)->Result<Vec<u8>,super::GlusterError>{
    let mut buffer: Vec<u8> = Vec::with_capacity(size as usize);
    for _ in 0..size {
        let b = try!(data.read_u8());
        buffer.push(b);
    }
    if skip_null{
        try!(data.read_u8());
    }
    return Ok(buffer);
}

/*
Serialization format:
 -------- --------  --------  ----------- -------------
|  count | key len | val len | key     \0| value
 ---------------------------------------- -------------
 4        4         4       <key len>   <value len>
 //NOTE keys are NULL terminated but not value's
*/
//Takes a HashMap and a buffer to serialize the HashMap into
pub fn serialize_dict(dict: &HashMap<String,Vec<u8>>)->Result<String,super::GlusterError>{
    //Maybe the problem is that this whole thing is supposed to be packed into a string
    //yeah... that could be it.  I'm writing the raw shit to the wire when I should be
    //returning a string which is then pack_opaque
    let mut buffer: Vec<u8> = Vec::new();

    //Only write if there's something to write
    if dict.len() > 0{
        try!(buffer.write_u32::<BigEndian>(dict.len() as u32));

        for (key, value) in dict.iter(){
            try!(buffer.write_u32::<BigEndian>(key.len() as u32));
            try!(buffer.write_u32::<BigEndian>(value.len() as u32));
            for b in key.clone().into_bytes(){
                try!(buffer.write_u8(b));
            }
            //Null terminate key
            try!(buffer.write_u8(0x00));

            for b in value.clone(){
                try!(buffer.write_u8(b));
            }
        }
    }
    let ret_string = try!(String::from_utf8(buffer));
    return Ok(ret_string);
}

pub fn deserialize_dict<R: Read>(cursor: &mut R)->Result<HashMap<String,Vec<u8>>, super::GlusterError>{
    let count = try!(cursor.read_u32::<BigEndian>());
    let mut map  = HashMap::with_capacity(count as usize);
    for _ in 0..count{

        let key_len = try!(cursor.read_u32::<BigEndian>());
        let value_len = try!(cursor.read_u32::<BigEndian>());

        let key = try!(unpack_key(cursor, key_len));
        let value = try!(unpack_value(cursor, value_len, false));

        map.insert(key, value);
    }
    return Ok(map);
}
