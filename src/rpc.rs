extern crate byteorder;
extern crate unix_socket;
extern crate xdr;

use std::io::Cursor;
use self::byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::prelude::*;
use std::collections::HashMap;
use self::xdr::xdr::{XdrWriter};
use self::unix_socket::UnixStream;


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
    use self::unix_socket::UnixStream;
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
        let prog = 1238463;
        let vers = super::RPC_VERSION;
        let verf = super::GlusterAuth{
            flavor: 0,
            stuff: "".to_string(),
        };
        let verf_bytes = verf.pack();
        let cred_flavor = 390039;
        let creds = super::pack_gluster_v2_cred(cred_flavor);

        let mut call_bytes = super::pack_cli_callheader(
            xid, prog, vers, super::GlusterCliCommand::GlusterCliListFriends, creds, verf_bytes);

        let peer_request = super::GlusterCliPeerListRequest{
                flags: 2,
                dict: "".to_string()
        };

        let peer_bytes = peer_request.pack();
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
        let mut reply_bytes: Vec<u8> = vec![/*0x00,0x00,0x00,0x7d,*/
        //Count
        0x00,0x00,0x00,0x04, //4
        //Key len
        0x00,0x00,0x00,0x04, //4
        //Value len
        0x00,0x00,0x00,0x25, //37

        //Key
        0x67,0x66,0x69,0x64, //37
        0x00, //NULL terminator
        //Value
        0x30,0x30,0x30,
        0x30,0x30,0x30,0x30,
        0x30,0x2d,0x30,0x30,
        0x30,0x30,0x2d,0x30,
        0x30,0x30,0x30,0x2d,
        0x30,0x30,0x30,0x30,
        0x2d,0x30,0x30,0x30,
        0x30,0x30,0x30,0x30,
        0x30,0x30,0x30,0x30,
        0x31,0x00, // END Value

        //Key Len
        0x00,0x00,0x00,0x0b, //11
        //Value len
        0x00,0x00,0x00,0x05, //5

        //Key
        0x76,0x6f,0x6c,0x75,
        0x6d,0x65,0x2d,0x75,
        0x75,0x69,0x64,
        0x00, //Null terminator
        //Value
        0x74,0x65,0x73,0x74,0x00,
        //Key len
        0x00,0x00,0x00,0x12, //18
        //Value len
        0x00,0x00,0x00,0x04, //4
        //Key
        0x64,0x65,0x66,0x61,
        0x75,0x6c,0x74,0x2d,
        0x73,0x6f,0x66,0x74,
        0x2d,0x6c,0x69,0x6d,
        0x69,0x74,
        0x00, //Null Terminator
        //Value
        0x38,0x30,0x25,0x00,

        //Key len
        0x00,0x00,0x00,0x04, //4
        //Value len
        0x00,0x00,0x00,0x02, //2
        //Key
        0x74,0x79,0x70,0x65,
        0x00,
        //Value
        0x35,0x00,
        0x00,0x00,0x00];
        //let map = deserialize(&mut test_data);
        //println!("Hashmap: {:?}", map);

        let xid = 1;
        let prog = 29852134;
        let vers = 1;

        let verf = super::GlusterAuth{
            flavor: 0,
            stuff: "".to_string(),
        };
        let verf_bytes = verf.pack();
        let cred_flavor = 390039;
        let creds = super::pack_gluster_v2_cred(cred_flavor);

        let mut call_bytes = super::pack_quota_callheader(
            xid, prog, vers, super::GlusterAggregatorCommand::GlusterAggregatorGetlimit, creds, verf_bytes);

        let peer_request = super::GlusterCliRequest{
                dict: "gfid\000000000-0000-0000-0000-000000000001".to_string()
                //type
                //volume-uuid=<volume_name>
                //version=1.20000005
        };

        let addr = Path::new("/var/run/gluster/quotad.socket");
        println!("Connecting to /var/run/gluster/quotad.socket");
        let mut sock = UnixStream::connect(&addr).unwrap();

        let result = super::sendrecord(&mut sock, &call_bytes);
        println!("Result: {:?}", result);
        let mut reply_bytes = super::recvrecord(&mut sock).unwrap();
        println!("Reply bytes: ");
        super::print_fragment(&reply_bytes);
    }

    #[test]
    fn serialize_dictionary_test(){
        let mut hm: HashMap<String,String> = HashMap::new();

        hm.insert("gfid".to_string(), "00000000-0000-0000-0000-000000000001".to_string());
        hm.insert("volume-uuid".to_string(), "test".to_string());
        hm.insert("default-soft-limit".to_string(), "80%".to_string());
        hm.insert("type".to_string(), "5".to_string());

        let mut bytes = super::serialize_dict(&hm);
        let result_map = super::deserialize_dict(&mut bytes).unwrap();

        assert_eq!(hm, result_map);
    }
}

pub trait Pack{
    fn pack(&self) -> Vec<u8>;
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
    pub dict: HashMap<String,String> //Should these both be strings?
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
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliRequest, super::GlusterError>{
        let size = try!(data.read_u32::<BigEndian>());
        let dict = try!(unpack_string(data, size));

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
    pub dict: HashMap<String,String>
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
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliResponse, super::GlusterError>{
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
pub struct GlusterCliPeerListRequest{
    pub flags: i32,
    pub dict: HashMap<String,String>
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
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliPeerListRequest, super::GlusterError>{
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
pub struct GlusterCliPeerListResponse{
    pub op_ret: i32,
    pub op_errno: i32,
    pub friends: HashMap<String,String>,
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
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliPeerListResponse, super::GlusterError>{
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
pub struct GlusterCliFsmLogRequest{
    pub name: String,
}

impl Pack for GlusterCliFsmLogRequest{
    fn pack(&self)->Vec<u8>{
        let mut wr = XdrWriter::new();
        wr.pack(self.name.clone());
        return wr.into_buffer();
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
    fn pack(&self)->Vec<u8>{
        let mut wr = XdrWriter::new();
        wr.pack(self.unused);
        return wr.into_buffer();
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
    fn pack(&self)->Vec<u8>{
        let mut wr = XdrWriter::new();
        wr.pack(self.op_ret);
        wr.pack(self.op_errno);
        wr.pack(self.wd.clone());
        return wr.into_buffer();
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
    pub dict: String,
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
    fn unpack<T: Read>(data: &mut T)->Result<GlusterCliMountRequest, super::GlusterError>{
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
pub struct GlusterCliMountResponse{
    pub op_ret: i32,
    pub op_errno: i32,
    pub path: String,
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
    fn pack(&self)->Vec<u8>{
        let mut wr = XdrWriter::new();
        wr.pack(self.lazy);
        wr.pack(self.path.clone());
        return wr.into_buffer();
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
    fn pack(&self)->Vec<u8>{
        let mut wr = XdrWriter::new();
        wr.pack(self.op_ret);
        wr.pack(self.op_errno);
        return wr.into_buffer();
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
    pub flavor: i32,
    pub stuff: String
}

#[derive(Debug)]
pub struct GlusterCred{
    pub flavor: i32,
}

impl Pack for GlusterAuth{
    fn pack(&self)->Vec<u8>{
        let mut wr = XdrWriter::new();
        wr.pack(self.flavor);
        wr.pack(self.stuff.clone());
        return wr.into_buffer();
    }
}

pub fn pack_gluster_v2_cred(flavor: i32)->Vec<u8>{
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
pub fn pack_quota_callheader(xid: u32, prog: i32, vers: u32, proc_num: GlusterAggregatorCommand,
    cred_flavor: Vec<u8>, verf: Vec<u8>)->Vec<u8>{
        return pack_callheader(xid, prog, vers, proc_num as u32, cred_flavor, verf);
}

pub fn pack_cli_callheader(xid: u32, prog: i32, vers: u32, proc_num: GlusterCliCommand,
    cred_flavor: Vec<u8>, verf: Vec<u8>)->Vec<u8>{
        return pack_callheader(xid, prog, vers, proc_num as u32, cred_flavor, verf);
}

fn pack_callheader(xid: u32, prog: i32, vers: u32, proc_num: u32,
    cred_flavor: Vec<u8>, verf: Vec<u8>)->Vec<u8>{
    let mut wr = XdrWriter::new();

    wr.pack(xid);
    wr.pack(CALL);
    wr.pack(RPC_VERSION);
    wr.pack(prog); // 1238463
    wr.pack(vers); // 2
    wr.pack(proc_num); // 3
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
pub fn unpack_replyheader<T: Read>(data: &mut T)->Result<(u32, GlusterAuth), String>{
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

pub fn send_fragment<T: Write>(socket: &mut T, last: bool, fragment: &Vec<u8>)->Result<usize,String>{
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

pub fn sendrecord(sock: &mut UnixStream, record: &Vec<u8>)->Result<usize,String>{
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
pub fn recv_fragment<T: Read>(socket: &mut T)-> Result<(bool, Vec<u8>), String>{
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

pub fn recvrecord(sock: &mut UnixStream)->Result<Vec<u8>, String>{
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

fn unpack_key<T: Read>(data: &mut T, size: u32)->Result<String,String>{
   return unpack_value(data, size, true);
}

fn unpack_value<T: Read>(data: &mut T, size: u32, skip_null: bool)->Result<String,String>{
    let mut buffer: Vec<u8> = Vec::with_capacity(size as usize);
    for _ in 0..size {
        let b = try!(data.read_u8().map_err(|e| e.to_string()));
        buffer.push(b);
    }
    if skip_null{
        try!(data.read_u8().map_err(|e| e.to_string()));
    }
    let s = try!(String::from_utf8(buffer).map_err(|e| e.to_string()));
    return Ok(s.trim_matches('\0').to_string());
}

/*
Serialization format:
 -------- --------  --------  ----------- -------------
|  count | key len | val len | key     \0| value
 ---------------------------------------- -------------
 4        4         4       <key len>   <value len>
 //NOTE keys are NULL terminated but not value's
*/
pub fn serialize_dict(dict: &HashMap<String,String>)->Vec<u8>{
    let mut buffer: Vec<u8> = Vec::new();
    buffer.write_u32::<BigEndian>(dict.len() as u32).unwrap();

    for (key, value) in dict.iter(){
        buffer.write_u32::<BigEndian>(key.len() as u32).unwrap();
        buffer.write_u32::<BigEndian>(value.len() as u32).unwrap();
        for b in key.clone().into_bytes(){
            buffer.write_u8(b);
        }
        //Null terminate key
        buffer.write_u8(0x00);

        for b in value.clone().into_bytes(){
            buffer.write_u8(b);
        }
    }
    return buffer;
}

pub fn deserialize_dict(dict: &mut Vec<u8>)->Result<HashMap<String,String>, String>{
    let mut cursor = Cursor::new(&mut dict[..]);
    let count = cursor.read_u32::<BigEndian>().unwrap();
    let mut map = HashMap::with_capacity(count as usize);
    for _ in 0..count{

        let key_len = cursor.read_u32::<BigEndian>().unwrap();

        let value_len = cursor.read_u32::<BigEndian>().unwrap();

        let key = match unpack_key(&mut cursor, key_len){
            Ok(s) => s,
            Err(e) => {
                return Err(format!("Unable to unpack string: {}", e));
            },
        };
        let value = match unpack_value(&mut cursor, value_len, false){
            Ok(s) => s,
            Err(e) => {
                return Err(format!("Unable to unpack string: {}", e));
            },
        };
        map.insert(key, value);
    }
    return Ok(map);
}
