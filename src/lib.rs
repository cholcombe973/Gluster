extern crate regex;
extern crate uuid;
#[macro_use]
extern crate log;
use regex::Regex;
use uuid::Uuid;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::io;
use std::path::PathBuf;

//Custom error handling for the library
#[derive(Debug)]
pub enum GlusterError{
    IoError(io::Error),
    FromUtf8Error(std::string::FromUtf8Error),
    ParseError(uuid::ParseError),
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
        }
    }
}

impl fmt::Display for GlusterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            GlusterError::IoError(ref err) => err.fmt(f),
            GlusterError::FromUtf8Error(ref err) => err.fmt(f),
            GlusterError::ParseError(ref err) => err.fmt(f),
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
}

impl State {
    pub fn new(name: &str)->State{
        match name.trim() {
            "Connected" => State::Connected,
            "Disconnected" => State::Disconnected,
            _ => State::Unknown,
        }
    }
    pub fn to_string(self) -> String {
        match self {
            State::Connected => "Connected".to_string(),
            State::Disconnected => "Disconnected".to_string(),
            State::Unknown => "Unknown".to_string(),
        }
    }
}
#[derive(Debug)]
pub struct Quota{
    pub path: PathBuf,
    pub limit: u64,
    pub used: u64,
}

#[derive(Clone)]
pub struct Peer {
   pub uuid: Uuid,
   pub hostname: String,
   pub status: State,
}
impl fmt::Debug for Peer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UUID: {} Hostname: {} Status: {}", self.uuid.to_hyphenated_string(), self.hostname, self.status.to_string())
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
        match name.trim() {
            "Distribute" => VolumeType::Distribute,
            "Stripe" => VolumeType::Stripe,
            "Replicate" => VolumeType::Replicate,
            "Striped-Replicate" => VolumeType::StripedAndReplicate,
            "Disperse" => VolumeType::Disperse,
            //"Tier" => VolumeType::Tier, //TODO: Waiting for this to become stable
            "Distributed-Stripe" => VolumeType::DistributedAndStripe,
            "Distributed-Replicate" => VolumeType::DistributedAndReplicate,
            "Distributed-Striped-Replicate" => VolumeType::DistributedAndStripedAndReplicate,
            "Distributed-Disperse" => VolumeType::DistributedAndDisperse,
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
        match name.trim() {
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

//TODO: figure out a better way to do this.  This seems hacky
pub fn get_local_ip()->Result<String, GlusterError>{//net::Ipv4Addr>{
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

    return Ok(local_ip[0].trim().to_string());
}

pub fn get_peer_by_hostname(hostname: &str) ->Result<Peer, GlusterError>{
    let peer_list = try!(peer_list());
    let local_ip = try!(get_local_ip());

    for peer in peer_list{
        if peer.hostname == "localhost" {
            println!("Peer name == localhost");
            //Check if we own the ip address
            if hostname == local_ip{
                println!("We own that local ip: {}", local_ip);
                debug!("Found peer: {:?}", peer);
                let mut peer_clone = peer.clone();
                //Swap out the IP.  hostname = "localhost" messes up down stream consumers
                peer_clone.hostname = local_ip;
                return Ok(peer_clone);
            }
        }
        if peer.hostname == hostname {
            debug!("Found peer: {:?}", peer);
            return Ok(peer.clone());
        }
    }
    return Err(GlusterError::new(format!("Unable to find peer by hostname: {}", hostname)));
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
            let peer = Peer{
                uuid: uuid,
                hostname: v[1].trim().to_string(),
                status: State::new(v[2]),
            };
            peers.push(peer);
        }
    }
    return Ok(peers);
}

//Probe a peer and prevent double probing
pub fn peer_probe(hostname: &str)->Result<i32, GlusterError>{
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

    let output = run_command("gluster", &arg_list, true, false);
    let status = output.status;

    match status.code(){
        Some(v) => Ok(v),
        None => Err(GlusterError::new(try!(String::from_utf8(output.stderr)))),
    }
}

pub fn peer_remove(hostname: &str, force: bool)->Result<i32, GlusterError>{
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("peer".to_string());
    arg_list.push("detach".to_string());
    arg_list.push(hostname.to_string());

    if force{
        arg_list.push("force".to_string());
    }

    let output = run_command("gluster", &arg_list, true, false);
    let status = output.status;

    match status.code(){
        Some(v) => Ok(v),
        None => Err(GlusterError::new(try!(String::from_utf8(output.stderr)))),
    }
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

pub fn volume_info(volume: &str) -> Option<Volume> {
    let mut arg_list: Vec<String>  = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("info".to_string());
    arg_list.push(volume.to_string());
    let output = run_command("gluster", &arg_list, true, false);
    let status = output.status;

    if !status.success(){
        debug!("Volume info get command failed");
        println!("Volume info get command failed");
        return None;
    }
    let output_str:String = match String::from_utf8(output.stdout){
        Ok(n) => n,
        Err(_) => {
            debug!("string matching failed");
            println!("string matching failed");
            return None
        },
    };

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
        return None;
    }

    if output_str.trim() == format!("Volume {} does not exist", volume){
        debug!("Volume {} does not exist", volume);
        println!("Volume {} does not exist", volume);
        return None;
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
            id = match Uuid::parse_str(&x){
                Ok(m) => m,
                //TODO: I'm ignoring this error for now
                Err(_) => Uuid::nil(),
            };
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

                let peer: Peer = match get_peer_by_hostname(brick_parts[0].trim()){
                    Ok(p) => p,
                    //TODO: could we insert a blank peer here?
                    Err(_) => {
                        debug!("Failed to get peer by hostname: {}", brick_parts[0]);
                        println!("Failed to get peer by hostname: {}", brick_parts[0]);
                        return None
                    },
                };
                debug!("get_peer_by_hostname result: Peer: {:?}", peer);
                println!("get_peer_by_hostname result: Peer: {:?}", peer);
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
    return Some(vol_info);
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
    arg_list.push("gluster".to_string());
    arg_list.push("volume".to_string());
    arg_list.push("quota".to_string());
    arg_list.push(volume.to_string());
    arg_list.push("enable".to_string());

    let output = run_command("gluster", &arg_list, true, false);
    let status = output.status;
    match status.success(){
        true => Ok(0),
        false => Err(
            GlusterError::new(
                try!(String::from_utf8(output.stderr))
                )
            ),
    }
}

pub fn volume_disable_quotas(volume: &str)->Result<i32, GlusterError>{
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("gluster".to_string());
    arg_list.push("volume".to_string());
    arg_list.push("quota".to_string());
    arg_list.push(volume.to_string());
    arg_list.push("disable".to_string());

    let output = run_command("gluster", &arg_list, true, false);
    let status = output.status;
    match status.success(){
        true => Ok(0),
        false => Err(GlusterError::new(try!(String::from_utf8(output.stderr)))),
    }
}

pub fn volume_add_quota(
    volume: &str,
    path: PathBuf,
    size: u64)->Result<i32,GlusterError>{

    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("gluster".to_string());
    arg_list.push("volume".to_string());
    arg_list.push("quota".to_string());
    arg_list.push(volume.to_string());
    arg_list.push("limit-usage".to_string());
    arg_list.push(path.to_str().unwrap().to_string());
    arg_list.push(size.to_string());

    let output = run_command("gluster", &arg_list, true, false);
    let status = output.status;
    match status.success(){
        true => Ok(0),
        false => Err(GlusterError::new(try!(String::from_utf8(output.stderr)))),
    }
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

        let output = run_command("gluster", &arg_list, true, true);
        let status = output.status;
        match status.success(){
            true => Ok(0),
            false => Err(GlusterError::new(try!(String::from_utf8(output.stderr)))),
        }
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
    let output = run_command("gluster", &arg_list, true, true);
    let status = output.status;
    match status.success(){
        true => Ok(0),
        false => Err(GlusterError::new(try!(String::from_utf8(output.stderr)))),
    }
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
    let output = run_command("gluster", &arg_list, true, true);
    let status = output.status;
    match status.success(){
        true => Ok(0),
        false => Err(GlusterError::new(try!(String::from_utf8(output.stderr)))),
    }
}

pub fn volume_stop(volume: &str, force: bool) -> Result<i32, GlusterError>{
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("stop".to_string());
    arg_list.push(volume.to_string());

    if force {
        arg_list.push("force".to_string());
    }
    let output = run_command("gluster", &arg_list, true, true);
    let status = output.status;
    match status.success(){
        true => Ok(0),
        false => Err(GlusterError::new(try!(String::from_utf8(output.stderr)))),
    }
}

pub fn volume_delete(volume: &str) -> Result<i32, GlusterError>{
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("delete".to_string());
    arg_list.push(volume.to_string());

    let output = run_command("gluster", &arg_list, true, true);
    let status = output.status;
    match status.success(){
        true => Ok(0),
        false => Err(GlusterError::new(try!(String::from_utf8(output.stderr)))),
    }
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
    let output = run_command("gluster", &arg_list, true, true);

    let status = output.status;
    match status.success(){
        true => Ok(0),
        false => Err(GlusterError::new(try!(String::from_utf8(output.stderr)))),
    }
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
