extern crate regex;
extern crate uuid;
use regex::Regex;
use uuid::Uuid;
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

/*
    Python will call rust with ctypes to do the hard work
*/
#[derive(Hash, PartialEq, Eq)]
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

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Peer {
   pub uuid: Uuid,
   pub hostname: String,
   pub status: State,
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

/*
#[cfg(test)]
fn fn run_command(command: &str, arg_list: Vec<String>, as_root: bool, script_mode: bool) -> std::process::Output {
    // mocked body, constructing a dummy http://doc.rust-lang.org/std/process/struct.Output.html from its public fields
}
*/

/*
#[cfg(not(test))]
*/
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
        let output = cmd.output().unwrap_or_else(|e| { panic!("failed to execute process: {} ", e)});
        return output;
    }
}

//List all peers including localhost
pub extern fn peer_list() ->Vec<Peer>{
    let mut peers: Vec<Peer> = Vec::new();
    let mut arg_list: Vec<String>  = Vec::new();
    arg_list.push("pool".to_string());
    arg_list.push("list".to_string());

    let output = run_command("gluster", &arg_list, true, false);
    let output_str = String::from_utf8(output.stdout).unwrap();

    for line in output_str.lines(){
        if line.contains("State"){
            continue;
        }else{
            let v: Vec<&str> = line.split('\t').collect();
            let uuid = Uuid::parse_str(v[0]).unwrap();
            let peer = Peer{
                uuid: uuid,
                hostname: v[1].to_string(),
                status: State::new(v[2]),
            };
            peers.push(peer);
        }
    }
    return peers;
}

//Probe a peer and prevent double probing
pub fn peer_probe(hostname: &str)->Result<i32,String>{
    let current_peers = peer_list();
    for peer in current_peers{
        if peer.hostname == *hostname{
            //Bail instead of double probing
            return Err(format!("hostname: {} is already part of the cluster", hostname));
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
        None => Err(String::from_utf8(output.stderr).unwrap()),
    }
}

pub fn peer_remove(hostname: &str, force: bool)->Result<i32, String>{
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
        None => Err(String::from_utf8(output.stderr).unwrap()),
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
        let num: u64 = value.trim_right_matches("PB").parse::<u64>().unwrap();
        return Some(num * 1024 * 1024 * 1024 * 1024 * 1024);
    }else if value.ends_with("TB"){
        let num: u64 = value.trim_right_matches("TB").parse::<u64>().unwrap();
        return Some(num * 1024 * 1024 * 1024 * 1024);
    }else if value.ends_with("GB"){
        let num: u64 = value.trim_right_matches("GB").parse::<u64>().unwrap();
        return Some(num * 1024 * 1024 * 1024);
    } else if value.ends_with("MB"){
        let num: u64 = value.trim_right_matches("MB").parse::<u64>().unwrap();
        return Some(num * 1024 * 1024);
    }else if value.ends_with("KB"){
        let num: u64 = value.trim_right_matches("KB").parse::<u64>().unwrap();
        return Some(num * 1024);
    }else if value.ends_with("Bytes"){
        let num: u64 = value.trim_right_matches("Bytes").parse::<u64>().unwrap();
        return Some(num);
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
        return None;
    }
    let output_str = String::from_utf8(output.stdout).unwrap();

    //Variables we will return in a struct
    let mut transport_type = String::new();
    let mut volume_type = String::new();
    let mut name = String::new();
    let mut status = String::new();
    let mut bricks: Vec<Brick> = Vec::new();
    let mut id = Uuid::nil();

    if output_str.trim() == "No volumes present"{
        return None;
    }

    if output_str.trim() == format!("Volume {} does not exist", volume){
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
            id = Uuid::parse_str(&x).unwrap();
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
            let re = match Regex::new(r"Brick\d+") {
                Ok(re) => re,
                Err(err) => panic!("{}", err),
            };
            if re.is_match(line){
                let brick_str = split_and_return_field(1, line.to_string());
                let brick_parts: Vec<&str> = brick_str.split(":").collect();
                assert!(brick_parts.len() == 2, "Failed to parse bricks from gluster vol info");
                let peer = Peer{
                    uuid: Uuid::new_v4(),
                    hostname: brick_parts[0].to_string(),
                    status: State::Unknown,
                };
                let brick = Brick{
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
  root@chris-ThinkPad-T410s:~# gluster vol quota test list
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

    let output_str = String::from_utf8(output.stdout).unwrap();

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
            let limit = translate_to_bytes(parts[1]).unwrap();
            let used = translate_to_bytes(parts[3]).unwrap();
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

pub fn volume_enable_quotas(volume: &str)->Result<i32, String>{
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("gluster".to_string());
    arg_list.push("volume".to_string());
    arg_list.push("quota".to_string());
    arg_list.push(volume.to_string());
    arg_list.push("enable".to_string());

    let output = run_command("gluster", &arg_list, true, false);
    let status = output.status;

    match status.code(){
        Some(v) => Ok(v),
        None => Err(String::from_utf8(output.stderr).unwrap()),
    }
}

pub fn volume_disable_quotas(volume: &str)->Result<i32, String>{
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("gluster".to_string());
    arg_list.push("volume".to_string());
    arg_list.push("quota".to_string());
    arg_list.push(volume.to_string());
    arg_list.push("disable".to_string());

    let output = run_command("gluster", &arg_list, true, false);
    let status = output.status;

    match status.code(){
        Some(v) => Ok(v),
        None => Err(String::from_utf8(output.stderr).unwrap()),
    }
}

pub fn volume_add_quota(
    volume: &str,
    path: PathBuf,
    size: u64)->Result<i32,String>{

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

    match status.code(){
        Some(v) => Ok(v),
        None => Err(String::from_utf8(output.stderr).unwrap()),
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

//volume add-brick <VOLNAME> [<stripe|replica> <COUNT>]
//<NEW-BRICK> ... [force] - add brick to volume <VOLNAME>
pub fn volume_add_brick(volume: &str,
    bricks: Vec<Brick>,
    force: bool) -> Result<i32,String> {

    if bricks.is_empty(){
        return Err("The brick list is empty. Not expanding volume".to_string());
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

    match status.code(){
        Some(v) => Ok(v),
        None => Err(String::from_utf8(output.stderr).unwrap()),
    }
}

pub fn volume_start(volume: &str, force: bool) -> Result<i32, String>{
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
    match status.code(){
        Some(v) => Ok(v),
        None => Err(String::from_utf8(output.stderr).unwrap()),
    }
}

pub fn volume_stop(volume: &str, force: bool) -> Result<i32, String>{
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("stop".to_string());
    arg_list.push(volume.to_string());

    if force {
        arg_list.push("force".to_string());
    }
    let output = run_command("gluster", &arg_list, true, true);
    let status = output.status;
    match status.code(){
        Some(v) => Ok(v),
        None => Err(String::from_utf8(output.stderr).unwrap()),
    }
}

pub fn volume_delete(volume: &str) -> Result<i32, String>{
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("delete".to_string());
    arg_list.push(volume.to_string());

    let output = run_command("gluster", &arg_list, true, true);
    let status = output.status;
    match status.code(){
        Some(v) => Ok(v),
        None => Err(String::from_utf8(output.stderr).unwrap()),
    }
}

pub fn volume_rebalance(volume: &str){
    //Usage: volume rebalance <VOLNAME> {{fix-layout start} | {start [force]|stop|status}}
}

fn volume_create<T: ToString>(volume: &str,
    options: HashMap<VolumeTranslator,T>,
    transport: &Transport,
    bricks: Vec<Brick>,
    force: bool) ->Result<i32, String>{

    if bricks.is_empty(){
        return Err("The brick list is empty. Not creating volume".to_string());
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
    match status.code(){
        Some(v) => Ok(v),
        None => Err(String::from_utf8(output.stderr).unwrap()),
    }
}


pub fn volume_create_replicated(volume: &str,
    replica_count: usize,
    transport: Transport,
    bricks: Vec<Brick>,
    force: bool) ->Result<i32, String>{

    let mut volume_translators: HashMap<VolumeTranslator, String> = HashMap::new();
    volume_translators.insert(VolumeTranslator::Replica, replica_count.to_string());

    return volume_create(volume, volume_translators, &transport, bricks, force);
}

pub fn volume_create_striped(volume: &str,
    stripe: usize,
    transport: Transport,
    bricks: Vec<Brick>,
    force: bool)->Result<i32, String>{

    let mut volume_translators: HashMap<VolumeTranslator, String> = HashMap::new();
    volume_translators.insert(VolumeTranslator::Stripe, stripe.to_string());

    return volume_create(volume, volume_translators, &transport, bricks, force);
}

pub fn volume_create_striped_replicated(volume: &str,
    stripe: usize,
    replica: usize,
    transport: Transport,
    bricks: Vec<Brick>,
    force: bool)->Result<i32, String>{

    let mut volume_translators: HashMap<VolumeTranslator, String> = HashMap::new();
    volume_translators.insert(VolumeTranslator::Stripe, stripe.to_string());
    volume_translators.insert(VolumeTranslator::Replica, replica.to_string());

    return volume_create(volume, volume_translators, &transport, bricks, force);
}

pub fn volume_create_distributed(volume: &str,
    transport: Transport,
    bricks: Vec<Brick>,
    force: bool)->Result<i32, String>{

    let volume_translators: HashMap<VolumeTranslator, String> = HashMap::new();

    return volume_create(volume, volume_translators, &transport, bricks, force);

}

pub fn volume_create_erasure(volume: &str,
    disperse: usize,
    redundancy: usize,
    transport: Transport,
    bricks: Vec<Brick>,
    force: bool)->Result<i32, String>{

    let mut volume_translators: HashMap<VolumeTranslator, String> = HashMap::new();
    volume_translators.insert(VolumeTranslator::Disperse, disperse.to_string());
    volume_translators.insert(VolumeTranslator::Redundancy, redundancy.to_string());

    return volume_create(volume, volume_translators, &transport, bricks, force);

}
//TODO: add functions for other vol types
