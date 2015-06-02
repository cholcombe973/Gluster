extern crate regex;
extern crate uuid;
use regex::Regex;
use uuid::Uuid;
use std::fmt;
use std::path::PathBuf;

/*
    Python will call rust with ctypes to do the hard work
*/
pub struct Brick {
    pub peer: Peer,
    pub path: PathBuf,
}

impl Brick{
    pub fn as_str(&self) -> String{
        self.peer.hostname.clone() + ":" + self.path.to_str().unwrap()
    }
}

impl fmt::Debug for Brick {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}:{:?}", self.peer.hostname, self.path.to_str())
    }
}

#[derive(Debug, Copy, Clone)]
pub enum State {
    Connected,
    Disconnected,
}

impl State {
    fn new(name: &str)->State{
        match name.trim() {
            "Connected" => State::Connected,
            "Disconnected" => State::Disconnected,
            _ => State::Disconnected,
        }
    }
    fn to_string(self) -> String {
        match self {
            State::Connected => "Connected".to_string(),
            State::Disconnected => "Disconnected".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Peer {
   pub uuid: Uuid,
   pub hostname: String,
   pub status: State,
}

#[derive(Debug)]
pub enum Transport {
    Tcp,
    Rdma,
    TcpAndRdma,
}

#[derive(Debug)]
pub enum VolumeType {
    Distribute,
    Stripe,
    Replicate,
    StripedAndReplicate,
    Disperse,
    Tier,
    DistributedAndStripe,
    DistributedAndReplicate,
    DistributedAndStripedAndReplicate,
    DistributedAndDisperse,
}

impl VolumeType {
    fn new(name: &str)->VolumeType{
        match name.trim() {
            "Distribute" => VolumeType::Distribute,
            "Stripe" => VolumeType::Stripe,
            "Replicate" => VolumeType::Replicate,
            "Striped-Replicate" => VolumeType::StripedAndReplicate,
            "Disperse" => VolumeType::Disperse,
            "Tier" => VolumeType::Tier,
            "Distributed-Stripe" => VolumeType::DistributedAndStripe,
            "Distributed-Replicate" => VolumeType::DistributedAndReplicate,
            "Distributed-Striped-Replicate" => VolumeType::DistributedAndStripedAndReplicate,
            "Distributed-Disperse" => VolumeType::DistributedAndDisperse,
            _ => VolumeType::Replicate,
        }
    }
    fn to_string(self) -> String {
        match self {
            VolumeType::Distribute => "Distribute".to_string(),
            VolumeType::Stripe => "Stripe".to_string(),
            VolumeType::Replicate => "Replicate".to_string(),
            VolumeType::StripedAndReplicate => "Striped-Replicate".to_string(),
            VolumeType::Disperse => "Disperse".to_string(),
            VolumeType::Tier => "Tier".to_string(),
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
    pub bricks: Box<Vec<String>>, //TODO: Change me to struct Brick
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
fn run_command(command: &str, arg_list: Vec<String>, as_root: bool, script_mode: bool) -> std::process::Output{
    if as_root{
        let mut cmd = std::process::Command::new("sudo");
        cmd.arg(command);
        if script_mode{
            cmd.arg("--mode=script");
        }
        for arg in arg_list{
            cmd.arg(&arg);
        }
        println!("Running command: {:?}", cmd);
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
        println!("Running command: {:?}", cmd);
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

    let output = run_command("gluster", arg_list, true, false);
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
pub fn peer_probe(hostname: &String){
    let current_peers = peer_list();
    for peer in current_peers{
        if peer.hostname == *hostname{
            println!("hostname: {} is already part of the cluster", hostname);
            //Bail instead of double probing
            return;
        }
    }
    let mut arg_list: Vec<String>  = Vec::new();
    arg_list.push("peer".to_string());
    arg_list.push("probe".to_string());
    arg_list.push(hostname.clone());

    let output = run_command("gluster", arg_list, true, false);
    let status = output.status;

    if ! status.success(){
        println!("gluster peer probe failed");
    }
}

pub fn peer_remove(hostname: String){
    let status = std::process::Command::new("sudo")
        .arg("gluster")
        .arg("peer")
        .arg("detach")
        .arg(&hostname)
        .status().unwrap_or_else(|e| { panic!("failed to execute process: {} ", e)});
    if ! status.success(){
        println!("gluster peer removal failed. Trying harder");
        let force = std::process::Command::new("sudo")
            .arg("gluster")
            .arg("peer")
            .arg("detach")
            .arg(&hostname)
            .arg("force")
            .status().unwrap_or_else(|e| { panic!("failed to execute process: {} ", e)});
        if ! force.success(){
            println!("gluster peer removal with force failed. Giving up");
        }
    }else{
        println!("gluster removed peer {}", hostname);
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

pub fn volume_info(volume: &String) -> Option<Volume> {
    let mut arg_list: Vec<String>  = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("info".to_string());
    arg_list.push(volume.clone());
    let output = run_command("gluster", arg_list, true, false);
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
    let mut bricks: Box<Vec<String>> =  Box::new(Vec::new());
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
                let brick = split_and_return_field(1, line.to_string());
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

//volume add-brick <VOLNAME> [<stripe|replica> <COUNT>]
//<NEW-BRICK> ... [force] - add brick to volume <VOLNAME>
fn volume_add_brick_replicated(volume: String,
    replica_count: usize,
    bricks: Vec<Brick>,
    force: bool){

    if bricks.is_empty(){
        println!("The brick list is empty.  Not creating volume");
        //TODO: change function to use Result<T, E>
        return;
    }
    let replica_count_str = replica_count.to_string();

    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("add-brick".to_string());
    arg_list.push(volume);
    arg_list.push("replica".to_string());
    arg_list.push(replica_count_str);

    for brick in bricks.iter(){
        arg_list.push(brick.as_str());
    }
    if force{
        arg_list.push("force".to_string());
    }
    let status = run_command("gluster", arg_list, true, true).status;
    if ! status.success(){
        println!("gluster volume add-brick failed.");
    }
}

pub fn volume_start(volume: String, force: bool){
    //Should I check the volume exists first?
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("start".to_string());
    arg_list.push(volume);

    if force {
        arg_list.push("force".to_string());
    }
    let status = run_command("gluster", arg_list, true, true).status;
    if ! status.success(){
        println!("gluster volume start failed.");
    }
}

pub fn volume_stop(volume: String, force: bool){
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("stop".to_string());
    arg_list.push(volume);

    if force {
        arg_list.push("force".to_string());
    }
    let status = run_command("gluster", arg_list, true, true).status;
    if ! status.success(){
        println!("gluster volume stop failed.");
    }
}

pub fn volume_delete(volume: String){
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("delete".to_string());
    arg_list.push(volume);

    let status = run_command("gluster", arg_list, true, true).status;
    if ! status.success(){
        println!("gluster volume delete failed.");
    }
}

pub fn volume_rebalance(volume: String){

}
/*
    volume create <NEW-VOLNAME> [stripe <COUNT>] [replica <COUNT>]
    [disperse [<COUNT>]] [redundancy <COUNT>] [transport <tcp|rdma|tcp,rdma>]
    <NEW-BRICK>?<vg_name>... [force]
*/
pub fn volume_create_replicated(volume: String,
    replica_count: usize,
    transport: Transport,
    bricks: Vec<Brick>,
    force: bool){

    if bricks.is_empty(){
        println!("The brick list is empty.  Not creating volume");
        //TODO: change function to use Result<T, E>
        return;
    }

    if (bricks.len() % replica_count) != 0 {
        println!("The brick list and replica count do not match.  Not creating volume");
        //TODO: change function to use Result<T, E>
        return;
    }

    let replica_count_str = replica_count.to_string();

    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("create".to_string());
    arg_list.push(volume);
    arg_list.push("replica".to_string());
    arg_list.push(replica_count_str);
    arg_list.push("transport".to_string());
    arg_list.push(transport.to_string());

    for brick in bricks.iter(){
        arg_list.push(brick.as_str());
    }
    if force{
        arg_list.push("force".to_string());
    }
    let output = run_command("gluster", arg_list, true, true);

    if ! output.status.success(){
        println!("gluster volume create failed.");
        println!("Error: {:?}", output.stderr);
    }
}

pub fn volume_create_erasure(volume: String,
    disperse: usize,
    redundancy: usize,
    transport: Transport,
    bricks: Vec<Brick>,
    force: bool){

    if bricks.is_empty(){
        println!("The brick list is empty.  Not creating volume");
        //TODO: change function to use Result<T, E>
        return;
    }

    if (bricks.len() % disperse) != 0 {
        println!("The brick list and disperse count do not match.  Not creating volume");
        //TODO: change function to use Result<T, E>
        return;
    }

    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("create".to_string());
    arg_list.push(volume);
    arg_list.push("disperse".to_string());
    arg_list.push(disperse.to_string());
    arg_list.push("redundancy".to_string());
    arg_list.push(redundancy.to_string());
    arg_list.push("transport".to_string());
    arg_list.push(transport.to_string());

    for brick in bricks.iter(){
        arg_list.push(brick.as_str());
    }
    if force{
        arg_list.push("force".to_string());
    }
    let status = run_command("gluster", arg_list, true, true).status;
    if ! status.success(){
        println!("gluster volume create failed.");
    }
}
//TODO: add functions for other vol types
