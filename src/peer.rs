
use std::ascii::AsciiExt;
use std::cmp::Ord;
use std::cmp::Ordering;
use std::fmt;
use std::net::IpAddr;

use regex::Regex;
use super::{GlusterError, process_output, resolve_to_ip, run_command};
use uuid::Uuid;

#[test]
fn test_parse_peer_status() {
    let test_result =
        vec![Peer {
                 uuid: Uuid::parse_str("afbd338e-881b-4557-8764-52e259885ca3").unwrap(),
                 hostname: "10.0.3.207".to_string(),
                 status: State::PeerInCluster,
             },
             Peer {
                 uuid: Uuid::parse_str("fa3b031a-c4ef-43c5-892d-4b909bc5cd5d").unwrap(),
                 hostname: "10.0.3.208".to_string(),
                 status: State::PeerInCluster,
             },
             Peer {
                 uuid: Uuid::parse_str("5f45e89a-23c1-41dd-b0cd-fd9cf37f1520").unwrap(),
                 hostname: "10.0.3.209".to_string(),
                 status: State::PeerInCluster,
             }];
    let test_line = r#"Number of Peers: 3 Hostname: 10.0.3.207
Uuid: afbd338e-881b-4557-8764-52e259885ca3 State: Peer in Cluster (Connected)
Hostname: 10.0.3.208 Uuid: fa3b031a-c4ef-43c5-892d-4b909bc5cd5d
State: Peer in Cluster (Connected) Hostname: 10.0.3.209
Uuid: 5f45e89a-23c1-41dd-b0cd-fd9cf37f1520 State: Peer in Cluster (Connected)"#
        .to_string();

    // Expect a 3 peer result
    let result = parse_peer_status(&test_line);
    println!("Result: {:?}", result);
    assert!(result.is_ok());

    let result_unwrapped = result.unwrap();
    assert_eq!(test_result, result_unwrapped);
}

/// A enum representing the possible States that a Peer can be in
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
    /// Create a new State object from a &str
    pub fn new(name: &str) -> State {
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
    /// Return a string representation of the State
    pub fn to_string(self) -> String {
        match self {
            State::Connected => "Connected".to_string(),
            State::Disconnected => "Disconnected".to_string(),
            State::Unknown => "Unknown".to_string(),
            State::EstablishingConnection => "establishing connection".to_string(),
            State::ProbeSentToPeer => "probe sent to peer".to_string(),
            State::ProbeReceivedFromPeer => "probe received from peer".to_string(),
            State::PeerInCluster => "peer in cluster".to_string(),
            State::AcceptedPeerRequest => "accepted peer request".to_string(),
            State::SentAndReceivedPeerRequest => "sent and received peer request".to_string(),
            State::PeerRejected => "peer rejected".to_string(),
            State::PeerDetachInProgress => "peer detach in progress".to_string(),
            State::ConnectedToPeer => "connected to peer".to_string(),
            State::PeerIsConnectedAndAccepted => "peer is connected and accepted".to_string(),
            State::InvalidState => "invalid state".to_string(),
        }
    }
}

/// A Gluster Peer.  A Peer is roughly equivalent to a server in Gluster.
#[derive(Clone, Eq, PartialEq)]
pub struct Peer {
    /// The unique identifer of this peer
    pub uuid: Uuid,
    // TODO: Lets stay with ip addresses.
    /// The hostname or IP address of the peer
    pub hostname: String,
    ///  The current State of the peer
    pub status: State,
}

impl Ord for Peer {
    fn cmp(&self, other: &Peer) -> Ordering {
        self.uuid.cmp(&other.uuid)
    }
}
impl PartialOrd for Peer {
    fn partial_cmp(&self, other: &Peer) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for Peer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "UUID: {} Hostname: {} Status: {}",
               self.uuid.hyphenated().to_string(),
               self.hostname,
               self.status.to_string())
    }
}

/// This will query the Gluster peer list and return a Peer struct for the peer
/// # Failures
/// Returns GlusterError if the peer could not be found
pub fn get_peer(hostname: &String) -> Result<Peer, GlusterError> {
    // Return dummy data if we're testing
    if cfg!(test) {
        return Ok(Peer {
            uuid: Uuid::parse_str("78f68270-201a-4d8a-bad3-7cded6e6b7d8").unwrap(),
            hostname: "test_ip".to_string(),
            status: State::Connected,
        });
    }
    let peer_list = try!(peer_list());

    for peer in peer_list {
        if peer.hostname == *hostname {
            debug!("Found peer: {:?}", peer);
            return Ok(peer.clone());
        }
    }
    return Err(GlusterError::new(format!("Unable to find peer by hostname: {}", hostname)));
}

fn parse_peer_status(line: &String) -> Result<Vec<Peer>, GlusterError> {
    let mut peers: Vec<Peer> = Vec::new();

    // TODO: It's either this or some kinda crazy looping or batching
    let regex_str = r#"Hostname:\s+(?P<hostname>[a-zA-Z0-9.]+)\s+
Uuid:\s+(?P<uuid>\w+-\w+-\w+-\w+-\w+)\s+
State:\s+(?P<state_detail>[a-zA-z ]+)\s+\((?P<state>\w+)\)"#;
    let peer_regex = try!(Regex::new(&regex_str.replace("\n", "")));
    for cap in peer_regex.captures_iter(line) {
        let hostname = try!(cap.name("hostname")
            .ok_or(GlusterError::new(format!("Invalid hostname for peer: {}", line))));

        let uuid = try!(cap.name("uuid")
            .ok_or(GlusterError::new(format!("Invalid uuid for peer: {}", line))));
        let uuid_parsed = try!(Uuid::parse_str(uuid));
        let state_details = try!(cap.name("state_detail")
            .ok_or(GlusterError::new(format!("Invalid state for peer: {}", line))));

        // Translate back into an IP address if needed
        let check_for_ip = hostname.parse::<IpAddr>();

        if check_for_ip.is_err() {
            // It's a hostname so lets resolve it
            match resolve_to_ip(&hostname) {
                Ok(ip_addr) => {
                    peers.push(Peer {
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
        } else {
            // It's an IP address so lets use it
            peers.push(Peer {
                uuid: uuid_parsed,
                hostname: hostname.to_string(),
                status: State::new(state_details),
            });
        }
    }
    return Ok(peers);
}

/// Runs gluster peer status and returns a Vec<Peer> representing all the peers
/// in the cluster
/// # Failures
/// Returns GlusterError if the command failed to run
pub fn peer_status() -> Result<Vec<Peer>, GlusterError> {
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("peer".to_string());
    arg_list.push("status".to_string());

    let output = run_command("gluster", &arg_list, true, false);
    let output_str = try!(String::from_utf8(output.stdout));
    // Number of Peers: 1
    // Hostname: 10.0.3.207
    // Uuid: afbd338e-881b-4557-8764-52e259885ca3
    // State: Peer in Cluster (Connected)
    //

    return parse_peer_status(&output_str);
}

// List all peers including localhost
/// Runs gluster pool list and returns a Vec<Peer> representing all the peers
/// in the cluster
/// This also returns information for the localhost as a Peer.  peer_status()
/// does not
/// # Failures
/// Returns GlusterError if the command failed to run
pub fn peer_list() -> Result<Vec<Peer>, GlusterError> {
    let mut peers: Vec<Peer> = Vec::new();
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("pool".to_string());
    arg_list.push("list".to_string());

    let output = run_command("gluster", &arg_list, true, false);
    let output_str = try!(String::from_utf8(output.stdout));

    for line in output_str.lines() {
        if line.contains("State") {
            continue;
        } else {
            let v: Vec<&str> = line.split('\t').collect();
            let uuid = try!(Uuid::parse_str(v[0]));
            let mut hostname = v[1].trim().to_string();

            // Translate back into an IP address if needed
            let check_for_ip = hostname.parse::<IpAddr>();

            if check_for_ip.is_err() {
                // It's a hostname so lets resolve it
                hostname = match resolve_to_ip(&hostname) {
                    Ok(ip_addr) => ip_addr,
                    Err(e) => {
                        return Err(GlusterError::new(e.to_string()));
                    }
                };
            }
            debug!("hostname from peer list command is {:?}", &hostname);

            peers.push(Peer {
                uuid: uuid,
                hostname: hostname,
                status: State::new(v[2]),
            });
        }
    }
    return Ok(peers);
}

// Probe a peer and prevent double probing
/// Adds a new peer to the cluster by hostname or ip address
/// # Failures
/// Returns GlusterError if the command failed to run
pub fn peer_probe(hostname: &String) -> Result<i32, GlusterError> {
    let current_peers = try!(peer_list());
    for peer in current_peers {
        if peer.hostname == *hostname {
            // Bail instead of double probing
            // return Err(format!("hostname: {} is already part of the cluster", hostname));
            return Ok(0); //Does it make sense to say this is ok?
        }
    }
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("peer".to_string());
    arg_list.push("probe".to_string());
    arg_list.push(hostname.to_string());

    return process_output(run_command("gluster", &arg_list, true, false));
}

/// Removes a peer from the cluster by hostname or ip address
/// # Failures
/// Returns GlusterError if the command failed to run
pub fn peer_remove(hostname: &String, force: bool) -> Result<i32, GlusterError> {
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("peer".to_string());
    arg_list.push("detach".to_string());
    arg_list.push(hostname.to_string());

    if force {
        arg_list.push("force".to_string());
    }

    return process_output(run_command("gluster", &arg_list, true, false));
}
