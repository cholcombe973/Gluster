//! This is a library to interface with
//! [Gluster](https://gluster.readthedocs.org/en/latest/)
//!
//! Most of the commands below are wrappers around the CLI functionality.
//! However recently I have
//! reverse engineered some of the Gluster RPC protocol so that calls can be
//! made directly against
//! the Gluster server.  This method of communication is much faster than
//! shelling out.
//!
//! Scale testing with this library has been done to about 60 servers
//! successfully.
//!
//! Please file any bugs found at: [Gluster
//! Repo](https://github.com/cholcombe973/Gluster)
//! Pull requests are more than welcome!
mod rpc;
extern crate byteorder;
extern crate regex;
extern crate unix_socket;
extern crate uuid;

#[macro_use]
extern crate log;

use byteorder::{BigEndian, ReadBytesExt};
use regex::Regex;
use uuid::Uuid;

use std::ascii::AsciiExt;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io;
use std::io::Cursor;
use std::io::prelude::*;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::path::Path;
use std::str::FromStr;
use unix_socket::UnixStream;

use rpc::Pack;
use rpc::UnPack;

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    #[test]
    fn test_parse_peer_status() {
        let test_result =
            vec![super::Peer {
                     uuid: Uuid::parse_str("afbd338e-881b-4557-8764-52e259885ca3").unwrap(),
                     hostname: "10.0.3.207".to_string(),
                     status: super::State::PeerInCluster,
                 },
                 super::Peer {
                     uuid: Uuid::parse_str("fa3b031a-c4ef-43c5-892d-4b909bc5cd5d").unwrap(),
                     hostname: "10.0.3.208".to_string(),
                     status: super::State::PeerInCluster,
                 },
                 super::Peer {
                     uuid: Uuid::parse_str("5f45e89a-23c1-41dd-b0cd-fd9cf37f1520").unwrap(),
                     hostname: "10.0.3.209".to_string(),
                     status: super::State::PeerInCluster,
                 }];
        let test_line = r#"Number of Peers: 3 Hostname: 10.0.3.207
Uuid: afbd338e-881b-4557-8764-52e259885ca3 State: Peer in Cluster (Connected)
Hostname: 10.0.3.208 Uuid: fa3b031a-c4ef-43c5-892d-4b909bc5cd5d
State: Peer in Cluster (Connected) Hostname: 10.0.3.209
Uuid: 5f45e89a-23c1-41dd-b0cd-fd9cf37f1520 State: Peer in Cluster (Connected)"#
            .to_string();

        // Expect a 3 peer result
        let result = super::parse_peer_status(&test_line);
        println!("Result: {:?}", result);
        assert!(result.is_ok());

        let result_unwrapped = result.unwrap();
        assert_eq!(test_result, result_unwrapped);
    }


}

pub enum SelfHealAlgorithm {
    Full,
    Diff,
    Reset,
}

impl SelfHealAlgorithm {
    fn to_string(&self) -> String {
        match self {
            &SelfHealAlgorithm::Full => "full".to_string(),
            &SelfHealAlgorithm::Diff => "diff".to_string(),
            &SelfHealAlgorithm::Reset => "reset".to_string(),
        }
    }
    fn from_str(s: &str) -> SelfHealAlgorithm {
        match s {
            "full" => SelfHealAlgorithm::Full,
            "diff" => SelfHealAlgorithm::Diff,
            "reset" => SelfHealAlgorithm::Reset,
            _ => SelfHealAlgorithm::Full,
        }
    }
}

pub enum AccessMode {
    ReadOnly,
    ReadWrite,
}

impl AccessMode {
    fn to_string(&self) -> String {
        match self {
            &AccessMode::ReadOnly => "read-only".to_string(),
            &AccessMode::ReadWrite => "read-write".to_string(),
        }
    }
    fn from_str(s: &str) -> AccessMode {
        match s {
            "read-only" => AccessMode::ReadOnly,
            "read-write" => AccessMode::ReadWrite,
            _ => AccessMode::ReadWrite,
        }
    }
}

impl<'a> Into<&'a str> for AccessMode {
    fn into(self) -> &'a str {
        match self {
            AccessMode::ReadOnly => "read-only",
            AccessMode::ReadWrite => "read-write",
        }
    }
}

pub enum Toggle {
    On,
    Off,
}

impl Into<bool> for Toggle {
    fn into(self) -> bool {
        match self {
            Toggle::On => true,
            Toggle::Off => false,
        }
    }
}

impl Toggle {
    fn to_string(&self) -> String {
        match self {
            &Toggle::On => "On".to_string(),
            &Toggle::Off => "Off".to_string(),
        }
    }
    fn from_str(s: &str) -> Toggle {
        match s {
            "on" => Toggle::On,
            "off" => Toggle::Off,
            "true" => Toggle::On,
            "false" => Toggle::Off,
            _ => Toggle::Off,
        }
    }
}

pub enum GlusterOption {
    /// Valid IP address which includes wild card patterns including *, such as 192.168.1.*
    AuthAllow(String),
    /// Valid IP address which includes wild card patterns including *, such as 192.168.2.*
    AuthReject(String),
    /// Specifies the duration for the lock state to be maintained on the client after a
    /// network disconnection in seconds
    /// Range: 10-1800
    ClientGraceTimeout(i64),
    /// Specifies the maximum number of blocks per file on which self-heal would happen
    /// simultaneously.
    /// Range: 0-1025
    ClusterSelfHealWindowSize(u16),
    /// Specifies the type of self-heal. If you set the option as "full", the entire file is
    /// copied from source to destinations. If the option is set to "diff" the file blocks
    /// that are not in sync are copied to destinations.
    ClusterDataSelfHealAlgorithm(SelfHealAlgorithm),
    /// Percentage of required minimum free disk space
    ClusterMinFreeDisk(u8),
    /// Specifies the size of the stripe unit that will be read from or written to in bytes
    ClusterStripeBlockSize(u64),
    /// Allows you to turn-off proactive self-heal on replicated
    ClusterSelfHealDaemon(Toggle),
    /// This option makes sure the data/metadata is durable across abrupt shutdown of the brick.
    ClusterEnsureDurability(Toggle),
    /// The log-level of the bricks.
    DiagnosticsBrickLogLevel(log::LogLevel),
    /// The log-level of the clients.
    DiagnosticsClientLogLevel(log::LogLevel),
    /// Statistics related to the latency of each operation would be tracked.
    DiagnosticsLatencyMeasurement(Toggle),
    /// Statistics related to file-operations would be tracked.
    DiagnosticsDumpFdStats(Toggle),
    /// Enables you to mount the entire volume as read-only for all the clients
    /// (including NFS clients) accessing it.
    FeaturesReadOnly(Toggle),
    /// Enables self-healing of locks when the network disconnects.
    FeaturesLockHeal(Toggle),
    /// For performance reasons, quota caches the directory sizes on client. You can set timeout
    /// indicating the maximum duration of directory sizes in cache, from the time they are
    /// populated, during which they are considered valid
    FeaturesQuotaTimeout(u16),
    /// Automatically sync the changes in the filesystem from Master to Slave.
    GeoReplicationIndexing(Toggle),
    /// The time frame after which the operation has to be declared as dead, if the server does
    /// not respond for a particular operation.
    NetworkFrameTimeout(u16),
    /// For 32-bit nfs clients or applications that do not support 64-bit inode numbers or large
    /// files, use this option from the CLI to make Gluster NFS return 32-bit inode numbers
    /// instead of 64-bit inode numbers.
    NfsEnableIno32(Toggle),
    /// Set the access type for the specified sub-volume.
    NfsVolumeAccess(AccessMode),
    /// If there is an UNSTABLE write from the client, STABLE flag will be returned to force the
    /// client to not send a COMMIT request. In some environments, combined with a replicated
    /// GlusterFS setup, this option can improve write performance. This flag allows users to
    /// trust Gluster replication logic to sync data to the disks and recover when required.
    /// COMMIT requests if received will be handled in a default manner by fsyncing. STABLE writes
    /// are still handled in a sync manner.
    NfsTrustedWrite(Toggle),
    /// All writes and COMMIT requests are treated as async. This implies that no write requests
    /// are guaranteed to be on server disks when the write reply is received at the NFS client.
    /// Trusted sync includes trusted-write behavior.
    NfsTrustedSync(Toggle),
    /// This option can be used to export specified comma separated subdirectories in the volume.
    /// The path must be an absolute path. Along with path allowed list of IPs/hostname can be
    /// associated with each subdirectory. If provided connection will allowed only from these IPs.
    /// Format: \<dir>[(hostspec[hostspec...])][,...]. Where hostspec can be an IP address,
    /// hostname or an IP range in CIDR notation. Note: Care must be taken while configuring
    /// this option as invalid entries and/or unreachable DNS servers can introduce unwanted
    /// delay in all the mount calls.
    NfsExportDir(String),
    /// Enable/Disable exporting entire volumes, instead if used in conjunction with
    /// nfs3.export-dir, can allow setting up only subdirectories as exports.
    NfsExportVolumes(Toggle),
    /// Enable/Disable the AUTH_UNIX authentication type. This option is enabled by default for
    /// better interoperability. However, you can disable it if required.
    NfsRpcAuthUnix(Toggle),
    /// Enable/Disable the AUTH_NULL authentication type. It is not recommended to change
    /// the default value for this option.
    NfsRpcAuthNull(Toggle),
    /// Allow client connections from unprivileged ports. By default only privileged ports are
    // allowed. This is a global setting in case insecure ports are to be enabled for all
    /// exports using a single option.
    NfsPortsInsecure(Toggle),
    /// Turn-off name lookup for incoming client connections using this option. In some setups,
    /// the name server can take too long to reply to DNS queries resulting in timeouts of mount
    /// requests. Use this option to turn off name lookups during address authentication. Note,
    NfsAddrNamelookup(Toggle),
    /// For systems that need to run multiple NFS servers, you need to prevent more than one from
    /// registering with portmap service. Use this option to turn off portmap registration for
    /// Gluster NFS.
    NfsRegisterWithPortmap(Toggle),
    /// Turn-off volume being exported by NFS
    NfsDisable(Toggle),
    /// Size of the per-file write-behind buffer.Size of the per-file write-behind buffer.
    PerformanceWriteBehindWindowSize(u64),
    /// The number of threads in IO threads translator.
    PerformanceIoThreadCount(u8),
    /// If this option is set ON, instructs write-behind translator to perform flush in
    /// background, by returning success (or any errors, if any of previous writes were failed)
    /// to application even before flush is sent to backend filesystem.
    PerformanceFlushBehind(Toggle),
    /// Sets the maximum file size cached by the io-cache translator. Can use the normal size
    /// descriptors of KB, MB, GB,TB or PB (for example, 6GB). Maximum size u64.
    PerformanceCacheMaxFileSize(u64),
    /// Sets the minimum file size cached by the io-cache translator. Values same as "max" above
    PerformanceCacheMinFileSize(u64),
    /// The cached data for a file will be retained till 'cache-refresh-timeout' seconds,
    /// after which data re-validation is performed.
    PerformanceCacheRefreshTimeout(u8),
    /// Size of the read cache in bytes
    PerformanceCacheSize(u64),
    /// Allow client connections from unprivileged ports. By default only privileged ports are
    /// allowed. This is a global setting in case insecure ports are to be enabled for all
    /// exports using a single option.
    ServerAllowInsecure(Toggle),
    /// Specifies the duration for the lock state to be maintained on the server after a
    /// network disconnection.
    ServerGraceTimeout(u16),
    /// Location of the state dump file.
    ServerStatedumpPath(PathBuf),
    /// Number of seconds between health-checks done on the filesystem that is used for the
    /// brick(s). Defaults to 30 seconds, set to 0 to disable.
    StorageHealthCheckInterval(u16),
}

impl GlusterOption {
    fn to_string(&self) -> String {
        match self {
            &GlusterOption::AuthAllow(_) => "auth.allow".to_string(),
            &GlusterOption::AuthReject(_) => "auth.reject".to_string(),
            &GlusterOption::ClientGraceTimeout(_) => "client.grace-timeout".to_string(),
            &GlusterOption::ClusterSelfHealWindowSize(_) => {
                "cluster.self-heal-window-size".to_string()
            }
            &GlusterOption::ClusterDataSelfHealAlgorithm(_) => {
                "cluster.data-self-heal-algorithm".to_string()
            }
            &GlusterOption::ClusterMinFreeDisk(_) => "cluster.min-free-disk".to_string(),
            &GlusterOption::ClusterStripeBlockSize(_) => "cluster.stripe-block-size".to_string(),
            &GlusterOption::ClusterSelfHealDaemon(_) => "cluster.self-heal-daemon".to_string(),
            &GlusterOption::ClusterEnsureDurability(_) => "cluster.ensure-durability".to_string(),
            &GlusterOption::DiagnosticsBrickLogLevel(_) => {
                "diagnostics.brick-log-level".to_string()
            },
            &GlusterOption::DiagnosticsClientLogLevel(_) => {
                "diagnostics.client-log-level".to_string()
            }
            &GlusterOption::DiagnosticsLatencyMeasurement(_) => {
                "diagnostics.latency-measurement".to_string()
            }
            &GlusterOption::DiagnosticsDumpFdStats(_) => {
                "diagnostics.dump-fd-stats".to_string()
            }
            &GlusterOption::FeaturesReadOnly(_) => "features.read-only".to_string(),
            &GlusterOption::FeaturesLockHeal(_) => "features.lock-heal".to_string(),
            &GlusterOption::FeaturesQuotaTimeout(_) => "features.quota-timeout".to_string(),
            &GlusterOption::GeoReplicationIndexing(_) => "geo-replication.indexing".to_string(),
            &GlusterOption::NetworkFrameTimeout(_) => "network.frame-timeout".to_string(),
            &GlusterOption::NfsEnableIno32(_) => "nfs.enable-ino32".to_string(),
            &GlusterOption::NfsVolumeAccess(_) => "nfs.volume-access".to_string(),
            &GlusterOption::NfsTrustedWrite(_) => "nfs.trusted-write".to_string(),
            &GlusterOption::NfsTrustedSync(_) => "nfs.trusted-sync".to_string(),
            &GlusterOption::NfsExportDir(_) => "nfs.export-dir".to_string(),
            &GlusterOption::NfsExportVolumes(_) => "nfs.export-volumes".to_string(),
            &GlusterOption::NfsRpcAuthUnix(_) => "nfs.rpc-auth-unix".to_string(),
            &GlusterOption::NfsRpcAuthNull(_) => "nfs.rpc-auth-null".to_string(),
            &GlusterOption::NfsPortsInsecure(_) => "nfs.ports-insecure".to_string(),
            &GlusterOption::NfsAddrNamelookup(_) => "nfs.addr-namelookup".to_string(),
            &GlusterOption::NfsRegisterWithPortmap(_) => "nfs.register-with-portmap".to_string(),
            &GlusterOption::NfsDisable(_) => "nfs.disable".to_string(),
            &GlusterOption::PerformanceWriteBehindWindowSize(_) => {
                "performance.write-behind-window-size".to_string()
            }
            &GlusterOption::PerformanceIoThreadCount(_) => {
                "performance.io-thread-count".to_string()
            }
            &GlusterOption::PerformanceFlushBehind(_) => "performance.flush-behind".to_string(),
            &GlusterOption::PerformanceCacheMaxFileSize(_) => {
                "performance.cache-max-file-size".to_string()
            }
            &GlusterOption::PerformanceCacheMinFileSize(_) => {
                "performance.cache-min-file-size".to_string()
            }
            &GlusterOption::PerformanceCacheRefreshTimeout(_) => {
                "performance.cache-refresh-timeout".to_string()
            }
            &GlusterOption::PerformanceCacheSize(_) => "performance.cache-size".to_string(),
            &GlusterOption::ServerAllowInsecure(_) => "server.allow-insecure".to_string(),
            &GlusterOption::ServerGraceTimeout(_) => "server.grace-timeout".to_string(),
            &GlusterOption::ServerStatedumpPath(_) => "server.statedump-path".to_string(),
            &GlusterOption::StorageHealthCheckInterval(_) => {
                "storage.health-check-interval".to_string()
            }
        }
    }
    fn value(&self) -> String {
        match self {
            &GlusterOption::AuthAllow(ref val) => val.to_string(),
            &GlusterOption::AuthReject(ref val) => val.to_string(),
            &GlusterOption::ClientGraceTimeout(val) => val.to_string(),
            &GlusterOption::ClusterSelfHealWindowSize(val) => val.to_string(),
            &GlusterOption::ClusterDataSelfHealAlgorithm(ref val) => val.to_string(),
            &GlusterOption::ClusterMinFreeDisk(val) => val.to_string(),
            &GlusterOption::ClusterStripeBlockSize(val) => val.to_string(),
            &GlusterOption::ClusterSelfHealDaemon(ref val) => val.to_string(),
            &GlusterOption::ClusterEnsureDurability(ref val) => val.to_string(),
            &GlusterOption::DiagnosticsBrickLogLevel(val) => val.to_string(),
            &GlusterOption::DiagnosticsClientLogLevel(val) => val.to_string(),
            &GlusterOption::DiagnosticsLatencyMeasurement(ref val) => val.to_string(),
            &GlusterOption::DiagnosticsDumpFdStats(ref val) => val.to_string(),
            &GlusterOption::FeaturesReadOnly(ref val) => val.to_string(),
            &GlusterOption::FeaturesLockHeal(ref val) => val.to_string(),
            &GlusterOption::FeaturesQuotaTimeout(val) => val.to_string(),
            &GlusterOption::GeoReplicationIndexing(ref val) => val.to_string(),
            &GlusterOption::NetworkFrameTimeout(val) => val.to_string(),
            &GlusterOption::NfsEnableIno32(ref val) => val.to_string(),
            &GlusterOption::NfsVolumeAccess(ref val) => val.to_string(),
            &GlusterOption::NfsTrustedWrite(ref val) => val.to_string(),
            &GlusterOption::NfsTrustedSync(ref val) => val.to_string(),
            &GlusterOption::NfsExportDir(ref val) => val.to_string(),
            &GlusterOption::NfsExportVolumes(ref val) => val.to_string(),
            &GlusterOption::NfsRpcAuthUnix(ref val) => val.to_string(),
            &GlusterOption::NfsRpcAuthNull(ref val) => val.to_string(),
            &GlusterOption::NfsPortsInsecure(ref val) => val.to_string(),
            &GlusterOption::NfsAddrNamelookup(ref val) => val.to_string(),
            &GlusterOption::NfsRegisterWithPortmap(ref val) => val.to_string(),
            &GlusterOption::NfsDisable(ref val) => val.to_string(),
            &GlusterOption::PerformanceWriteBehindWindowSize(val) => val.to_string(),
            &GlusterOption::PerformanceIoThreadCount(val) => val.to_string(),
            &GlusterOption::PerformanceFlushBehind(ref val) => val.to_string(),
            &GlusterOption::PerformanceCacheMaxFileSize(val) => val.to_string(),
            &GlusterOption::PerformanceCacheMinFileSize(val) => val.to_string(),
            &GlusterOption::PerformanceCacheRefreshTimeout(val) => val.to_string(),
            &GlusterOption::PerformanceCacheSize(val) => val.to_string(),
            &GlusterOption::ServerAllowInsecure(ref val) => val.to_string(),
            &GlusterOption::ServerGraceTimeout(val) => val.to_string(),
            &GlusterOption::ServerStatedumpPath(ref val) => val.to_string_lossy().into_owned(),
            &GlusterOption::StorageHealthCheckInterval(val) => val.to_string(),
        }
    }
    pub fn from_str(s: &str, value: String) -> Result<GlusterOption, GlusterError> {
        match s {
            "auth-allow" => {
                return Ok(GlusterOption::AuthAllow(value));
            }
            "auth-reject" => {
                return Ok(GlusterOption::AuthReject(value));
            }
            "client-grace-timeout" => {
                let i = try!(i64::from_str(&value));
                return Ok(GlusterOption::ClientGraceTimeout(i));
            }
            "cluster-self-heal-window-size" => {
                let i = try!(u16::from_str(&value));
                return Ok(GlusterOption::ClusterSelfHealWindowSize(i));
            }
            "cluster-data-self-heal-algorithm" => {
                let s = SelfHealAlgorithm::from_str(&value);
                return Ok(GlusterOption::ClusterDataSelfHealAlgorithm(s));
            }
            "cluster-min-free-disk" => {
                let i = try!(u8::from_str(&value));
                return Ok(GlusterOption::ClusterMinFreeDisk(i));
            }
            "cluster-stripe-block-size" => {
                let i = try!(u64::from_str(&value));
                return Ok(GlusterOption::ClusterStripeBlockSize(i));
            }
            "cluster-self-heal-daemon" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::ClusterSelfHealDaemon(t));
            }
            "cluster-ensure-durability" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::ClusterEnsureDurability(t));
            }
            "diagnostics-brick-log-level" => {
                let l = log::LogLevel::from_str(&value).unwrap_or(log::LogLevel::Debug);
                return Ok(GlusterOption::DiagnosticsBrickLogLevel(l));
            }
            "diagnostics-client-log-level" => {
                let l = log::LogLevel::from_str(&value).unwrap_or(log::LogLevel::Debug);
                return Ok(GlusterOption::DiagnosticsClientLogLevel(l));
            }
            "diagnostics-latency-measurement" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::DiagnosticsLatencyMeasurement(t));
            }
            "diagnostics-dump-fd-stats" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::DiagnosticsDumpFdStats(t));
            }
            "features-read-only" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::FeaturesReadOnly(t));
            }
            "features-lock-heal" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::FeaturesLockHeal(t));
            }
            "features-quota-timeout" => {
                let i = try!(u16::from_str(&value));
                return Ok(GlusterOption::FeaturesQuotaTimeout(i));
            }
            "geo-replication-indexing" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::GeoReplicationIndexing(t));
            }
            "network-frame-timeout" => {
                let i = try!(u16::from_str(&value));
                return Ok(GlusterOption::NetworkFrameTimeout(i));
            }
            "nfs-enable-ino32" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::NfsEnableIno32(t));
            }
            "nfs-volume-access" => {
                let s = AccessMode::from_str(&value);
                return Ok(GlusterOption::NfsVolumeAccess(s));
            }
            "nfs-trusted-write" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::NfsTrustedWrite(t));
            }
            "nfs-trusted-sync" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::NfsTrustedSync(t));
            }
            "nfs-export-dir" => {
                return Ok(GlusterOption::NfsExportDir(value));
            }
            "nfs-export-volumes" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::NfsExportVolumes(t));
            }
            "nfs-rpc-auth-unix" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::NfsRpcAuthUnix(t));
            }
            "nfs-rpc-auth-null" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::NfsRpcAuthNull(t));
            }
            "nfs-ports-insecure" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::NfsPortsInsecure(t));
            }
            "nfs-addr-namelookup" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::NfsAddrNamelookup(t));
            }
            "nfs-register-with-portmap" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::NfsRegisterWithPortmap(t));
            }
            "nfs-disable" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::NfsDisable(t));
            }
            "performance-write-behind-window-size" => {
                let i = try!(u64::from_str(&value));
                return Ok(GlusterOption::PerformanceWriteBehindWindowSize(i));
            }
            "performance-io-thread-count" => {
                let i = try!(u8::from_str(&value));
                return Ok(GlusterOption::PerformanceIoThreadCount(i));
            }
            "performance-flush-behind" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::PerformanceFlushBehind(t));
            }
            "-performance-cache-max-file-size" => {
                let i = try!(u64::from_str(&value));
                return Ok(GlusterOption::PerformanceCacheMaxFileSize(i));
            }
            "-performance-cache-min-file-size" => {
                let i = try!(u64::from_str(&value));
                return Ok(GlusterOption::PerformanceCacheMinFileSize(i));
            }
            "performance-cache-refresh-timeout" => {
                let i = try!(u8::from_str(&value));
                return Ok(GlusterOption::PerformanceCacheRefreshTimeout(i));
            }
            "performance-cache-size" => {
                let i = try!(u64::from_str(&value));
                return Ok(GlusterOption::PerformanceCacheSize(i));
            }
            "server-allow-insecure" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::ServerAllowInsecure(t));
            }
            "server-grace-timeout" => {
                let i = try!(u16::from_str(&value));
                return Ok(GlusterOption::ServerGraceTimeout(i));
            }
            "server-statedump-path" => {
                let p = PathBuf::from(&value);
                return Ok(GlusterOption::ServerStatedumpPath(p));
            }
            "storage-health-check-interval" => {
                let i = try!(u16::from_str(&value));
                return Ok(GlusterOption::StorageHealthCheckInterval(i));
            }
            _ => {
                return Err(GlusterError::new(format!("Unknown option: {}", s)));
            }
        }
    }
}

/// Custom error handling for the library
#[derive(Debug)]
pub enum GlusterError {
    IoError(io::Error),
    FromUtf8Error(std::string::FromUtf8Error),
    ParseError(uuid::ParseError),
    AddrParseError(String),
    ParseIntError(std::num::ParseIntError),
    ParseBoolErr(std::str::ParseBoolError),
    ByteOrder(byteorder::Error),
    RegexError(regex::Error),
    NoVolumesPresent,
}

impl GlusterError {
    /// Create a new GlusterError with a String message
    fn new(err: String) -> GlusterError {
        GlusterError::IoError(io::Error::new(std::io::ErrorKind::Other, err))
    }

    /// Convert a GlusterError into a String representation.
    pub fn to_string(&self) -> String {
        match *self {
            GlusterError::IoError(ref err) => err.description().to_string(),
            GlusterError::FromUtf8Error(ref err) => err.description().to_string(),
            // TODO fix this
            GlusterError::ParseError(_) => "Parse error".to_string(),
            GlusterError::AddrParseError(_) => "IP Address parsing error".to_string(),
            GlusterError::ParseIntError(ref err) => err.description().to_string(),
            GlusterError::ParseBoolErr(ref err) => err.description().to_string(),
            GlusterError::ByteOrder(ref err) => err.description().to_string(),
            GlusterError::RegexError(ref err) => err.description().to_string(),
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

impl From<std::num::ParseIntError> for GlusterError {
    fn from(err: std::num::ParseIntError) -> GlusterError {
        GlusterError::ParseIntError(err)
    }
}

impl From<std::str::ParseBoolError> for GlusterError {
    fn from(err: std::str::ParseBoolError) -> GlusterError {
        GlusterError::ParseBoolErr(err)
    }
}

impl From<byteorder::Error> for GlusterError {
    fn from(err: byteorder::Error) -> GlusterError {
        GlusterError::ByteOrder(err)
    }
}

impl From<regex::Error> for GlusterError {
    fn from(err: regex::Error) -> GlusterError {
        GlusterError::RegexError(err)
    }
}

/// A Gluster Brick consists of a Peer and a path to the mount point
#[derive(Clone, Eq, PartialEq)]
pub struct Brick {
    pub peer: Peer,
    pub path: PathBuf,
}

impl Brick {
    /// Returns a String representation of the selected enum variant.
    pub fn to_string(&self) -> String {
        format!("{}:{}",
                self.peer.hostname.clone(),
                self.path.to_string_lossy())
    }
}

impl fmt::Debug for Brick {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}:{:?}", self.peer.hostname, self.path.to_str())
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct BrickStatus {
    brick: Brick,
    tcp_port: u16,
    rdma_port: u16,
    online: bool,
    pid: u16,
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

/// A Quota can be used set limits on the pool usage.  All limits are set in
/// bytes.
#[derive(Debug, Eq, PartialEq)]
pub struct Quota {
    pub path: PathBuf,
    pub limit: u64,
    pub used: u64,
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

impl fmt::Debug for Peer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "UUID: {} Hostname: {} Status: {}",
               self.uuid.hyphenated().to_string(),
               self.hostname,
               self.status.to_string())
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
    Stripe,
    Replica,
    Disperse,
    Redundancy,
}

impl VolumeTranslator {
    /// Returns a String representation of the selected enum variant.
    fn to_string(self) -> String {
        match self {
            VolumeTranslator::Stripe => "stripe".to_string(),
            VolumeTranslator::Replica => "replica".to_string(),
            VolumeTranslator::Disperse => "disperse".to_string(),
            VolumeTranslator::Redundancy => "redundancy".to_string(),
        }
    }
}

/// These are all the different Volume types that are possible in Gluster
/// Note: Tier is not represented here because I'm waiting for it to become
/// more stable
/// For more information about these types see: [Gluster Volume]
/// (https://gluster.readthedocs.
/// org/en/latest/Administrator%20Guide/Setting%20Up%20Volumes/)
#[derive(Debug, Eq, PartialEq)]
pub enum VolumeType {
    Distribute,
    Stripe,
    Replicate,
    StripedAndReplicate,
    Disperse,
    // Tier,
    DistributedAndStripe,
    DistributedAndReplicate,
    DistributedAndStripedAndReplicate,
    DistributedAndDisperse,
}

impl VolumeType {
    /// Constructs a new VolumeType from a &str
    pub fn new(name: &str) -> VolumeType {
        match name.trim().to_ascii_lowercase().as_ref() {
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

fn process_output(output: std::process::Output) -> Result<i32, GlusterError> {
    let status = output.status;

    if status.success() {
        return Ok(0);
    } else {
        return Err(GlusterError::new(try!(String::from_utf8(output.stderr))));
    }
}

// TODO: Change me to Result<std::process::Output, String>
fn run_command(command: &str,
               arg_list: &Vec<String>,
               as_root: bool,
               script_mode: bool)
               -> std::process::Output {
    if as_root {
        let mut cmd = std::process::Command::new("sudo");
        cmd.arg(command);
        if script_mode {
            cmd.arg("--mode=script");
        }
        for arg in arg_list {
            cmd.arg(&arg);
        }
        debug!("About to run command: {:?}", cmd);
        let output = cmd.output().unwrap_or_else(|e| panic!("failed to execute process: {} ", e));
        return output;
    } else {
        let mut cmd = std::process::Command::new(command);
        if script_mode {
            cmd.arg("--mode=script");
        }
        for arg in arg_list {
            cmd.arg(&arg);
        }
        debug!("About to run command: {:?}", cmd);
        let output = cmd.output().unwrap_or_else(|e| panic!("failed to execute process: {} ", e));
        return output;
    }
}

// TODO: figure out a better way to do this.  This seems hacky
/// Returns the local IPv4Addr address associated with this server
/// # Failures
/// Returns a GlusterError representing any failure that may have happened
/// while trying to
/// query this information.
pub fn get_local_ip() -> Result<Ipv4Addr, GlusterError> {
    let mut default_route: Vec<String> = Vec::new();
    default_route.push("route".to_string());
    default_route.push("show".to_string());
    default_route.push("0.0.0.0/0".to_string());

    let cmd_output = run_command("ip", &default_route, false, false);
    let default_route_stdout: String = try!(String::from_utf8(cmd_output.stdout));

    // default via 192.168.1.1 dev wlan0  proto static
    let addr_regex = try!(Regex::new(r"(?P<addr>via \S+)"));
    let default_route_parse = match addr_regex.captures(&default_route_stdout) {
        Some(a) => a,
        None => {
            return Err(GlusterError::new(format!("Unable to parse default route from: {}",
                                                 &default_route_stdout)));
        }
    };

    let addr_raw = match default_route_parse.name("addr") {
        Some(a) => a,
        None => {
            return Err(GlusterError::new(format!("Unable to find addr default route from: {}",
                                                 &default_route_stdout)));
        }
    };

    // Skip "via" in the capture
    let addr: Vec<&str> = addr_raw.split(" ").skip(1).collect();

    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("route".to_string());
    arg_list.push("get".to_string());
    arg_list.push(addr[0].to_string());

    let src_address_output = run_command("ip", &arg_list, false, false);
    // 192.168.1.1 dev wlan0  src 192.168.1.7
    let local_address_stdout = try!(String::from_utf8(src_address_output.stdout));
    let src_regex = try!(Regex::new(r"(?P<src>src \S+)"));
    let capture_output = match src_regex.captures(&local_address_stdout) {
        Some(a) => a,
        None => {
            return Err(GlusterError::new(format!("Unable to parse local_address from: {}",
                                                 &local_address_stdout)));
        }
    };

    let local_address_src = match capture_output.name("src") {
        Some(a) => a,
        None => {
            return Err(GlusterError::new(format!("Unable to parse src from: {}",
                                                 &local_address_stdout)));
        }
    };

    // Skip src in the capture
    let local_ip: Vec<&str> = local_address_src.split(" ").skip(1).collect();
    let ip_addr = try!(local_ip[0].trim().parse::<Ipv4Addr>());

    return Ok(ip_addr);
}

/// Resolves a &str hostname into a ip address.
pub fn resolve_to_ip(address: &str) -> Result<String, String> {
    // Return dummy data if we're testing
    if cfg!(test) {
        return Ok("test_ip".to_string());
    }

    if address == "localhost" {
        let local_ip = try!(get_local_ip().map_err(|e| e.to_string()));
        debug!("hostname is localhost.  Resolving to local ip {}",
               &local_ip.to_string());
        return Ok(local_ip.to_string());
    }

    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("+short".to_string());
    // arg_list.push("-x".to_string());
    arg_list.push(address.trim().to_string());
    let output = run_command("dig", &arg_list, false, false);

    let status = output.status;

    if status.success() {
        let output_str = try!(String::from_utf8(output.stdout).map_err(|e| e.to_string()));
        // Remove the trailing . and newline
        let trimmed = output_str.trim().trim_right_matches(".");
        return Ok(trimmed.to_string());
    } else {
        return Err(try!(String::from_utf8(output.stderr).map_err(|e| e.to_string())));
    }
}

/// A function to get the information from /etc/hostname
pub fn get_local_hostname() -> Result<String, GlusterError> {
    let mut f = try!(File::open("/etc/hostname"));
    let mut s = String::new();
    try!(f.read_to_string(&mut s));
    return Ok(s.trim().to_string());
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
        let check_for_ip = hostname.parse::<Ipv4Addr>();

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
            let check_for_ip = hostname.parse::<Ipv4Addr>();

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

// Note this will panic on failure to parse u64
/// This is a helper function to convert values such as 1PB into a bytes
/// # Examples
/// ```
/// extern crate gluster;
/// let bytes = gluster::translate_to_bytes("1GB").unwrap();
/// assert_eq!(bytes, 1073741824);
/// ```

pub fn translate_to_bytes(value: &str) -> Option<u64> {
    if value.ends_with("PB") {
        match value.trim_right_matches("PB").parse::<u64>() {
            Ok(n) => return Some(n * 1024 * 1024 * 1024 * 1024 * 1024),
            Err(_) => return None,
        };
    } else if value.ends_with("TB") {
        match value.trim_right_matches("TB").parse::<u64>() {
            Ok(n) => return Some(n * 1024 * 1024 * 1024 * 1024),
            Err(_) => return None,
        };
    } else if value.ends_with("GB") {
        match value.trim_right_matches("GB").parse::<u64>() {
            Ok(n) => return Some(n * 1024 * 1024 * 1024),
            Err(_) => return None,
        };
    } else if value.ends_with("MB") {
        match value.trim_right_matches("MB").parse::<u64>() {
            Ok(n) => return Some(n * 1024 * 1024),
            Err(_) => return None,
        };
    } else if value.ends_with("KB") {
        match value.trim_right_matches("KB").parse::<u64>() {
            Ok(n) => return Some(n * 1024),
            Err(_) => return None,
        };
    } else if value.ends_with("Bytes") {
        match value.trim_right_matches("Bytes").parse::<u64>() {
            Ok(n) => return Some(n),
            Err(_) => return None,
        };
    } else {
        return None;
    }
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
                     },
                     ],
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
        return Err(GlusterError::new(format!("Volume: {} does not exist", volume)));
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
                if name == "Number of Bricks" {

                }
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
                assert!(brick_parts.len() == 2,
                        "Failed to parse bricks from gluster vol info");

                let mut hostname = brick_parts[0].trim().to_string();

                // Translate back into an IP address if needed
                let check_for_ip = hostname.parse::<Ipv4Addr>();

                if check_for_ip.is_err() {
                    // It's a hostname so lets resolve it
                    hostname = match resolve_to_ip(&hostname) {
                        Ok(ip_addr) => ip_addr,
                        Err(e) => {
                            return Err(GlusterError::new(format!("Failed to resolve hostname: \
                                                                  {}. Error: {}",
                                                                 &hostname,
                                                                 e)));
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
        println!("Volume info get command failed with error: {}",
                 String::from_utf8_lossy(&output.stderr));

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

    let mut call_bytes = try!(rpc::pack_quota_callheader(xid,
                                        prog,
                                        vers,
                                        rpc::GlusterAggregatorCommand::GlusterAggregatorGetlimit,
                                        cred_bytes,
                                        verf_bytes));

    let mut dict: HashMap<String, Vec<u8>> = HashMap::with_capacity(4);

    // TODO: Make a Gluster wd RPC call and parse this from the quota.conf file
    // This is crap
    let mut gfid = "00000000-0000-0000-0000-000000000001".to_string().into_bytes();
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
            return Err(GlusterError::new("trusted.glusterfs.quota.size was not returned from \
                                          quotad"
                .to_string()));
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
        debug!("Volume quota list command failed with error: {}",
               String::from_utf8_lossy(&output.stderr));
        return Err(GlusterError::new(String::from_utf8_lossy(&output.stderr).into_owned()));
    }
    let output_str: String = try!(String::from_utf8(output.stdout));
    let quota_list = parse_quota_list(volume, output_str);

    Ok(quota_list)
}

#[test]
fn test_quota_list() {
    let test_data = r#"
    Path                   Hard-limit  Soft-limit      Used  Available  Soft-limit exceeded? Hard-limit exceeded?
-------------------------------------------------------------------------------------------------------------------------------
/                                          1.0KB     80%(819Bytes)   0Bytes   1.0KB              No                   No
"#;
    let result = parse_quota_list("test", test_data.to_string());
    let quotas = vec![Quota {
                          path: PathBuf::from("/"),
                          limit: 0,
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
        let parts: Vec<&str> = line.split(" ").filter(|s| !s.is_empty()).collect::<Vec<&str>>();
        // Output should match: ["/", "100.0MB", "80%", "0Bytes", "100.0MB", "No", "No"]
        if parts.len() > 3 {
            let limit = match translate_to_bytes(parts[1]) {
                Some(v) => v,
                // TODO:  is this sane?
                None => 0,
            };
            let used = match translate_to_bytes(parts[3]) {
                Some(v) => v,
                // TODO:  is this sane?
                None => 0,
            };
            let quota = Quota {
                path: PathBuf::from(parts[0].to_string()),
                limit: limit,
                used: used,
            };
            quota_list.push(quota);
        }
        // else?
    }
    return quota_list;
}

/// Enable quotas on the volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_enable_quotas(volume: &str) -> Result<i32, GlusterError> {
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("quota".to_string());
    arg_list.push(volume.to_string());
    arg_list.push("enable".to_string());

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
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("quota".to_string());
    arg_list.push(volume.to_string());
    arg_list.push("disable".to_string());

    return process_output(run_command("gluster", &arg_list, true, false));
}

/// Removes a size quota to the volume and path.
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_remove_quota(volume: &str, path: PathBuf) -> Result<i32, GlusterError> {

    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("quota".to_string());
    arg_list.push(volume.to_string());
    arg_list.push("remove".to_string());
    arg_list.push(path.to_string_lossy().to_string());

    return process_output(run_command("gluster", &arg_list, true, false));
}

/// Adds a size quota to the volume and path.
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_add_quota(volume: &str, path: PathBuf, size: u64) -> Result<i32, GlusterError> {

    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("quota".to_string());
    arg_list.push(volume.to_string());
    arg_list.push("limit-usage".to_string());
    arg_list.push(path.to_string_lossy().to_string());
    arg_list.push(size.to_string());

    return process_output(run_command("gluster", &arg_list, true, false));
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
        let regex_str = r#"Brick\s+(?P<hostname>[a-zA-Z0-9.])+
:(?P<path>[/a-zA-z0-9])+
\s+(?P<tcp>[0-9])+\s+(?P<rdma>[0-9])+\s+(?P<online>[Y,N])+\s+(?P<pid>[0-9])+"#;
        let brick_regex = try!(Regex::new(&regex_str.replace("\n", "")));
        match brick_regex.captures(&line) {
            Some(result) => {
                let tcp_port = match result.name("tcp") {
                    Some(port) => port,
                    None => {
                        return Err(GlusterError::new("Unable to find tcp port in gluster vol \
                                                      status output"
                            .to_string()));
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
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("vol".to_string());
    arg_list.push("status".to_string());
    arg_list.push(volume.to_string());

    let output = run_command("gluster", &arg_list, true, false);
    if !output.status.success() {
        let stderr = try!(String::from_utf8(output.stderr));
        return Err(GlusterError::new(stderr));
    }

    let output_str = try!(String::from_utf8(output.stdout));
    let bricks = try!(parse_volume_status(output_str));

    Ok(bricks)
}

/// Based on the replicas or erasure bits that are still available in the
/// volume this will return
/// True or False as to whether you can remove a Brick. This should be called
/// before volume_remove_brick()
pub fn ok_to_remove(volume: &str, brick: &Brick) -> Result<bool, GlusterError> {
    // TODO: switch over to native RPC call to eliminate String regex parsing
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("vol".to_string());
    arg_list.push("status".to_string());
    arg_list.push(volume.to_string());

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

/// This will remove a brick from the volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_remove_brick(volume: &str,
                           bricks: Vec<Brick>,
                           force: bool)
                           -> Result<i32, GlusterError> {

    if bricks.is_empty() {
        return Err(GlusterError::new("The brick list is empty. Not shrinking volume".to_string()));
    }

    for brick in bricks {
        let ok = try!(ok_to_remove(&volume, &brick));
        if ok {
            let mut arg_list: Vec<String> = Vec::new();
            arg_list.push("volume".to_string());
            arg_list.push("remove-brick".to_string());
            arg_list.push(volume.to_string());

            if force {
                arg_list.push("force".to_string());
            }
            arg_list.push("start".to_string());

            let status = process_output(run_command("gluster", &arg_list, true, true));
        } else {
            return Err(GlusterError::new("Unable to remove brick due to redundancy failure"
                .to_string()));
        }
    }
    return Ok(0);
}

// volume add-brick <VOLNAME> [<stripe|replica> <COUNT>]
// <NEW-BRICK> ... [force] - add brick to volume <VOLNAME>
/// This adds a new brick to the volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_add_brick(volume: &str,
                        bricks: Vec<Brick>,
                        force: bool)
                        -> Result<i32, GlusterError> {

    if bricks.is_empty() {
        return Err(GlusterError::new("The brick list is empty. Not expanding volume".to_string()));
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
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("start".to_string());
    arg_list.push(volume.to_string());

    if force {
        arg_list.push("force".to_string());
    }
    return process_output(run_command("gluster", &arg_list, true, true));
}

/// This stops a running volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_stop(volume: &str, force: bool) -> Result<i32, GlusterError> {
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("stop".to_string());
    arg_list.push(volume.to_string());

    if force {
        arg_list.push("force".to_string());
    }
    return process_output(run_command("gluster", &arg_list, true, true));
}

/// This deletes a stopped volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_delete(volume: &str) -> Result<i32, GlusterError> {
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push("volume".to_string());
    arg_list.push("delete".to_string());
    arg_list.push(volume.to_string());

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

fn volume_create<T: ToString>(volume: &str,
                              options: HashMap<VolumeTranslator, T>,
                              transport: &Transport,
                              bricks: Vec<Brick>,
                              force: bool)
                              -> Result<i32, GlusterError> {

    if bricks.is_empty() {
        return Err(GlusterError::new("The brick list is empty. Not creating volume".to_string()));
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
    let results: Vec<Result<i32, GlusterError>> =
        settings.iter().map(|gluster_opt| vol_set(volume, gluster_opt)).collect();

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
pub fn volume_create_replicated(volume: &str,
                                replica_count: usize,
                                transport: Transport,
                                bricks: Vec<Brick>,
                                force: bool)
                                -> Result<i32, GlusterError> {

    let mut volume_translators: HashMap<VolumeTranslator, String> = HashMap::new();
    volume_translators.insert(VolumeTranslator::Replica, replica_count.to_string());

    return volume_create(volume, volume_translators, &transport, bricks, force);
}

/// This creates a new striped volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_create_striped(volume: &str,
                             stripe: usize,
                             transport: Transport,
                             bricks: Vec<Brick>,
                             force: bool)
                             -> Result<i32, GlusterError> {

    let mut volume_translators: HashMap<VolumeTranslator, String> = HashMap::new();
    volume_translators.insert(VolumeTranslator::Stripe, stripe.to_string());

    return volume_create(volume, volume_translators, &transport, bricks, force);
}

/// This creates a new striped and replicated volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_create_striped_replicated(volume: &str,
                                        stripe: usize,
                                        replica: usize,
                                        transport: Transport,
                                        bricks: Vec<Brick>,
                                        force: bool)
                                        -> Result<i32, GlusterError> {

    let mut volume_translators: HashMap<VolumeTranslator, String> = HashMap::new();
    volume_translators.insert(VolumeTranslator::Stripe, stripe.to_string());
    volume_translators.insert(VolumeTranslator::Replica, replica.to_string());

    return volume_create(volume, volume_translators, &transport, bricks, force);
}

/// This creates a new distributed volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_create_distributed(volume: &str,
                                 transport: Transport,
                                 bricks: Vec<Brick>,
                                 force: bool)
                                 -> Result<i32, GlusterError> {

    let volume_translators: HashMap<VolumeTranslator, String> = HashMap::new();

    return volume_create(volume, volume_translators, &transport, bricks, force);

}

/// This creates a new erasure coded volume
/// # Failures
/// Will return GlusterError if the command fails to run
pub fn volume_create_erasure(volume: &str,
                             disperse: usize,
                             redundancy: usize,
                             transport: Transport,
                             bricks: Vec<Brick>,
                             force: bool)
                             -> Result<i32, GlusterError> {

    let mut volume_translators: HashMap<VolumeTranslator, String> = HashMap::new();
    volume_translators.insert(VolumeTranslator::Disperse, disperse.to_string());
    volume_translators.insert(VolumeTranslator::Redundancy, redundancy.to_string());

    return volume_create(volume, volume_translators, &transport, bricks, force);

}
