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

pub mod fop;
pub mod heal;
pub mod peer;
pub mod volume;
mod rpc;

extern crate byteorder;
#[macro_use]
extern crate log;
extern crate regex;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate unix_socket;
extern crate uuid;

use regex::Regex;

use std::cmp::Ord;
use std::cmp::Ordering;
use std::error::Error;
use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::net::IpAddr;
use std::path::PathBuf;
use std::str::FromStr;

use volume::{Brick, volume_info};

// "%0.6lf,%s,%s,%0.4lf,%s,%s,%s,%s,%s,%s",
// epoch_time, fop_enum_to_pri_string (sample->fop_type),
// fop_enum_to_string (sample->fop_type),
// sample->elapsed, xlator_name, instance_name, username,
// group_name, hostname, port);
//

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

pub enum SplitBrainPolicy {
    Ctime,
    Disable,
    Majority,
    Mtime,
    Size,
}

impl SplitBrainPolicy {
    pub fn to_string(&self) -> String {
        match self {
            &SplitBrainPolicy::Ctime => "ctime".to_string(),
            &SplitBrainPolicy::Disable => "none".to_string(),
            &SplitBrainPolicy::Majority => "majority".to_string(),
            &SplitBrainPolicy::Mtime => "mtime".to_string(),
            &SplitBrainPolicy::Size => "size".to_string(),
        }
    }
    pub fn from_str(s: &str) -> Result<SplitBrainPolicy, GlusterError> {
        match s {
            "ctime" => Ok(SplitBrainPolicy::Ctime),
            "none" => Ok(SplitBrainPolicy::Disable),
            "majority" => Ok(SplitBrainPolicy::Majority),
            "mtime" => Ok(SplitBrainPolicy::Mtime),
            "size" => Ok(SplitBrainPolicy::Size),
            _ => Err(GlusterError::new("Unknown access mode".to_string())),
        }
    }
}
impl<'a> Into<&'a str> for SplitBrainPolicy {
    fn into(self) -> &'a str {
        match self {
            SplitBrainPolicy::Ctime => "ctime",
            SplitBrainPolicy::Disable => "none",
            SplitBrainPolicy::Majority => "majority",
            SplitBrainPolicy::Mtime => "mtime",
            SplitBrainPolicy::Size => "size",
        }
    }
}

pub enum AccessMode {
    ReadOnly,
    ReadWrite,
}

impl AccessMode {
    pub fn to_string(&self) -> String {
        match self {
            &AccessMode::ReadOnly => "read-only".to_string(),
            &AccessMode::ReadWrite => "read-write".to_string(),
        }
    }
    pub fn from_str(s: &str) -> AccessMode {
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
    pub fn to_string(&self) -> String {
        match self {
            &Toggle::On => "On".to_string(),
            &Toggle::Off => "Off".to_string(),
        }
    }
    pub fn from_str(s: &str) -> Toggle {
        match s {
            "on" => Toggle::On,
            "off" => Toggle::Off,
            "true" => Toggle::On,
            "false" => Toggle::Off,
            _ => Toggle::Off,
        }
    }
}

pub enum ScrubSchedule {
    Hourly,
    Daily,
    Weekly,
    BiWeekly,
    Monthly,
}
impl ScrubSchedule {
    pub fn to_string(&self) -> String {
        match self {
            &ScrubSchedule::Hourly => "hourly".to_string(),
            &ScrubSchedule::Daily => "daily".to_string(),
            &ScrubSchedule::Weekly => "weekly".to_string(),
            &ScrubSchedule::BiWeekly => "biweekly".to_string(),
            &ScrubSchedule::Monthly => "monthly".to_string(),
        }
    }
    pub fn from_str(s: &str) -> ScrubSchedule {
        match s {
            "hourly" => ScrubSchedule::Hourly,
            "daily" => ScrubSchedule::Daily,
            "weekly" => ScrubSchedule::Weekly,
            "biweekly" => ScrubSchedule::BiWeekly,
            "monthly" => ScrubSchedule::Monthly,
            _ => ScrubSchedule::Weekly,
        }
    }
}
pub enum ScrubAggression {
    Aggressive,
    Lazy,
    Normal,
}
impl ScrubAggression {
    pub fn to_string(&self) -> String {
        match self {
            &ScrubAggression::Aggressive => "aggressive".to_string(),
            &ScrubAggression::Lazy => "lazy".to_string(),
            &ScrubAggression::Normal => "normal".to_string(),
        }
    }
    pub fn from_str(s: &str) -> ScrubAggression {
        match s {
            "aggressive" => ScrubAggression::Aggressive,
            "lazy" => ScrubAggression::Lazy,
            "normal" => ScrubAggression::Normal,
            _ => ScrubAggression::Normal,
        }
    }
}

pub enum ScrubControl {
    Pause,
    Resume,
    Status,
    OnDemand,
}

impl ScrubControl {
    fn to_string(&self) -> String {
        match self {
            &ScrubControl::Pause => "pause".to_string(),
            &ScrubControl::Resume => "resume".to_string(),
            &ScrubControl::Status => "status".to_string(),
            &ScrubControl::OnDemand => "ondemand".to_string(),
        }
    }
}

impl FromStr for ScrubControl {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pause" => Ok(ScrubControl::Pause),
            "resume" => Ok(ScrubControl::Resume),
            "status" => Ok(ScrubControl::Status),
            "ondemand" => Ok(ScrubControl::OnDemand),
            _ => Err(format!("Unknown Control value: {}", s)),
        }
    }
}
pub enum BitrotOption {
    ScrubThrottle(ScrubAggression),
    ScrubFrequency(ScrubSchedule),
    Scrub(ScrubControl),
}

impl BitrotOption {
    fn to_string(&self) -> String {
        match self {
            &BitrotOption::ScrubThrottle(_) => "scrub-throttle".to_string(),
            &BitrotOption::ScrubFrequency(_) => "scrub-frequency".to_string(),
            &BitrotOption::Scrub(_) => "scrub".to_string(),

        }
    }
    fn value(&self) -> String {
        match self {
            &BitrotOption::ScrubThrottle(ref val) => val.to_string(),
            &BitrotOption::ScrubFrequency(ref val) => val.to_string(),
            &BitrotOption::Scrub(ref val) => val.to_string(),
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
    /// enable/disable client.ssl flag in the volume
    ClientSsl(Toggle),
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
    /// Interval in which we want to collect FOP latency samples.  2 means collect a sample every
    /// 2nd FOP.
    DiagnosticsFopSampleInterval(u64),
    /// The maximum size of our FOP sampling ring buffer. Default: 65535
    DiagnosticsFopSampleBufSize(u64),
    /// Enable the File Operation count translator
    DiagnosticsCountFopHits(Toggle),
    /// Interval (in seconds) at which to auto-dump statistics. Zero disables automatic dumping.
    DiagnosticsStatsDumpInterval(u64),
    /// The interval after wish a cached DNS entry will be re-validated.  Default: 24 hrs
    DiagnosticsStatsDnscacheTtlSec(u64),
    /// Statistics related to the latency of each operation would be tracked.
    DiagnosticsLatencyMeasurement(Toggle),
    /// Statistics related to file-operations would be tracked.
    DiagnosticsDumpFdStats(Toggle),
    /// Enables automatic resolution of split brain issues
    FavoriteChildPolicy(SplitBrainPolicy),
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
    /// enable/disable readdir-ahead translator in the volume
    PerformanceReadDirAhead(Toggle),
    /// If this option is enabled, the readdir operation is performed parallely on all the bricks,
    /// thus improving the performance of readdir. Note that the performance improvement is higher
    /// in large clusters
    PerformanceParallelReadDir(Toggle),
    /// maximum size of cache consumed by readdir-ahead xlator. This value is global and total
    /// memory consumption by readdir-ahead is capped by this value, irrespective of the
    /// number/size of directories cached
    PerformanceReadDirAheadCacheLimit(u64),
    /// Allow client connections from unprivileged ports. By default only privileged ports are
    /// allowed. This is a global setting in case insecure ports are to be enabled for all
    /// exports using a single option.
    ServerAllowInsecure(Toggle),
    /// Specifies the duration for the lock state to be maintained on the server after a
    /// network disconnection.
    ServerGraceTimeout(u16),
    /// enable/disable server.ssl flag in the volume
    ServerSsl(Toggle),
    /// Location of the state dump file.
    ServerStatedumpPath(PathBuf),

    SslAllow(String),
    SslCertificateDepth(u8),
    SslCipherList(String),
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
            &GlusterOption::ClientSsl(_) => "client.ssl".to_string(),
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
            }
            &GlusterOption::DiagnosticsClientLogLevel(_) => {
                "diagnostics.client-log-level".to_string()
            }
            &GlusterOption::DiagnosticsLatencyMeasurement(_) => {
                "diagnostics.latency-measurement".to_string()
            }
            &GlusterOption::DiagnosticsCountFopHits(_) => "diagnostics.count-fop-hits".to_string(),
            &GlusterOption::DiagnosticsDumpFdStats(_) => "diagnostics.dump-fd-stats".to_string(),
            &GlusterOption::DiagnosticsFopSampleBufSize(_) => {
                "diagnostics.fop-sample-buf-size".to_string()
            }
            &GlusterOption::DiagnosticsFopSampleInterval(_) => {
                "diagnostics.fop-sample-interval".to_string()
            }
            &GlusterOption::DiagnosticsStatsDnscacheTtlSec(_) => {
                "diagnostics.stats-dnscache-ttl-sec".to_string()
            }
            &GlusterOption::DiagnosticsStatsDumpInterval(_) => {
                "diagnostics.stats-dump-interval".to_string()
            }
            &GlusterOption::FavoriteChildPolicy(_) => "cluster.favorite-child-policy".to_string(),
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
            &GlusterOption::PerformanceReadDirAhead(_) => "performance.readdir-ahead".to_string(),
            &GlusterOption::PerformanceParallelReadDir(_) => {
                "performance.parallel-readdir".to_string()
            }
            &GlusterOption::PerformanceReadDirAheadCacheLimit(_) => {
                "performance.rda-cache-limit".to_string()
            }
            &GlusterOption::ServerAllowInsecure(_) => "server.allow-insecure".to_string(),
            &GlusterOption::ServerGraceTimeout(_) => "server.grace-timeout".to_string(),
            &GlusterOption::ServerSsl(_) => "server.ssl".to_string(),
            &GlusterOption::ServerStatedumpPath(_) => "server.statedump-path".to_string(),
            &GlusterOption::SslAllow(_) => "auth.ssl-allow".to_string(),
            &GlusterOption::SslCertificateDepth(_) => "ssl.certificate-depth".to_string(),
            &GlusterOption::SslCipherList(_) => "ssl.cipher-list".to_string(),
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
            &GlusterOption::ClientSsl(ref val) => val.to_string(),
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
            &GlusterOption::DiagnosticsFopSampleInterval(ref val) => val.to_string(),
            &GlusterOption::DiagnosticsFopSampleBufSize(ref val) => val.to_string(),
            &GlusterOption::DiagnosticsCountFopHits(ref val) => val.to_string(),
            &GlusterOption::DiagnosticsStatsDumpInterval(ref val) => val.to_string(),
            &GlusterOption::DiagnosticsStatsDnscacheTtlSec(ref val) => val.to_string(),
            &GlusterOption::FavoriteChildPolicy(ref val) => val.to_string(),
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
            &GlusterOption::PerformanceReadDirAhead(ref val) => val.to_string(),
            &GlusterOption::PerformanceParallelReadDir(ref val) => val.to_string(),
            &GlusterOption::PerformanceReadDirAheadCacheLimit(val) => val.to_string(),
            &GlusterOption::ServerAllowInsecure(ref val) => val.to_string(),
            &GlusterOption::ServerGraceTimeout(val) => val.to_string(),
            &GlusterOption::ServerSsl(ref val) => val.to_string(),
            &GlusterOption::SslAllow(ref val) => val.to_string(),
            &GlusterOption::SslCertificateDepth(val) => val.to_string(),
            &GlusterOption::SslCipherList(ref val) => val.to_string(),
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
            "auth.ssl-allow" => {
                return Ok(GlusterOption::SslAllow(value));
            }
            "client.ssl" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::ClientSsl(t));
            }
            "cluster.favorite-child-policy" => {
                let policy = SplitBrainPolicy::from_str(&value)?;
                return Ok(GlusterOption::FavoriteChildPolicy(policy));
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
            "diagnostics.count-fop-hits" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::DiagnosticsCountFopHits(t));
            }
            "diagnostics.stats-dump-interval" => {
                let i = try!(u64::from_str(&value));
                return Ok(GlusterOption::DiagnosticsStatsDumpInterval(i));
            }
            "diagnostics.fop-sample-buf-size" => {
                let i = try!(u64::from_str(&value));
                return Ok(GlusterOption::DiagnosticsFopSampleBufSize(i));
            }
            "diagnostics.fop-sample-interval" => {
                let i = try!(u64::from_str(&value));
                return Ok(GlusterOption::DiagnosticsFopSampleInterval(i));
            }
            "diagnostics.stats-dnscache-ttl-sec" => {
                let i = try!(u64::from_str(&value));
                return Ok(GlusterOption::DiagnosticsStatsDnscacheTtlSec(i));
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
            "performance-cache-max-file-size" => {
                let i = try!(u64::from_str(&value));
                return Ok(GlusterOption::PerformanceCacheMaxFileSize(i));
            }
            "performance-cache-min-file-size" => {
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
            "performance-readdir-ahead" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::PerformanceReadDirAhead(t));
            }
            "performance-parallel-readdir" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::PerformanceReadDirAhead(t));
            }
            "performance-readdir-cache-limit" => {
                let i = try!(u64::from_str(&value));
                return Ok(GlusterOption::PerformanceReadDirAheadCacheLimit(i));
            }
            "server.ssl" => {
                let t = Toggle::from_str(&value);
                return Ok(GlusterOption::ServerSsl(t));
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
            "ssl.certificate-depth" => {
                let i = try!(u8::from_str(&value));
                return Ok(GlusterOption::SslCertificateDepth(i));
            }
            "ssl.cipher-list" => {
                return Ok(GlusterOption::SslCipherList(value));
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
    AddrParseError(std::net::AddrParseError),
    FromUtf8Error(std::string::FromUtf8Error),
    IoError(io::Error),
    NoVolumesPresent,
    ParseError(uuid::ParseError),
    ParseBoolErr(std::str::ParseBoolError),
    ParseIntError(std::num::ParseIntError),
    RegexError(regex::Error),
    SerdeError(serde_json::Error),
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
            GlusterError::ParseError(ref err) => err.description().to_string(),
            GlusterError::AddrParseError(ref err) => err.description().to_string(),
            GlusterError::ParseIntError(ref err) => err.description().to_string(),
            GlusterError::ParseBoolErr(ref err) => err.description().to_string(),
            GlusterError::RegexError(ref err) => err.description().to_string(),
            GlusterError::NoVolumesPresent => "No volumes present".to_string(),
            GlusterError::SerdeError(ref err) => err.description().to_string(),
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
    fn from(err: std::net::AddrParseError) -> GlusterError {
        GlusterError::AddrParseError(err)
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

impl From<regex::Error> for GlusterError {
    fn from(err: regex::Error) -> GlusterError {
        GlusterError::RegexError(err)
    }
}

impl From<serde_json::Error> for GlusterError {
    fn from(err: serde_json::Error) -> GlusterError {
        GlusterError::SerdeError(err)
    }
}



#[derive(Debug, Eq, PartialEq)]
pub struct BrickStatus {
    pub brick: Brick,
    pub tcp_port: u16,
    pub rdma_port: u16,
    pub online: bool,
    pub pid: u16,
}

impl Ord for BrickStatus {
    fn cmp(&self, other: &BrickStatus) -> Ordering {
        self.brick.peer.cmp(&other.brick.peer)
    }
}
impl PartialOrd for BrickStatus {
    fn partial_cmp(&self, other: &BrickStatus) -> Option<Ordering> {
        Some(self.cmp(other))
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

fn process_output(output: std::process::Output) -> Result<i32, GlusterError> {
    let status = output.status;

    if status.success() {
        return Ok(0);
    } else {
        return Err(GlusterError::new(try!(String::from_utf8(output.stderr))));
    }
}

// TODO: Change me to Result<std::process::Output, String>
fn run_command<T>(command: &str,
                  arg_list: &Vec<T>,
                  as_root: bool,
                  script_mode: bool)
                  -> std::process::Output
    where T: AsRef<OsStr>
{
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
/// Returns the local IPAddr address associated with this server
/// # Failures
/// Returns a GlusterError representing any failure that may have happened
/// while trying to
/// query this information.
pub fn get_local_ip() -> Result<IpAddr, GlusterError> {
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
    let ip_addr = try!(local_ip[0].trim().parse::<IpAddr>());

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

// Note this will panic on failure to parse u64
/// This is a helper function to convert values such as 1PB into a bytes
/// # Examples
/// ```
/// extern crate gluster;
/// let bytes: u64 = gluster::translate_to_bytes("1GB").unwrap();
/// assert_eq!(bytes, 1073741824);
/// let bytes: f64 = gluster::translate_to_bytes("1.0GB").unwrap();
/// assert_eq!(bytes, 1073741824.0);
/// ```

pub fn translate_to_bytes<T>(value: &str) -> Option<T>
    where T: FromStr + ::std::ops::Mul<Output = T> + Copy
{
    let k = match T::from_str("1024") {
        Ok(n) => n,
        Err(_) => return None,
    };
    if value.ends_with("PB") {
        match value.trim_right_matches("PB").parse::<T>() {
            Ok(n) => return Some(n * k * k * k * k * k),
            Err(_) => return None,
        };
    } else if value.ends_with("TB") {
        match value.trim_right_matches("TB").parse::<T>() {
            Ok(n) => return Some(n * k * k * k * k),
            Err(_) => return None,
        };
    } else if value.ends_with("GB") {
        match value.trim_right_matches("GB").parse::<T>() {
            Ok(n) => return Some(n * k * k * k),
            Err(_) => return None,
        };
    } else if value.ends_with("MB") {
        match value.trim_right_matches("MB").parse::<T>() {
            Ok(n) => return Some(n * k * k),
            Err(_) => return None,
        };
    } else if value.ends_with("KB") {
        match value.trim_right_matches("KB").parse::<T>() {
            Ok(n) => return Some(n * k),
            Err(_) => return None,
        };
    } else if value.ends_with("Bytes") {
        match value.trim_right_matches("Bytes").parse::<T>() {
            Ok(n) => return Some(n),
            Err(_) => return None,
        };
    } else {
        return None;
    }
}

/// Return all bricks that are being served locally in the volume
pub fn get_local_bricks(volume: &str) -> Result<Vec<Brick>, GlusterError> {
    let vol_info = volume_info(volume)?;
    let local_ip = get_local_ip()?.to_string();
    let bricks: Vec<Brick> = vol_info.bricks
        .iter()
        .filter(|brick| brick.peer.hostname == local_ip)
        .map(|brick| brick.clone())
        .collect();
    Ok(bricks)
}
