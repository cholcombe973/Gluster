extern crate serde_json;

use std::collections::HashMap;

/// Read the stats from glusterd. This stat is typically located at: /var/lib/glusterd/stats
/// This function removes the mountpoint name from all the keys
pub fn read_aggr_fop(json_data: &str,
                     filename: &str)
                     -> Result<HashMap<String, f64>, super::GlusterError> {
    // Remove the mountpoint name from all the keys
    let cleaned_data = json_data.replace(&format!("gluster.brick.{}.aggr.", filename), "");
    let deserialized: HashMap<String, f64> = serde_json::from_str(&cleaned_data)?;
    Ok(deserialized)
}

/// Read the stats from glusterd. This stat is typically located at: /var/lib/glusterd/stats
/// This function removes the mountpoint name from all the keys
pub fn read_inter_fop(json_data: &str,
                      filename: &str)
                      -> Result<HashMap<String, f64>, super::GlusterError> {
    let cleaned_data = json_data.replace(&format!("gluster.brick.{}.inter.", filename), "");
    let deserialized: HashMap<String, f64> = serde_json::from_str(&cleaned_data)?;
    Ok(deserialized)
}
