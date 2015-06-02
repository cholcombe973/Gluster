extern crate gluster;
use gluster::*;

#[test]
fn test_volume_info(){
    let output = gluster::volume_info(&"test".to_string());
    println!("{:?}", output);
}
