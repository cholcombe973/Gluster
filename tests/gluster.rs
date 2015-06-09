extern crate gluster;
use gluster::*;

#[test]
fn test_translate_to_bytes(){
    let kb_result = gluster::translate_to_bytes("100KB").unwrap();
    println!("kb_result: {}", kb_result);
    let mb_result = gluster::translate_to_bytes("100MB").unwrap();
    println!("mb_result: {}", mb_result);
    let gb_result = gluster::translate_to_bytes("100GB").unwrap();
    println!("gb_result: {}", gb_result);
    let tb_result = gluster::translate_to_bytes("100TB").unwrap();
    println!("tb_result: {}", tb_result);
    let pb_result = gluster::translate_to_bytes("100PB").unwrap();
    assert!("pb_result: {}", pb_result);
}

#[test]
fn test_volume_info(){
    let output = gluster::volume_info(&"test".to_string());
    println!("{:?}", output);
}
