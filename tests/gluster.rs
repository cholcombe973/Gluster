extern crate gluster;
use gluster::*;

#[test]
fn test_translate_to_bytes(){
    let kb_result = gluster::translate_to_bytes("100KB").unwrap();
    println!("kb_result: {}", kb_result);
    assert_eq!(kb_result, 102400);

    let mb_result = gluster::translate_to_bytes("100MB").unwrap();
    println!("mb_result: {}", mb_result);
    assert_eq!(mb_result, 104857600);

    let gb_result = gluster::translate_to_bytes("100GB").unwrap();
    println!("gb_result: {}", gb_result);
    assert_eq!(gb_result, 107374182400);

    let tb_result = gluster::translate_to_bytes("100TB").unwrap();
    println!("tb_result: {}", tb_result);
    assert_eq!(tb_result, 109951162777600);

    let pb_result = gluster::translate_to_bytes("100PB").unwrap();
    println!("pb_result: {}", pb_result);
    assert_eq!(pb_result, 112589990684262400);
}

#[test]
fn test_get_local_ip(){
    let result = gluster::get_local_ip();
    println!("local ip: {:?}", result);
}
