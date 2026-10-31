use super::*;

#[test]
fn reads_advance_the_cursor() {
    let frame = [1u8, 2, 0, 3, 0, 0, 0];
    let mut cursor = BinaryCursor::new(&frame);

    assert_eq!(cursor.read_u8().unwrap(), 1);
    assert_eq!(cursor.position(), 1);
    assert_eq!(cursor.read_u16().unwrap(), 2);
    assert_eq!(cursor.read_u32().unwrap(), 3);
    assert_eq!(cursor.position(), frame.len());
}

#[test]
fn seeking_and_skipping_move_the_cursor() {
    let frame = [0u8; 16];
    let mut cursor = BinaryCursor::new(&frame);

    cursor.skip(4).unwrap();
    assert_eq!(cursor.position(), 4);
    cursor.seek(8).unwrap();
    assert_eq!(cursor.position(), 8);
    assert!(cursor.seek(frame.len() + 1).is_err());
}

#[test]
fn reads_past_the_end_fail_without_advancing() {
    let frame = [1u8];
    let mut cursor = BinaryCursor::new(&frame);

    assert!(cursor.read_u16().is_err());
    assert_eq!(cursor.position(), 0);
}

#[test]
fn reads_length_prefixed_strings() {
    let frame = b"\x03abc";
    let mut cursor = BinaryCursor::new(frame);

    assert_eq!(cursor.read_var_string().unwrap(), "abc");
    assert_eq!(cursor.position(), frame.len());
}
