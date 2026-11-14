#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum TopOfMinute {
    One = 1,
    Two = 2,
    Three = 3,
    Four = 4,
    Five = 5,
    Six = 6,
    Ten = 10,
    Twelve = 12,
    Fifteen = 15,
    Twenty = 20,
    Thirty = 30,
}
