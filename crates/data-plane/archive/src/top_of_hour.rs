#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum TopOfHour {
    One = 1,
    Two = 2,
    Three = 3,
    Four = 4,
    Six = 6,
    Twelve = 12,
}
