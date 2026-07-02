/// L2CAP Protocol Service Multiplexer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Psm(pub u16);

impl Psm {
    #[must_use]
    pub fn value(self) -> u16 {
        self.0
    }
}

impl From<u16> for Psm {
    fn from(v: u16) -> Self {
        Self(v)
    }
}
