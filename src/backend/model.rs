#[derive(Debug)]
pub struct Event {
    pub id: uuid::Uuid,
    pub version: u32,
    pub data: Vec<u8>,
}
