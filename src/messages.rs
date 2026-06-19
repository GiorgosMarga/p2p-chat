pub enum Packet {
    Message(Vec<u8>),
    Ack(usize),
}

#[derive(Debug)]
pub struct Message {
    seq: u32,
    sender_id: u32,
    clock: usize,
    body: Vec<u8>,
}

impl From<Packet> for Vec<u8> {
    fn from(value: Packet) -> Self {
        match value {
            Packet::Ack(id) => {
                let mut encoded = Vec::with_capacity(9);
                encoded.extend((0 as u8).to_le_bytes());
                encoded.extend(id.to_le_bytes());
                encoded
            }
            Packet::Message(buf) => {
                let mut encoded = Vec::with_capacity(1 + buf.len());
                encoded.extend((1 as u8).to_le_bytes());
                encoded.extend_from_slice(&buf);
                encoded
            }
        }
    }
}
impl From<Vec<u8>> for Packet {
    fn from(value: Vec<u8>) -> Self {
        let t = value.get(0).unwrap();
        match t {
            0 => {
                let id = usize::from_le_bytes(value[1..].try_into().unwrap());
                Packet::Ack(id)
            }
            1 => Packet::Message(value[1..].to_vec()),
            _ => unreachable!(),
        }
    }
}
impl Message {
    pub fn new(seq: u32, sender_id: u32, clock: usize, body: Vec<u8>) -> Self {
        Self {
            seq,
            sender_id,
            clock,
            body,
        }
    }
}

impl From<Vec<u8>> for Message {
    fn from(value: Vec<u8>) -> Self {
        let seq = u32::from_le_bytes(value[0..4].try_into().unwrap());
        let sender_id = u32::from_le_bytes(value[4..8].try_into().unwrap());
        let clock = usize::from_le_bytes(value[8..16].try_into().unwrap());
        let mut body = Vec::with_capacity(value.len() - 16);
        body.extend_from_slice(&value[16..]);
        Message {
            seq,
            sender_id,
            clock,
            body,
        }
    }
}

impl From<Message> for Vec<u8> {
    fn from(value: Message) -> Self {
        let mut result: Vec<u8> = Vec::with_capacity(value.body.len() + 16);
        result.extend_from_slice(&value.seq.to_le_bytes());
        result.extend_from_slice(&value.sender_id.to_le_bytes());
        result.extend_from_slice(&value.clock.to_le_bytes());
        result.extend_from_slice(&value.body);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_round_trips_through_bytes() {
        let message = Message {
            seq: 42,
            sender_id: 7,
            clock: 1_234_567_890,
            body: b"hello, world".to_vec(),
        };

        let bytes: Vec<u8> = message.into();
        let decoded = Message::from(bytes);

        assert_eq!(decoded.seq, 42);
        assert_eq!(decoded.sender_id, 7);
        assert_eq!(decoded.clock, 1_234_567_890);
        assert_eq!(decoded.body, b"hello, world");
    }

    #[test]
    fn message_encoding_uses_little_endian_header() {
        let message = Message {
            seq: 0x0102_0304,
            sender_id: 0x0506_0708,
            clock: 0x090a_0b0c_0d0e_0f10,
            body: vec![0xaa, 0xbb],
        };

        let bytes: Vec<u8> = message.into();

        assert_eq!(
            bytes,
            vec![
                0x04, 0x03, 0x02, 0x01, // seq
                0x08, 0x07, 0x06, 0x05, // sender_id
                0x10, 0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 0x09, // clock
                0xaa, 0xbb, // body
            ]
        );
    }
}
