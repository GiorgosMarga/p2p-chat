pub type MessageId = u64;

#[derive(Debug)]
pub enum Packet {
    ProposalRequest(MessageId, Vec<u8>),
    ProposalReply(MessageId, u64),
    Agreement(MessageId, u64),
}

pub type Message = Vec<u8>;

impl From<Packet> for Vec<u8> {
    fn from(value: Packet) -> Self {
        match value {
            Packet::ProposalReply(message_id, proposed_seq) => {
                let mut encoded = Vec::with_capacity(1 + 2 * size_of::<u64>()); // one byte for type + 8 bytes for message_id + 8 bytes for proposed_seq
                encoded.extend((0 as u8).to_le_bytes());
                encoded.extend(message_id.to_le_bytes());
                encoded.extend(proposed_seq.to_le_bytes());
                encoded
            }
            Packet::ProposalRequest(message_id, buf) => {
                let mut encoded = Vec::with_capacity(1 + size_of::<u64>() + buf.len());
                encoded.extend((1 as u8).to_le_bytes());
                encoded.extend(message_id.to_le_bytes());
                encoded.extend_from_slice(&buf);
                encoded
            }
            Packet::Agreement(message_id, final_seq) => {
                let mut encoded = Vec::with_capacity(1 + 2 * size_of::<u64>());
                encoded.extend((2 as u8).to_le_bytes());
                encoded.extend(message_id.to_le_bytes());
                encoded.extend(final_seq.to_le_bytes());
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
                let message_id = u64::from_le_bytes(value[1..9].try_into().unwrap());
                let proposed_seq = u64::from_le_bytes(value[9..].try_into().unwrap());
                Packet::ProposalReply(message_id, proposed_seq)
            }
            1 => {
                let message_id = u64::from_le_bytes(value[1..9].try_into().unwrap());
                Packet::ProposalRequest(message_id, value[9..].to_vec())
            }
            2 => {
                let message_id = u64::from_le_bytes(value[1..9].try_into().unwrap());
                let final_seq = u64::from_le_bytes(value[9..17].try_into().unwrap());
                Packet::Agreement(message_id, final_seq)
            }
            _ => unreachable!(),
        }
    }
}
