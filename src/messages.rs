pub type MessageId = u64;

#[derive(Debug, Clone)]
pub enum Packet {
    ProposalRequest(MessageId, Vec<u8>),
    ProposalReply(MessageId, u64),
    Agreement(MessageId, u64),
    Ping(),
    Pong(),
}

pub type Message = Vec<u8>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketDecodeError {
    Empty,
    UnknownType(u8),
    InvalidLength,
}

impl From<&Packet> for Vec<u8> {
    fn from(value: &Packet) -> Self {
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
            Packet::Ping() => {
                vec![3]
            }
            Packet::Pong() => {
                vec![4]
            }
        }
    }
}
impl From<Packet> for Vec<u8> {
    fn from(value: Packet) -> Self {
        (&value).into()
    }
}

impl TryFrom<&[u8]> for Packet {
    type Error = PacketDecodeError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let Some(&packet_type) = value.first() else {
            return Err(PacketDecodeError::Empty);
        };

        match packet_type {
            0 => {
                if value.len() < 17 {
                    return Err(PacketDecodeError::InvalidLength);
                }
                let message_id = u64::from_le_bytes(value[1..9].try_into().unwrap());
                let proposed_seq = u64::from_le_bytes(value[9..17].try_into().unwrap());
                Ok(Packet::ProposalReply(message_id, proposed_seq))
            }
            1 => {
                if value.len() < 9 {
                    return Err(PacketDecodeError::InvalidLength);
                }
                let message_id = u64::from_le_bytes(value[1..9].try_into().unwrap());
                Ok(Packet::ProposalRequest(message_id, value[9..].to_vec()))
            }
            2 => {
                if value.len() < 17 {
                    return Err(PacketDecodeError::InvalidLength);
                }
                let message_id = u64::from_le_bytes(value[1..9].try_into().unwrap());
                let final_seq = u64::from_le_bytes(value[9..17].try_into().unwrap());
                Ok(Packet::Agreement(message_id, final_seq))
            }
            3 => Ok(Packet::Ping()),
            4 => Ok(Packet::Pong()),
            other => Err(PacketDecodeError::UnknownType(other)),
        }
    }
}
