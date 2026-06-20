use std::{
    collections::{BTreeMap, HashMap},
    io::ErrorKind::InvalidInput,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{
            AtomicU64,
            Ordering::{self, Relaxed},
        },
    },
};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Notify},
};

use crate::messages::{self, Message, MessageId, Packet};

struct QueueEntry {
    is_deliverable: bool,
    msg: Vec<u8>,
}
pub struct Node {
    address: String,
    peers: Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>,
    id: u32,
    seq: AtomicU64,
    queue: Mutex<BTreeMap<(u64, MessageId), QueueEntry>>, // this queue is used to display messages
    proposed_msgs: Mutex<HashMap<MessageId, (u64, u8)>>,
    msg_idx: Mutex<HashMap<MessageId, u64>>,
    delivery_notify: Notify,
}

impl Node {
    pub fn new(address: impl Into<String>, id: u32) -> Self {
        Self {
            address: address.into(),
            peers: Mutex::new(HashMap::new()),
            id,
            seq: AtomicU64::new(0),
            queue: Mutex::new(BTreeMap::new()),
            msg_idx: Mutex::new(HashMap::new()),
            proposed_msgs: Mutex::new(HashMap::new()),
            delivery_notify: Notify::new(),
        }
    }

    pub async fn start(self: Arc<Self>) -> io::Result<tokio::task::JoinHandle<()>> {
        let listener = TcpListener::bind(&self.address).await?;

        println!("Node is listening on {}...", self.address);

        let handle = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((socket, _)) => {
                        let server = Arc::clone(&self);

                        tokio::spawn(async move {
                            if let Err(e) = server.handle_conn(socket).await {
                                eprintln!("{e}")
                            };
                        });
                    }
                    Err(e) => eprintln!("{e}"),
                }
            }
        });

        Ok(handle)
    }
    pub async fn connect(&self, addr: &str) -> io::Result<()> {
        let address: SocketAddr = addr.parse().map_err(|e| io::Error::new(InvalidInput, e))?;
        let mut stream = TcpStream::connect(address).await?;

        let address_bytes = self.address.as_bytes();
        stream.write_u32_le(address_bytes.len() as u32).await?;
        stream.write_all(address_bytes).await?;

        let stream = Arc::new(Mutex::new(stream));

        self.peers
            .lock()
            .await
            .entry(addr.to_owned())
            .or_insert(stream);
        Ok(())
    }

    pub async fn consume_messages(self: Arc<Self>) -> ! {
        loop {
            let ready = self.take_deliverable_messages().await;
            if ready.is_empty() {
                self.delivery_notify.notified().await;
                continue;
            }

            for msg in ready {
                println!("{}", String::from_utf8_lossy(&msg));
            }
        }
    }
    async fn handle_conn(self: Arc<Self>, mut stream: TcpStream) -> io::Result<()> {
        let peer_address = read_framed_string(&mut stream).await?;
        println!("Connected with {}", peer_address);

        let exists = {
            let map = self.peers.lock().await;
            map.contains_key(&peer_address)
        };

        if !exists {
            self.connect(&peer_address).await?;
        }

        loop {
            let msg_buf = match read_framed_bytes(&mut stream).await {
                Ok(msg) => msg,
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(err),
            };
            let packet_received = messages::Packet::from(msg_buf);
            let packet_to_send: Option<Packet> = match packet_received {
                messages::Packet::Agreement(message_id, final_seq) => {
                    self.apply_agreement(message_id, final_seq).await;
                    None
                }
                messages::Packet::ProposalReply(message_id, proposed) => {
                    let count = {
                        let mut msgs = self.proposed_msgs.lock().await;

                        let entry = msgs.entry(message_id).or_insert((0, 0));

                        entry.0 = entry.0.max(proposed);
                        entry.1 += 1;

                        *entry
                    };

                    if count.1 == 2 {
                        self.apply_agreement(message_id, count.0).await;
                        Some(Packet::Agreement(message_id, count.0))
                    } else {
                        None
                    }
                }
                messages::Packet::ProposalRequest(message_id, msg) => {
                    let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
                    {
                        let mut idx = self.msg_idx.lock().await;
                        idx.insert(message_id, seq);
                    }
                    {
                        self.queue.lock().await.insert(
                            (seq, message_id),
                            QueueEntry {
                                is_deliverable: false,
                                msg,
                            },
                        );
                    }
                    Some(Packet::ProposalReply(message_id, seq))
                }
            };
            if let Some(packet_to_send) = packet_to_send {
                if let Err(err) = self.send_packet(&peer_address, packet_to_send).await {
                    eprintln!("{err}");
                }
            }
        }

        stream.shutdown().await
    }

    async fn apply_agreement(&self, message_id: MessageId, final_seq: u64) {
        if let Some(old_seq) = self.msg_idx.lock().await.remove(&message_id) {
            {
                let mut queue = self.queue.lock().await;
                let entry = queue.remove(&(old_seq, message_id)).unwrap();
                queue.insert(
                    (final_seq, message_id),
                    QueueEntry {
                        is_deliverable: true,
                        msg: entry.msg,
                    },
                );
            }
            self.seq.fetch_max(final_seq, Relaxed);
            self.delivery_notify.notify_one();
        }
    }
    pub async fn send_message(&self, to: &str, msg: &str) -> io::Result<()> {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let msg_id = rand::random();

        let msg_buf = Message::from(msg);
        {
            self.proposed_msgs.lock().await.insert(msg_id, (seq, 1));
        }
        {
            self.queue.lock().await.insert(
                (seq, msg_id),
                QueueEntry {
                    is_deliverable: false,
                    msg: Vec::from(msg),
                },
            );
        }

        {
            self.msg_idx.lock().await.insert(msg_id, seq);
        }

        let packet = Packet::ProposalRequest(msg_id, msg_buf);
        self.send_packet(to, packet).await
    }

    async fn send_packet(&self, to: &str, packet: messages::Packet) -> io::Result<()> {
        let peer = {
            let peers = self.peers.lock().await;
            peers.get(to).cloned()
        };

        if let Some(stream) = peer {
            let buf: Vec<u8> = packet.into();
            let mut stream = stream.lock().await;
            stream.write_u32_le(buf.len() as u32).await?;
            stream.write_all(&buf).await?;
        }

        Ok(())
    }
    pub async fn broadcast(&self, msg: &[u8]) {
        let peers = {
            let peers = self.peers.lock().await;
            peers.values().cloned().collect::<Vec<_>>()
        };

        for peer in peers {
            let mut stream = peer.lock().await;
            if let Err(e) = stream.write_all(msg).await {
                eprintln!("{e}");
            }
        }
    }

    async fn take_deliverable_messages(&self) -> Vec<Vec<u8>> {
        let mut ready = Vec::new();
        let mut queue = self.queue.lock().await;

        while let Some((_, queue_entry)) = queue.first_key_value() {
            if !queue_entry.is_deliverable {
                break;
            }

            let (_, entry) = queue.pop_first().unwrap();
            ready.push(entry.msg);
        }

        ready
    }
}

async fn read_framed_bytes(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let len = stream.read_u32_le().await? as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn read_framed_string(stream: &mut TcpStream) -> io::Result<String> {
    let bytes = read_framed_bytes(stream).await?;
    String::from_utf8(bytes).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}
