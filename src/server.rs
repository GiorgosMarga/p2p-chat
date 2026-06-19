use std::{collections::HashMap, io::ErrorKind::InvalidInput, net::SocketAddr, sync::{atomic::{AtomicU32, AtomicUsize, Ordering}, Arc}};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

use crate::messages::{self, Message};

pub struct Server {
    address: String,
    peers: Mutex<HashMap<String, Arc<Mutex<TcpStream>>>>,
    id: u32,
    seq: AtomicU32,
    clock: AtomicUsize,
}

impl Server {
    pub fn new(address: impl Into<String>, id: u32) -> Self {
        Self {
            address: address.into(),
            peers: Mutex::new(HashMap::new()),
            id,
            seq: AtomicU32::new(0),
            clock: AtomicUsize::new(0),
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

            let message = Message::from(msg_buf);
            dbg!(&message);
        }

        stream.shutdown().await
    }
    pub async fn send_message(&self, to: &str, msg: &str) -> io::Result<()> {
        let peer = {
            let peers = self.peers.lock().await;
            peers.get(to).cloned()
        };

        if let Some(stream) = peer {
            let mut stream = stream.lock().await;
            let seq = self.seq.fetch_add(1, Ordering::Relaxed);
            let clock = self.clock.fetch_add(1, Ordering::Relaxed);
            let message = messages::Message::new(seq, self.id, clock, Vec::from(msg));
            let msg_buf: Vec<u8> = message.into();
            stream.write_u32_le(msg_buf.len() as u32).await?;
            stream.write_all(&msg_buf).await?;
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
