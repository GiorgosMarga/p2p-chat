use rand::random;
use std::fmt::format;
use std::time::Duration;
use std::{env::args, sync::Arc};
use tokio::fs::File;
use tokio::io::{self, AsyncBufReadExt};
use tokio::time::{self, sleep};

mod messages;
mod server;

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Vec<_> = args().collect();

    let node_address: &str = args.get(1).expect("provide node address").as_ref();
    let id: u32 = args
        .get(2)
        .expect("provide a node address")
        .parse()
        .expect("id must be a number");
    let peer_address = args.get(3);

    let server = Arc::new(server::Node::new(node_address, id));
    let _ = server.clone().start().await?;

    if let Some(peer_address) = peer_address {
        let addresses = peer_address.split(",");
        for address in addresses {
            server.connect(address).await.expect("cant connect with");
        }
    }
    tokio::time::sleep(Duration::from_secs(15)).await;

    let _consumer = tokio::spawn(server.clone().consume_messages());
    let _heartbeat = tokio::spawn(server.clone().heartbeat_loop());
    let dummy_word = format!("node{}.txt", id);

    let f = File::open(&dummy_word)
        .await
        .expect(&format!("file doesnt exist {}", dummy_word));
    let mut lines = tokio::io::BufReader::new(f).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let sleep_duration: u8 = random();
        time::sleep(Duration::from_secs((sleep_duration % 5) as u64)).await;
        server.send_message(&line).await;
    }
    let _ = tokio::signal::ctrl_c().await;

    Ok(())
}
