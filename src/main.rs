use std::{env::args, sync::Arc};
use tokio::io;
use tokio::time::sleep;

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
        server.connect(peer_address).await?;
        sleep(std::time::Duration::from_secs(1)).await;
        for i in 0..=5 {
            server
                .send_message(peer_address, &format!("hello_msg {}", i))
                .await?;
            // sleep(std::time::Duration::from_secs(1)).await;
        }
    }
    let _consumer = tokio::spawn(server.clone().consume_messages());
    let _ = tokio::signal::ctrl_c().await;

    Ok(())
}
