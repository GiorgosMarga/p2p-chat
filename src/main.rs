use std::{env::args, sync::Arc};
use tokio::io::{self, AsyncReadExt};
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

    let server = Arc::new(server::Server::new(node_address, id));
    let _ = server.clone().start().await?;

    // give listener time to bind (or use a channel/ready signal)
    if let Some(peer_address) = peer_address {
        server.connect(peer_address).await?;
        sleep(std::time::Duration::from_secs(1)).await;
        for _ in 0..=5 {
            server.send_message("127.0.0.1:3000", "hello msg").await?;
            // sleep(std::time::Duration::from_secs(1)).await;
        }
    }
    let mut input = [0u8; 1024];
    loop {
        if let Ok(n) = io::stdin().read(&mut input).await {
            if n == 1 {
                break;
            }
            server.clone().broadcast(&input[..n]).await;
        }
    }
    let _ = tokio::signal::ctrl_c().await;

    Ok(())
}
