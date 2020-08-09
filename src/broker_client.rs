use tokio::sync::mpsc::{self, Sender};
use crate::client;
use uuid::Uuid;


#[derive(Debug)]
pub struct BrokerClient {
  pub client_id: Uuid,
  pub tx: Sender<client::ClientMessage>,
}

impl BrokerClient {
  pub fn new(
    client_id: &Uuid,
    tx: Sender<client::ClientMessage>
  ) -> Self {
    BrokerClient {
      client_id: client_id.clone(),
      tx,
    }
  }

  pub async fn send_msg(&mut self, msg: client::ClientMessage) -> Result<(), mpsc::error::SendError<client::ClientMessage>> {
    self.tx.send(msg).await?;

    Ok(())
  }
}
