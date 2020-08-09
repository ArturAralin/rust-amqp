use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Sender, Receiver};
use tokio::sync::mpsc;
use uuid::Uuid;
use bytes::Bytes;

use crate::utils;

#[derive(Debug, Clone)]
pub enum Action {
  Data,
  Disconnect,
}

#[derive(Debug)]
pub struct Client {
  pub id: Uuid,
  tx: Sender<ClientMessage>,
  socket: Box<TcpStream>,
  channel: (Sender<ClientMessage>, Receiver<ClientMessage>),
}

impl Client {
  pub fn new(
    tx: Sender<ClientMessage>,
    socket: TcpStream,
  ) -> Self {
    Client {
      id: Uuid::new_v4(),
      tx,
      socket: Box::new(socket),
      channel: mpsc::channel::<ClientMessage>(20),
    }
  }

  async fn read_socket(id: &Uuid, socket: &mut Box<TcpStream>) -> Result<Vec<ClientMessage>, std::io::Error> {
    let mut b = [0; 1024];
    let read = socket.read(&mut b[..]).await?;
    let client_id = id.clone();

    if read == 0 {
      return Ok(vec![ClientMessage::disconnected(&client_id)]);
    }

    let mut bodies: Vec<Vec<u8>> = vec![];
    let mut acc: Vec<u8> = vec![];

    for idx in 0..read {
      acc.push(b[idx]);

      if b[idx] == utils::SYMBOL_NEW_LINE {
        bodies.push(acc);
        acc = vec![];

        continue;
      }
    }

    // BUG here! Message must be large then b or MTU

    let messages = bodies.into_iter()
      .map(|body| ClientMessage::data(&client_id, &body))
      .collect::<Vec<_>>();

    Ok(messages)
  }

  pub async fn process_socket(&mut self) {
    println!("Client connected ID = {}", self.id);

    loop {
      let ch = &mut self.channel;
      let socket = &mut self.socket;

      tokio::select! {
        Some(msg) = ch.1.recv() => {
          if let Some(bytes) = &msg.bytes {
            if let Err(_) = socket.write_all(bytes).await {
              return;
            };
          }

          match msg.action {
            Action::Disconnect => {
              return;
            },
            _ => {},
          };
        }
        Ok(messages) = Self::read_socket(&self.id, socket) => {
          for message in messages.into_iter() {
            let action = message.action.clone();

            if let Err(_) = self.tx.send(message).await {
              return;
            }

            match action {
              Action::Disconnect => {
                return;
              },
              _ => {},
            };
          }
        }
      }
    }
  }

  pub fn get_tx(&mut self) -> Sender<ClientMessage> {
    self.channel.0.clone()
  }
}

#[derive(Debug)]
pub struct ClientMessage {
  pub client_id: Uuid,
  pub bytes: Option<Bytes>,
  pub action: Action,
}

impl ClientMessage {
  pub fn disconnected(client_id: &Uuid) -> Self {
    ClientMessage {
      client_id: client_id.clone(),
      action: Action::Disconnect,
      bytes: None,
    }
  }

  pub fn disconnected_with_reason(client_id: &Uuid, reason: &str) -> Self {
    ClientMessage {
      client_id: client_id.clone(),
      action: Action::Disconnect,
      bytes: Some(Bytes::from(reason.to_owned())),
    }
  }

  pub fn data(client_id: &Uuid, data: &[u8]) -> Self {
    ClientMessage {
      client_id: client_id.clone(),
      action: Action::Data,
      bytes: Some(Bytes::from(data.to_owned())),
    }
  }

  pub fn ok(client_id: &Uuid) -> Self {
    ClientMessage {
      client_id: client_id.clone(),
      action: Action::Data,
      bytes: Some(Bytes::from("OK\n")),
    }
  }
}


