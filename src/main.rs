mod client;
mod broker;
mod broker_client;
mod queue;
mod utils;

use tokio::sync::mpsc;
use tokio::stream::{StreamExt};
use tokio::net::TcpListener;
use tokio::task;
use std::str::from_utf8;
use broker::{
  Broker,
  BrokerAction,
  ResponseStatus,
};
use queue::Queue;
use tokio::time::{self, Duration};

use client::{
  Action,
  ClientMessage,
};

enum Event {
  Connection(client::Client),
  IoError(std::io::Error),
}

async fn handle_client_msg(
  broker: &mut Broker,
  msg: ClientMessage
) -> Result<(), mpsc::error::SendError<ClientMessage>> {
  match msg.action {
    Action::Data => {
      if msg.bytes.is_none() {
        unreachable!();
      }

      let bytes = msg.bytes.unwrap();
      let client = broker.clients.get_mut(&msg.client_id).unwrap();
      let delimiter_idx = utils::find_idx(&bytes, utils::SYMBOL_SPACE).unwrap_or(bytes.len());
      let command = bytes.slice(..delimiter_idx);
      let command = from_utf8(&command).unwrap_or("unknown");

      match Broker::match_action(command, &bytes) {
        BrokerAction::CreateQue(queue_name) => {
          let queue_name = queue_name.unwrap();

          println!("Queue created {:?}", queue_name);

          if broker.queues.get(&queue_name).is_none() {
            broker.queues.insert(String::from(&queue_name), Queue::new(&queue_name));
          }

          client.send_msg(ClientMessage::ok(&msg.client_id)).await?;
        },
        BrokerAction::Subscribe(queue_name) => {
          let queue_name = queue_name.unwrap();
          let queue = broker.queues.get_mut(&queue_name);

          if queue.is_none() {
            client.send_msg(ClientMessage::data(&msg.client_id, b"unknown_queue\n")).await?;

            return Ok(());
          }

          let queue = queue.unwrap();

          queue.subscribe(&client.client_id);
          client.send_msg(ClientMessage::ok(&msg.client_id)).await?;
        },
        BrokerAction::Publish(d) => {
          // TODO: check here queue existence
          let (queue_name, body) = d.unwrap();
          let queue = broker.queues.get_mut(&queue_name);

          if queue.is_none() {
            client.send_msg(ClientMessage::data(&msg.client_id, b"ERROR unknown_queue\n")).await?;

            return Ok(());
          }

          let queue = queue.unwrap();
          let message_id = queue.send_message(&body);

          broker.processing_messages.insert(message_id, queue_name);

          client.send_msg(ClientMessage::ok(&msg.client_id)).await?;

          stat_msg(&broker);
        },
        BrokerAction::Response(d) => {
          let (message_id, status) = d.unwrap();
          let queue_name = broker.processing_messages.remove(&message_id).unwrap();
          let queue = broker.queues.get_mut(&queue_name).unwrap();

          match status {
            ResponseStatus::Success => {
              broker.processing_messages.remove(&message_id);
              queue.message_ready(&message_id);
            },
            ResponseStatus::Fail => {
              queue.message_fail(&message_id);
            },
            ResponseStatus::Unknown => {
              // handle this!!
            },
          };
        }
        _ => {
          client.send_msg(ClientMessage::data(&msg.client_id, b"ERROR unknown_action\n")).await?;
        },
      };
    },
    Action::Disconnect => {
      println!("Client has been disconnected ID = {}", &msg.client_id);
      broker.clients.remove(&msg.client_id);

      for (_, queue) in broker.queues.iter_mut() {
        queue.unsubscribe(&msg.client_id);
      }
    },
  }

  Ok(())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
  let mut listener = TcpListener::bind("0.0.0.0:5544").await?;
  let (tx, mut rx) = mpsc::channel::<client::ClientMessage>(100);

  let mut income = listener.incoming().map(|s| {
    match s {
      Ok(stream) => Event::Connection(client::Client::new(
        tx.clone(),
        stream,
      )),
      Err(err) => Event::IoError(err),
    }
  });

  let mut broker: broker::Broker = broker::Broker::new();
  let mut loop_timeout = time::delay_for(Duration::from_millis(100));

  loop {
    tokio::select! {
      _ = &mut loop_timeout, if !loop_timeout.is_elapsed() => {
        loop_timeout = time::delay_for(Duration::from_millis(100));
      }
      Some(Event::Connection(client)) = income.next() => {
        let mut client = client;
        let tx = client.get_tx();

        broker.clients.insert(client.id.clone(), broker_client::BrokerClient::new(
          &client.id,
          tx,
        ));

        task::spawn(async move {
          client.process_socket().await;
        });
      }

      Some(msg) = rx.recv() => {
        if let Err(e) = handle_client_msg(
          &mut broker,
          msg,
        ).await {
          eprintln!("Error while message handling: {}", e);
        };
      }

      Some(messages) = broker.next_msg() => {
        Queue::process_messages(
          &mut broker,
          messages
        ).await;

        stat_msg(&broker);
      }
    };
  }
}

pub fn stat_msg(
  broker: &Broker,
) {
  let clients_count = broker.clients.len();
  let messages_count = broker.queues.iter()
    .map(|(_, queue)| queue.get_messages_count())
    .sum::<usize>();

  println!("CLIENTS = {}; MESSAGES = {}", clients_count, messages_count);
}