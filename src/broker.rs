use std::collections::HashMap;
use uuid::Uuid;
use bytes::Bytes;
use std::str::from_utf8;

use crate::broker_client::{BrokerClient};
use crate::queue::{Queue, QueueMessage};
use crate::utils;

#[derive(Debug)]
pub enum ResponseStatus {
  Success,
  Fail,
  Unknown,
}

impl From<&str> for ResponseStatus {
  fn from(s: &str) -> Self {
    match s {
      "SUCCESS" => ResponseStatus::Success,
      "FAIL" => ResponseStatus::Fail,
      _ => ResponseStatus::Unknown,
    }
  }
}

#[derive(Debug)]
pub enum BrokerAction {
  Publish(Result<(String, Bytes), std::str::Utf8Error>),
  Subscribe(Result<String, std::str::Utf8Error>),
  Response(Result<(Uuid, ResponseStatus), std::str::Utf8Error>),
  Unsubscribe,
  CreateQue(Result<String, std::str::Utf8Error>),
  Unknown,
}

pub struct Broker {
  pub clients: HashMap<Uuid, BrokerClient>,
  pub queues: HashMap<String, Queue>,
  pub processing_messages: HashMap<Uuid, String>
}

impl Broker {
  pub fn new() -> Self {
    Broker {
      clients: HashMap::new(),
      queues: HashMap::new(),
      processing_messages: HashMap::new(),
    }
  }

  pub fn match_action(s: &str, bytes: &Bytes) -> BrokerAction {
    match s {
      "CREATE_QUEUE" => {
        let queue_name = match std::str::from_utf8(&bytes.slice(13..bytes.len() - 1)) {
          Ok(s) => Ok(String::from(s)),
          Err(e) => Err(e),
        };

        BrokerAction::CreateQue(queue_name)
      },
      "PUBLISH" => {
        let body = bytes.slice(8..bytes.len() -1);
        let queue_name_idx = utils::find_idx(&body, utils::SYMBOL_SPACE).unwrap_or(body.len());
        let queue_name = body.slice(..queue_name_idx);
        let d = match from_utf8(&queue_name) {
          Ok(s) => Ok((
            String::from(s),
            body.slice(queue_name_idx + 1..)
          )),
          Err(e) => Err(e)
        };

        BrokerAction::Publish(d)
      },
      "SUBSCRIBE" => {
        let queue_name = match std::str::from_utf8(&bytes.slice(10..bytes.len() - 1)) {
          Ok(s) => Ok(String::from(s)),
          Err(e) => Err(e),
        };

        BrokerAction::Subscribe(queue_name)
      },
      "RESPONSE" => {
        let body = bytes.slice(9..bytes.len() - 1);
        let parts = from_utf8(&body)
          .unwrap()
          .split(" ")
          .collect::<Vec<&str>>();

        let status = ResponseStatus::from(parts[0]);
        let uuid = Uuid::parse_str(parts[1]).unwrap();

        BrokerAction::Response(Ok((uuid, status)))
      },
      "UNSUBSCRIBE" => BrokerAction::Unsubscribe,
      _ => BrokerAction::Unknown,
    }
  }

  pub async fn next_msg(&mut self) -> Option<Vec<QueueMessage>> {
    let mut messages: Vec<QueueMessage> = vec![];

    for (_, queue) in self.queues.iter_mut() {
      if let Some(msg) = queue.next_message() {
        messages.push(msg);
      }
    }

    if messages.len() == 0 {
      return None
    }

    if self.clients.len() == 0 {
      return None
    }

    Some(messages)
  }
}
