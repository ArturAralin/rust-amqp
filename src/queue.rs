use std::collections::{
  HashMap,
  HashSet,
};
use tokio::sync::mpsc::{self, Sender};
use bytes::{Bytes};
use uuid::Uuid;
use rand::random;
use tokio::time::{self, Duration, Delay};

use crate::client::ClientMessage;
use crate::broker::Broker;
use crate::utils;

#[derive(Clone, Debug)]
pub enum QueueMessageState {
  Wait,
  Sent,
  Ready,
  Timeout,
}

#[derive(Clone, Debug)]
pub struct QueueMessage {
  id: Uuid,
  queue_topic: String,
  state: QueueMessageState,
  attempts: u8,
  message: Bytes,
  timeout: u64,
}

impl QueueMessage {
  fn get_delay(&self) -> Delay {
    time::delay_for(Duration::from_millis(self.timeout))
  }
}

pub struct Queue {
  topic: String,
  messages: HashMap<Uuid, QueueMessage>,
  subscribers: HashSet<Uuid>,
  message_delay: HashMap<Uuid, Delay>,
}

impl Queue {
  pub fn new(topic: &str) -> Self {
    Queue {
      topic: String::from(topic),
      messages: HashMap::new(),
      subscribers: HashSet::new(),
      message_delay: HashMap::new(),
    }
  }

  pub fn get_messages_count(&self) -> usize {
    self.messages.len()
  }

  pub fn subscribe(
    &mut self,
    client_id: &Uuid,
  ) {
    self.subscribers.insert(client_id.clone());
  }

  pub fn unsubscribe(
    &mut self,
    client_id: &Uuid,
  ) {
    self.subscribers.remove(client_id);
  }

  pub fn message_ready(&mut self, message_id: &Uuid) {
    self.messages.remove(message_id);
    self.message_delay.remove(message_id);
  }

  pub fn message_renew(&mut self, message_id: &Uuid) {
    let message = self.messages.get_mut(message_id).unwrap();
    message.state = QueueMessageState::Wait;

    self.message_delay.insert(message_id.clone(), message.get_delay());
  }

  pub fn message_fail(&mut self, message_id: &Uuid) {
    if let Some(message) = self.messages.get_mut(message_id) {
      if message.attempts == 0 {
        self.messages.remove(message_id);

        return;
      }

      message.state = QueueMessageState::Wait;
      message.attempts -= 1;
      self.message_delay.insert(message_id.clone(), message.get_delay());
    }
  }

  fn build_message(
    que_message: &mut QueueMessage,
  ) -> Vec<u8> {
    let header = Vec::from(format!("{} ", que_message.id));
    let body = que_message.message.to_vec();
    let body_end = Vec::from("\n");
    let data = [
      &header[..],
      &body[..],
      &body_end[..]
    ].concat();

    data
  }

  pub fn send_message(&mut self, message: &Bytes) -> Uuid {
    let message_id = Uuid::new_v4();

    let q_msg = QueueMessage {
      id: message_id.clone(),
      queue_topic: self.topic.clone(),
      state: QueueMessageState::Wait,
      message: message.clone(),
      attempts: 5,
      timeout: 3000,
    };

    self.message_delay.insert(message_id.clone(), q_msg.get_delay());
    self.messages.insert(
      message_id.clone(),
      q_msg,
    );

    message_id
  }

  pub fn next_message(&mut self) -> Option<QueueMessage> {
    let mut timeout_messages: Vec<Uuid> = vec![];
    let mut result_message: Option<QueueMessage> = None;

    for (_, message) in self.messages.iter_mut() {
      match message.state {
        QueueMessageState::Sent => {
          let delay = self.message_delay.get(&message.id).unwrap();

          if delay.is_elapsed() {
            timeout_messages.push(message.id.clone());
          }
        }
        QueueMessageState::Wait => {
          message.state = QueueMessageState::Sent;
          result_message = Some(message.clone());

          break;
        },
        _ => {}
      };
    }

    for message_id in timeout_messages {
      self.message_fail(&message_id);
    }

    if self.subscribers.len() == 0 {
      return None
    }

    result_message
  }

  pub async fn process_messages(
    broker: &mut Broker,
    mut messages: Vec<QueueMessage>
  ) {
    for message in messages.iter_mut() {
      match message.state {
        QueueMessageState::Sent => {
          let queue = broker.queues.get_mut(&message.queue_topic).unwrap();

          if queue.subscribers.len() == 0 {
            continue;
          }

          let client_id: Uuid = {
            // this can be more faster
            // if hashset will be duplicated with a vector
            let idx = random::<usize>() % queue.subscribers.len();
            let ids = queue.subscribers.iter().collect::<Vec<&Uuid>>();
            *ids.get(idx).unwrap().clone()
          };

          let client = broker.clients.get_mut(&client_id).unwrap();
          let data = Self::build_message(message);

          if let Err(_) = client.send_msg(ClientMessage::data(&client.client_id, &data)).await {
            queue.unsubscribe(&client_id);
            queue.message_renew(&message.id);
          }
        },
        _ => {}
      };
    }
  }
}

