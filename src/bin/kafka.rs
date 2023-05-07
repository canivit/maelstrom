use anyhow::Context;
use env_logger::Target;
use log::LevelFilter;
use maelstrom::{
    DeconstructedInMessage, MessageSerializer, Node, OutMessage, PartialInMessage,
    SerializableIterator,
};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;

#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InPayload {
    Send {
        key: String,
        #[serde(rename = "msg")]
        item: usize,
        client_info: Option<ClientInfo>,
    },
    Poll {
        offsets: HashMap<String, usize>,
        client_info: Option<ClientInfo>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
        client_info: Option<ClientInfo>,
    },
    ListCommittedOffsets {
        keys: Vec<String>,
        client_info: Option<ClientInfo>,
    },
}

#[derive(Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
enum OutPayload<'a> {
    SendOk {
        offset: usize,
    },
    PollOk {
        #[serde(rename = "msgs")]
        items: HashMap<String, SerializableIterator<'a, [usize; 2]>>,
    },
    CommitOffsetsOk,
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
    Send {
        key: &'a str,
        #[serde(rename = "msg")]
        item: usize,
        client_info: Option<ClientInfo>,
    },
    Poll {
        offsets: &'a HashMap<String, usize>,
        client_info: Option<ClientInfo>,
    },
    CommitOffsets {
        offsets: &'a HashMap<String, usize>,
        client_info: Option<ClientInfo>,
    },
    ListCommittedOffsets {
        keys: &'a Vec<String>,
        client_info: Option<ClientInfo>,
    },
}

#[derive(Serialize, Deserialize)]
struct ClientInfo {
    client_id: String,
    msg_id: Option<usize>,
}

struct LogManager {
    map: HashMap<String, LogItems>,
}

impl LogManager {
    const INITIAL_SIZE: usize = 100;
    fn new() -> Self {
        Self {
            map: HashMap::with_capacity(Self::INITIAL_SIZE),
        }
    }

    fn send(&mut self, key: String, item: usize) -> usize {
        let log = self.map.entry(key).or_insert(LogItems::new());
        log.send(item)
    }

    fn poll(&self, key: &str, offset: usize) -> SerializableIterator<[usize; 2]> {
        match self.map.get(key) {
            Some(log) => SerializableIterator::new(log.poll(offset)),
            None => SerializableIterator::new(std::iter::empty()),
        }
    }

    fn commit(&mut self, key: &str, offset: usize) {
        if let Some(log) = self.map.get_mut(key) {
            log.commit(offset);
        }
    }

    fn comitted_offset(&self, key: &str) -> Option<usize> {
        self.map.get(key)?.committed_offset()
    }
}

struct LogItems {
    commit_idx: Option<usize>,
    items: Vec<usize>,
}

impl LogItems {
    const INITIAL_SIZE: usize = 100;
    fn new() -> Self {
        Self {
            commit_idx: None,
            items: Vec::with_capacity(Self::INITIAL_SIZE),
        }
    }

    fn send(&mut self, item: usize) -> usize {
        self.items.push(item);
        self.items.len() - 1
    }

    fn poll(&self, offset: usize) -> impl Iterator<Item = [usize; 2]> + '_ {
        self.items
            .iter()
            .enumerate()
            .filter(move |(idx, _item)| *idx >= offset)
            .map(|(idx, item)| [idx, *item])
    }

    fn commit(&mut self, offset: usize) {
        match self.commit_idx {
            Some(commit_idx) if offset > commit_idx && offset < self.items.len() => {
                self.commit_idx = Some(offset);
            }
            None if offset < self.items.len() => {
                self.commit_idx = Some(offset);
            }
            _ => (),
        }
    }

    fn committed_offset(&self) -> Option<usize> {
        self.commit_idx
    }
}

#[derive(Clone, Copy)]
enum Role {
    Leader,
    Follower,
}

struct KafkaNode<W>
where
    W: std::io::Write + Send + Sync + 'static,
{
    serializer: RefCell<MessageSerializer<W>>,
    log_manager: LogManager,
    role: Role,
    leader_id: String,
    node_id: String,
}

impl<W> Node<W, InPayload> for KafkaNode<W>
where
    W: std::io::Write + Send + Sync + 'static,
{
    fn new(node_id: String, mut neighbors: Vec<String>, serializer: MessageSerializer<W>) -> Self {
        neighbors.sort();
        let (role, leader_id) = match neighbors.first() {
            Some(id) if id < &node_id => (Role::Follower, id.clone()),
            _ => (Role::Leader, node_id.clone()),
        };
        Self {
            serializer: serializer.into(),
            log_manager: LogManager::new(),
            role,
            leader_id,
            node_id,
        }
    }

    fn process(&mut self, in_msg: maelstrom::InMessage<InPayload>) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        let DeconstructedInMessage {
            partial_in_msg,
            in_payload,
        } = in_msg.into();
        match in_payload {
            InPayload::Send {
                key,
                item,
                client_info,
            } => self.handle_send_msg(partial_in_msg, key, item, client_info),
            InPayload::Poll {
                offsets,
                client_info,
            } => self.handle_poll_msg(partial_in_msg, offsets, client_info),
            InPayload::CommitOffsets {
                offsets,
                client_info,
            } => self.handle_commit_offsets_msg(partial_in_msg, offsets, client_info),
            InPayload::ListCommittedOffsets { keys, client_info } => {
                self.handle_list_committed_offsets_msg(partial_in_msg, keys, client_info)
            }
        }
    }

    fn shutdown(self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl<W> KafkaNode<W>
where
    W: std::io::Write + Send + Sync + 'static,
{
    fn handle_send_msg(
        &mut self,
        partial_in_msg: PartialInMessage,
        key: String,
        item: usize,
        client_info: Option<ClientInfo>,
    ) -> anyhow::Result<()> {
        match (self.role, client_info) {
            (Role::Leader, None) => {
                let offset = self.log_manager.send(key, item);
                let payload = OutPayload::SendOk { offset };
                let mut out_msg = partial_in_msg.to_out_msg(payload);
                self.serializer
                    .borrow_mut()
                    .send(&mut out_msg)
                    .context("failed to serialize send_ok message")
            }
            (Role::Leader, Some(ClientInfo { client_id, msg_id })) => {
                let offset = self.log_manager.send(key, item);
                let payload = OutPayload::SendOk { offset };
                let mut out_msg = OutMessage::new(&partial_in_msg.src, &client_id, msg_id, payload);
                self.serializer
                    .borrow_mut()
                    .send(&mut out_msg)
                    .context("failed to serialize send_ok message")
            }
            (Role::Follower, None) => {
                let payload = OutPayload::Send {
                    key: &key,
                    item,
                    client_info: Some(ClientInfo {
                        client_id: partial_in_msg.src,
                        msg_id: partial_in_msg.msg_id,
                    }),
                };
                let mut out_msg =
                    OutMessage::new(&partial_in_msg.dst, &self.leader_id, None, payload);
                out_msg.dst = &self.leader_id;
                self.serializer
                    .borrow_mut()
                    .send(&mut out_msg)
                    .context("failed to serialize send message")
            }
            (Role::Follower, Some(ClientInfo { client_id, .. })) => anyhow::bail!(
                "Node {} is a follower but received a send message with {client_id}",
                &self.node_id
            ),
        }
    }

    fn handle_poll_msg(
        &mut self,
        partial_in_msg: PartialInMessage,
        offsets: HashMap<String, usize>,
        client_info: Option<ClientInfo>,
    ) -> anyhow::Result<()> {
        match (self.role, client_info) {
            (Role::Leader, None) => {
                let payload = OutPayload::PollOk {
                    items: self.poll(offsets),
                };
                let mut out_msg = partial_in_msg.to_out_msg(payload);
                self.serializer
                    .borrow_mut()
                    .send(&mut out_msg)
                    .context("failed to serialize poll_ok message")
            }
            (Role::Leader, Some(ClientInfo { client_id, msg_id })) => {
                let payload = OutPayload::PollOk {
                    items: self.poll(offsets),
                };
                let mut out_msg = OutMessage::new(&partial_in_msg.src, &client_id, msg_id, payload);
                self.serializer
                    .borrow_mut()
                    .send(&mut out_msg)
                    .context("failed to serialize poll_ok message")
            }
            (Role::Follower, None) => {
                let payload = OutPayload::Poll {
                    offsets: &offsets,
                    client_info: Some(ClientInfo {
                        client_id: partial_in_msg.src,
                        msg_id: partial_in_msg.msg_id,
                    }),
                };
                let mut out_msg =
                    OutMessage::new(&partial_in_msg.dst, &self.leader_id, None, payload);
                out_msg.dst = &self.leader_id;
                self.serializer
                    .borrow_mut()
                    .send(&mut out_msg)
                    .context("failed to serialize poll message")
            }
            (Role::Follower, Some(ClientInfo { client_id, .. })) => anyhow::bail!(
                "Node {} is a follower but received a poll message with client_id {client_id}",
                &self.node_id,
            ),
        }
    }

    fn handle_commit_offsets_msg(
        &mut self,
        partial_in_msg: PartialInMessage,
        offsets: HashMap<String, usize>,
        client_info: Option<ClientInfo>,
    ) -> anyhow::Result<()> {
        match (self.role, client_info) {
            (Role::Leader, None) => {
                self.commit_offsets(offsets);
                let mut out_msg = partial_in_msg.to_out_msg(OutPayload::CommitOffsetsOk);
                self.serializer
                    .borrow_mut()
                    .send(&mut out_msg)
                    .context("failed to serialize commit_offsets_ok message")
            }
            (Role::Leader, Some(ClientInfo { client_id, msg_id })) => {
                self.commit_offsets(offsets);
                let payload = OutPayload::CommitOffsetsOk;
                let mut out_msg = OutMessage::new(&partial_in_msg.src, &client_id, msg_id, payload);
                self.serializer
                    .borrow_mut()
                    .send(&mut out_msg)
                    .context("failed to serialize commit_offsets_ok message")
            }
            (Role::Follower, None) => {
                let payload = OutPayload::CommitOffsets {
                    offsets: &offsets,
                    client_info: Some(ClientInfo {
                        client_id: partial_in_msg.src,
                        msg_id: partial_in_msg.msg_id,
                    }),
                };
                let mut out_msg = OutMessage::new(
                    &partial_in_msg.dst,
                    &self.leader_id,
                    None,
                    payload,
                );
                self.serializer
                    .borrow_mut()
                    .send(&mut out_msg)
                    .context("failed to serialize commit_offsets message")
            }
            (Role::Follower, Some(ClientInfo { client_id, .. })) => anyhow::bail!(
                "Node {} is a follower but received a commit_offsets message with client_id {client_id}",
                &self.node_id,
            ),
        }
    }

    fn handle_list_committed_offsets_msg(
        &mut self,
        partial_in_msg: PartialInMessage,
        keys: Vec<String>,
        client_info: Option<ClientInfo>,
    ) -> anyhow::Result<()> {
        match (self.role, client_info) {
            (Role::Leader, None) => {
                let offsets = self.list_comitted_offsets(keys);
                let payload = OutPayload::ListCommittedOffsetsOk { offsets };
                let mut out_msg = partial_in_msg.to_out_msg(payload);
                self.serializer
                    .borrow_mut()
                    .send(&mut out_msg)
                    .context("failed to serialize list_committed_offsets_ok message")
            }
            (Role::Leader, Some(ClientInfo { client_id, msg_id })) => {
                let offsets = self.list_comitted_offsets(keys);
                let payload = OutPayload::ListCommittedOffsetsOk { offsets };
                let mut out_msg = OutMessage::new(&partial_in_msg.src, &client_id, msg_id, payload);
                self.serializer
                    .borrow_mut()
                    .send(&mut out_msg)
                    .context("failed to serialize list_committed_offsets_ok message")
            }
            (Role::Follower, None) => {
                let payload = OutPayload::ListCommittedOffsets {
                    keys: &keys,
                    client_info: Some(ClientInfo {
                        client_id: partial_in_msg.src,
                        msg_id: partial_in_msg.msg_id,
                    }),
                };
                let mut out_msg =
                    OutMessage::new(&partial_in_msg.dst, &self.leader_id, None, payload);
                self.serializer
                    .borrow_mut()
                    .send(&mut out_msg)
                    .context("failed to serialize list_committed_offsets message")
            }
            (Role::Follower, Some(ClientInfo { client_id, .. })) => anyhow::bail!(
                "Node {} is a follower but received a list_committed_offsets message with client_id {client_id}",
                &self.node_id,
            ),
        }
    }

    fn poll(
        &self,
        offsets: HashMap<String, usize>,
    ) -> HashMap<String, SerializableIterator<[usize; 2]>> {
        offsets
            .into_iter()
            .map(|(key, offset)| {
                let items = self.log_manager.poll(&key, offset);
                (key, items)
            })
            .collect()
    }

    fn commit_offsets(&mut self, offsets: HashMap<String, usize>) {
        offsets
            .into_iter()
            .for_each(|(key, offset)| self.log_manager.commit(&key, offset));
    }

    fn list_comitted_offsets(&self, keys: Vec<String>) -> HashMap<String, usize> {
        keys.into_iter()
            .filter_map(|key| {
                self.log_manager
                    .comitted_offset(&key)
                    .map(|offset| (key, offset))
            })
            .collect()
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .target(Target::Stderr)
        .try_init()
        .context("failed to init logger")?;
    let reader = std::io::stdin().lock();
    let writer = std::io::stdout();
    maelstrom::run_node::<KafkaNode<_>, _, _, _>(reader, writer)
}
