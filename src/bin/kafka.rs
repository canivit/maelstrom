use anyhow::Context;
use env_logger::Target;
use log::{info, LevelFilter};
use maelstrom::{
    DeconstructedInMessage, MessageSerializer, Node, PartialInMessage, SerializableIterator,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InPayload {
    Send {
        key: String,
        #[serde(rename = "msg")]
        item: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    ListCommittedOffsets {
        keys: Vec<String>,
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

struct KafkaNode<W>
where
    W: std::io::Write + Send + Sync + 'static,
{
    serializer: MessageSerializer<W>,
    log_manager: LogManager,
}

impl<W> Node<W, InPayload> for KafkaNode<W>
where
    W: std::io::Write + Send + Sync + 'static,
{
    fn new(_node_id: String, _node_ids: Vec<String>, serializer: MessageSerializer<W>) -> Self {
        Self {
            serializer,
            log_manager: LogManager::new(),
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
            InPayload::Send { key, item } => self.handle_send_msg(partial_in_msg, key, item),
            InPayload::Poll { offsets } => self.handle_poll_msg(partial_in_msg, offsets),
            InPayload::CommitOffsets { offsets } => {
                self.handle_commit_offsets_msg(partial_in_msg, offsets)
            }
            InPayload::ListCommittedOffsets { keys } => {
                self.handle_list_committed_offsets_msg(partial_in_msg, keys)
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
    ) -> anyhow::Result<()> {
        info!(
            "Received send message from {:?}. Key: {key:?}, Item: {item:?}",
            &partial_in_msg.src
        );
        let offset = self.log_manager.send(key, item);
        let payload = OutPayload::SendOk { offset };
        let mut out_msg = partial_in_msg.to_out_msg(payload);
        self.serializer
            .send(&mut out_msg)
            .context("failed to serialize send_ok message")
    }

    fn handle_poll_msg(
        &mut self,
        partial_in_msg: PartialInMessage,
        offsets: HashMap<String, usize>,
    ) -> anyhow::Result<()> {
        let items: HashMap<String, SerializableIterator<[usize; 2]>> = offsets
            .into_iter()
            .map(|(key, offset)| {
                let items = self.log_manager.poll(&key, offset);
                (key, items)
            })
            .collect();
        let payload = OutPayload::PollOk { items };
        let mut out_msg = partial_in_msg.to_out_msg(payload);
        self.serializer
            .send(&mut out_msg)
            .context("failed to serialize poll_ok message")
    }

    fn handle_commit_offsets_msg(
        &mut self,
        partial_in_msg: PartialInMessage,
        offsets: HashMap<String, usize>,
    ) -> anyhow::Result<()> {
        offsets
            .into_iter()
            .for_each(|(key, offset)| self.log_manager.commit(&key, offset));
        let mut out_msg = partial_in_msg.to_out_msg(OutPayload::CommitOffsetsOk);
        self.serializer
            .send(&mut out_msg)
            .context("failed to serialize commit_offsets_ok message")
    }

    fn handle_list_committed_offsets_msg(
        &mut self,
        partial_in_msg: PartialInMessage,
        keys: Vec<String>,
    ) -> anyhow::Result<()> {
        let offsets: HashMap<String, usize> = keys
            .into_iter()
            .filter_map(|key| {
                self.log_manager
                    .comitted_offset(&key)
                    .map(|offset| (key, offset))
            })
            .collect();
        let payload = OutPayload::ListCommittedOffsetsOk { offsets };
        let mut out_msg = partial_in_msg.to_out_msg(payload);
        self.serializer
            .send(&mut out_msg)
            .context("failed to serialize list_committed_offsets_ok message")
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
