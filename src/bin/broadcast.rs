use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::{anyhow, Context};
use maelstrom::{run_node, Body, InMessage, MessageSerializer, Node, OutMessage};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InPayload {
    Broadcast {
        message: usize,
    },
    Read,
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    Gossip {
        message: usize,
    },
    GossipOk {
        message: usize,
    },
}

#[derive(Copy, Clone, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum OutPayload<'a> {
    BroadcastOk,
    ReadOk { messages: &'a [usize] },
    TopologyOk,
    Gossip { message: usize },
    GossipOk { message: usize },
}

struct BroadcastNode<W>
where
    W: std::io::Write + Send + Sync + 'static,
{
    node_id: String,
    serializer: Arc<Mutex<MessageSerializer<W>>>,
    map: Arc<Mutex<HashMap<usize, HashSet<String>>>>,
    neighbors: Vec<String>,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
    tx: Option<Sender<bool>>,
}

impl<W> Node<W, InPayload> for BroadcastNode<W>
where
    W: std::io::Write + Send + Sync,
{
    fn new(node_id: String, _node_ids: Vec<String>, serializer: MessageSerializer<W>) -> Self {
        Self {
            node_id,
            serializer: Arc::new(Mutex::new(serializer)),
            map: Arc::new(Mutex::new(HashMap::new())),
            neighbors: Vec::new(),
            handle: None,
            tx: None,
        }
    }

    fn process(&mut self, in_msg: InMessage<InPayload>) -> anyhow::Result<()> {
        match in_msg.body.payload {
            InPayload::Broadcast { message } => self.handle_broadcast_msg(&in_msg, message),
            InPayload::Read => {
                let messages = self.lock_map()?.keys().copied().collect::<Vec<_>>();
                let payload = OutPayload::ReadOk {
                    messages: messages.as_slice(),
                };
                let out_msg = in_msg.to_reply(payload);
                self.lock_serializer()?
                    .send(out_msg)
                    .context("failed to serialize read_ok message")
            }
            InPayload::Topology { topology } => {
                self.handle_topology_msg(topology, &in_msg.src, &in_msg.dst, in_msg.body.msg_id)
            }
            InPayload::Gossip { message } => self.handle_gossip_msg(&in_msg, message),
            InPayload::GossipOk { message } => {
                self.lock_map()?.entry(message).and_modify(|set| {
                    set.insert(in_msg.src);
                });
                Ok(())
            }
        }
    }

    fn shutdown(self) -> anyhow::Result<()> {
        if let Some(tx) = self.tx {
            tx.send(true)
                .context("failed to send shutdown signal to gossip thread")?;
        }
        if let Some(handle) = self.handle {
            handle
                .join()
                .map_err(|_| anyhow!("failed to join gossip thread"))??;
        }
        Ok(())
    }
}

impl<W> BroadcastNode<W>
where
    W: std::io::Write + Send + Sync,
{
    fn handle_broadcast_msg(
        &mut self,
        broadcast_msg: &InMessage<InPayload>,
        message: usize,
    ) -> anyhow::Result<()> {
        let out_msg = broadcast_msg.to_reply(OutPayload::BroadcastOk);
        self.lock_serializer()?
            .send(out_msg)
            .context("failed to serialize broadcast_ok message")?;
        {
            let mut map = self.lock_map()?;
            if map.contains_key(&message) {
                return Ok(());
            }
            map.insert(message, HashSet::new());
        }
        self.gossip_to_neighbors(message)
    }

    fn handle_gossip_msg(
        &mut self,
        gossip_msg: &InMessage<InPayload>,
        message: usize,
    ) -> anyhow::Result<()> {
        let payload = OutPayload::GossipOk { message };
        let out_msg = gossip_msg.to_reply(payload);
        self.lock_serializer()?
            .send(out_msg)
            .context("failed to serialize gossip_ok message")?;
        {
            let mut map = self.lock_map()?;
            if map.contains_key(&message) {
                return Ok(());
            }
            map.insert(message, HashSet::new());
        }
        self.gossip_to_neighbors(message)
    }

    fn handle_topology_msg(
        &mut self,
        mut topology: HashMap<String, Vec<String>>,
        msg_src: &str,
        msg_dst: &str,
        msg_id: Option<usize>,
    ) -> anyhow::Result<()> {
        self.neighbors = topology
            .remove(&self.node_id)
            .ok_or(anyhow!("topology does not contain self"))?;

        let node_id = self.node_id.clone();
        let map = Arc::clone(&self.map);
        let serializer = Arc::clone(&self.serializer);
        let neighbors = HashSet::from_iter(self.neighbors.clone());
        let (tx, rx) = mpsc::channel();
        self.tx = Some(tx);
        self.handle = Some(thread::spawn(move || {
            replicate_map(node_id, map, serializer, neighbors, rx)
        }));

        let out_msg = OutMessage {
            src: msg_dst,
            dst: msg_src,
            body: Body {
                msg_id: None,
                in_reply_to: msg_id,
                payload: OutPayload::TopologyOk,
            },
        };
        self.lock_serializer()?
            .send(out_msg)
            .context("failed to serialize topology_ok message")
    }

    fn gossip_to_neighbors(&mut self, message: usize) -> anyhow::Result<()> {
        for neighbor in &self.neighbors {
            let out_msg = OutMessage {
                src: &self.node_id,
                dst: neighbor,
                body: Body {
                    msg_id: None,
                    in_reply_to: None,
                    payload: OutPayload::Gossip { message },
                },
            };
            self.lock_serializer()?
                .send(out_msg)
                .context("failed to serialize gossip message")?;
        }
        Ok(())
    }

    fn lock_map(&self) -> anyhow::Result<MutexGuard<HashMap<usize, HashSet<String>>>> {
        self.map
            .lock()
            .map_err(|_| anyhow!("failed to acquire lock for map"))
    }

    fn lock_serializer(&self) -> anyhow::Result<MutexGuard<MessageSerializer<W>>> {
        self.serializer
            .lock()
            .map_err(|_| anyhow!("failed to acquire lock for serializer"))
    }
}

/// runs on a seperate thread and replicates all keys in other nodes by periodically gossiping
fn replicate_map<W>(
    node_id: String,
    map: Arc<Mutex<HashMap<usize, HashSet<String>>>>,
    serializer: Arc<Mutex<MessageSerializer<W>>>,
    all_neighbors: HashSet<String>,
    rx: Receiver<bool>,
) -> anyhow::Result<()>
where
    W: std::io::Write + Send + Sync,
{
    while rx.try_recv().is_err() {
        thread::sleep(Duration::from_millis(1));
        let map = map
            .lock()
            .map_err(|_| anyhow!("failed to acquire lock for map"))?;
        let mut serializer = serializer
            .lock()
            .map_err(|_| anyhow!("failed to acquire lock for serializer"))?;
        map.iter()
            .try_for_each(|(&message, neighbors)| -> anyhow::Result<()> {
                let mut missing_neighbors = all_neighbors.difference(neighbors);
                missing_neighbors.try_for_each(|neighbor| -> anyhow::Result<()> {
                    let out_msg = OutMessage {
                        src: &node_id,
                        dst: neighbor,
                        body: Body {
                            msg_id: None,
                            in_reply_to: None,
                            payload: OutPayload::Gossip { message },
                        },
                    };
                    serializer
                        .send(out_msg)
                        .context("failed to serialize gossip message in gossip thread")
                })
            })?;
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let reader = std::io::stdin().lock();
    let writer = std::io::stdout();
    run_node::<BroadcastNode<_>, _, _, _>(reader, writer)
}
