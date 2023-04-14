use std::collections::{HashMap, HashSet};

use anyhow::Context;
use maelstrom::{run_node, Message, Node, TrailingLineSerializer};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Gossip {
        message: usize,
    },
}

struct BroadcastNode<W>
where
    W: std::io::Write,
{
    node_ids: Vec<String>,
    msg_id: usize,
    serializer: TrailingLineSerializer<W>,
    store: HashSet<usize>,
    topology: HashMap<String, Vec<String>>,
}

impl<W> Node<W, Payload> for BroadcastNode<W>
where
    W: std::io::Write,
{
    fn new(
        _node_id: String,
        node_ids: Vec<String>,
        msg_id: usize,
        serializer: TrailingLineSerializer<W>,
    ) -> Self {
        Self {
            node_ids,
            msg_id,
            serializer,
            store: HashSet::new(),
            topology: HashMap::new(),
        }
    }

    fn process(mut self, msg: Message<Payload>) -> anyhow::Result<Self> {
        self.serializer
            .serialize(&msg)
            .context("failed to serialize msg")?;
        let mut reply = msg.into_reply(self.msg_id);
        match reply.body.payload {
            Payload::Broadcast { message } => {
                self.handle_broadcast(reply, message)?;
            }

            Payload::Read => {
                reply.body.payload = Payload::ReadOk {
                    messages: self.store,
                };
                self.serializer
                    .serialize(&reply)
                    .context("failed to serialize read_ok message")?;
                let Payload::ReadOk { messages } = reply.body.payload else {
                    anyhow::bail!("failed to recover store after move")
                };
                self.store = messages;
                self.msg_id += 1;
            }

            Payload::Topology { topology } => {
                self.topology = topology;
                reply.body.payload = Payload::TopologyOk;
                self.serializer
                    .serialize(&reply)
                    .context("failed to serialize topology_ok message")?;
                self.msg_id += 1;
            }

            Payload::Gossip { message } => {
                self.store.insert(message);
            }

            Payload::BroadcastOk => anyhow::bail!("received unexpected broadcast_ok message"),
            Payload::ReadOk { .. } => anyhow::bail!("received unexpected read_ok message"),
            Payload::TopologyOk => anyhow::bail!("received unexpected topology_ok message"),
        }

        Ok(self)
    }
}

impl<W> BroadcastNode<W>
where
    W: std::io::Write,
{
    fn handle_broadcast(
        &mut self,
        mut reply: Message<Payload>,
        message: usize,
    ) -> anyhow::Result<()> {
        self.store.insert(message);

        reply.body.payload = Payload::BroadcastOk;
        self.serializer
            .serialize(&reply)
            .context("failed to serialize broadcast_ok message")?;
        self.msg_id += 1;

        reply.body.payload = Payload::Gossip { message };
        for neighbor in &self.node_ids {
            reply.dst = neighbor.clone();
            reply.body.msg_id = self.msg_id;
            self.serializer
                .serialize(&reply)
                .context("failed to serialize gossip message")?;
            self.msg_id += 1;
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let reader = std::io::stdin().lock();
    let writer = std::io::stdout().lock();
    run_node::<BroadcastNode<_>, _, _, _>(reader, writer)
}
