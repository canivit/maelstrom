use std::collections::{HashMap, HashSet};

use anyhow::Context;
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
}

#[derive(Copy, Clone, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum OutPayload<'a> {
    BroadcastOk,
    ReadOk { messages: &'a HashSet<usize> },
    TopologyOk,
    Gossip { message: usize },
}

struct BroadcastNode<W>
where
    W: std::io::Write,
{
    node_id: String,
    node_ids: Vec<String>,
    serializer: MessageSerializer<W>,
    store: HashSet<usize>,
    topology: HashMap<String, Vec<String>>,
}

impl<W> Node<W, InPayload> for BroadcastNode<W>
where
    W: std::io::Write,
{
    fn new(node_id: String, node_ids: Vec<String>, serializer: MessageSerializer<W>) -> Self {
        Self {
            node_id,
            node_ids,
            serializer,
            store: HashSet::new(),
            topology: HashMap::new(),
        }
    }

    fn process(&mut self, in_msg: InMessage<InPayload>) -> anyhow::Result<()> {
        match in_msg.body.payload {
            InPayload::Broadcast { message } => {
                self.handle_broadcast_msg(&in_msg, message)?;
            }

            InPayload::Read => {
                let payload = OutPayload::ReadOk {
                    messages: &self.store,
                };
                let out_msg = in_msg.to_reply(payload);
                self.serializer
                    .send(out_msg)
                    .context("failed to serialize read_ok message")?;
            }

            InPayload::Topology { topology } => {
                self.topology = topology;
                let out_msg = OutMessage {
                    src: &in_msg.dst,
                    dst: &in_msg.src,
                    body: Body {
                        msg_id: None,
                        in_reply_to: in_msg.body.msg_id,
                        payload: OutPayload::TopologyOk,
                    },
                };
                self.serializer
                    .send(out_msg)
                    .context("failed to serialize topology_ok message")?;
            }

            InPayload::Gossip { message } => {
                self.store.insert(message);
            }
        }
        Ok(())
    }
}

impl<W> BroadcastNode<W>
where
    W: std::io::Write,
{
    fn handle_broadcast_msg(
        &mut self,
        broadcast_msg: &InMessage<InPayload>,
        message: usize,
    ) -> anyhow::Result<()> {
        self.store.insert(message);

        let out_msg = broadcast_msg.to_reply(OutPayload::BroadcastOk);
        self.serializer
            .send(out_msg)
            .context("failed to serialize broadcast_ok message")?;

        for neighbor in &self.node_ids {
            let out_msg = OutMessage {
                src: &self.node_id,
                dst: neighbor,
                body: Body {
                    msg_id: None,
                    in_reply_to: None,
                    payload: OutPayload::Gossip { message },
                },
            };
            self.serializer
                .send(out_msg)
                .context("failed to serialize gossip message")?;
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let reader = std::io::stdin().lock();
    let writer = std::io::stdout().lock();
    run_node::<BroadcastNode<_>, _, _, _>(reader, writer)
}
