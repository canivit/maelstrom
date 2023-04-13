use anyhow::Context;
use maelstrom::{run_node, Message, Node, TrailingLineSerializer};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum GeneratePayload {
    Generate,
    GenerateOk { id: u64 },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast { message: usize },
    BroadcastOk,
    Read,
    ReadOk { messages: Vec<usize> },
    Topology,
    TopologyOk,
}

struct UniqueNode<W>
where
    W: std::io::Write,
{
    msg_id: usize,
    serializer: TrailingLineSerializer<W>,
    store: Vec<usize>,
}

impl<W> Node<W, Payload> for UniqueNode<W>
where
    W: std::io::Write,
{
    fn new(
        _node_id: String,
        _node_ids: Vec<String>,
        msg_id: usize,
        serializer: TrailingLineSerializer<W>,
    ) -> Self {
        Self {
            msg_id,
            serializer,
            store: Vec::new(),
        }
    }

    fn process(mut self, msg: Message<Payload>) -> anyhow::Result<Self> {
        self.serializer
            .serialize(&msg)
            .context("failed to serialize msg")?;
        let mut reply = msg.into_reply(self.msg_id);
        match reply.body.payload {
            Payload::Broadcast { message } => {
                self.store.push(message);
                reply.body.payload = Payload::BroadcastOk;
                self.serializer
                    .serialize(&reply)
                    .context("failed to serialize broadcast_ok message")?;
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
            }

            Payload::Topology => {
                reply.body.payload = Payload::TopologyOk;
                self.serializer
                    .serialize(&reply)
                    .context("failed to serialize topology_ok message")?;
            }

            Payload::BroadcastOk => anyhow::bail!("received unexpected broadcast_ok message"),
            Payload::ReadOk { .. } => anyhow::bail!("received unexpected read_ok message"),
            Payload::TopologyOk => anyhow::bail!("received unexpected topology_ok message"),
        }

        self.msg_id += 1;
        Ok(self)
    }
}

fn main() -> anyhow::Result<()> {
    let reader = std::io::stdin().lock();
    let writer = std::io::stdout().lock();
    run_node::<UniqueNode<_>, _, _, _>(reader, writer)
}
