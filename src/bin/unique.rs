use anyhow::Context;
use maelstrom::{run_node, Message, Node, TrailingLineSerializer};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum GeneratePayload {
    Generate,
    GenerateOk { id: u64 },
}

struct UniqueNode<W>
where
    W: std::io::Write,
{
    node_id: String,
    msg_id: usize,
    serializer: TrailingLineSerializer<W>,
}

impl<W> Node<W, GeneratePayload> for UniqueNode<W>
where
    W: std::io::Write,
{
    fn new(
        node_id: String,
        _node_ids: Vec<String>,
        msg_id: usize,
        serializer: TrailingLineSerializer<W>,
    ) -> Self {
        Self {
            node_id,
            msg_id,
            serializer,
        }
    }

    fn process(mut self, msg: Message<GeneratePayload>) -> anyhow::Result<Self> {
        self.serializer
            .serialize(&msg)
            .context("failed to serialize msg")?;
        let mut reply = msg.into_reply(self.msg_id);
        match reply.body.payload {
            GeneratePayload::Generate => {
                let mut hasher = DefaultHasher::new();
                self.node_id.hash(&mut hasher);
                self.msg_id.hash(&mut hasher);
                let id = hasher.finish();
                reply.body.payload = GeneratePayload::GenerateOk { id };
                self.serializer
                    .serialize(&reply)
                    .context("failed to serialize reply")?;
                self.msg_id += 1;
                Ok(self)
            }
            _ => anyhow::bail!("received unexpected echo_ok message"),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let reader = std::io::stdin().lock();
    let writer = std::io::stdout().lock();
    run_node::<UniqueNode<_>, _, _, _>(reader, writer)
}
