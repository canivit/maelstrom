use anyhow::Context;
use maelstrom::{run_node, InMessage, MessageSerializer, Node};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InPayload {
    Generate,
}

#[derive(Copy, Clone, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum OutPayload {
    GenerateOk { id: u64 },
}

struct UniqueNode<W>
where
    W: std::io::Write,
{
    node_id: String,
    serializer: MessageSerializer<W>,
}

impl<W> Node<W, InPayload> for UniqueNode<W>
where
    W: std::io::Write,
{
    fn new(node_id: String, _node_ids: Vec<String>, serializer: MessageSerializer<W>) -> Self {
        Self {
            node_id,
            serializer,
        }
    }

    fn process(&mut self, in_msg: InMessage<InPayload>) -> anyhow::Result<()> {
        let mut hasher = DefaultHasher::new();
        self.node_id.hash(&mut hasher);
        self.serializer.msg_id().hash(&mut hasher);
        let id = hasher.finish();
        let payload = OutPayload::GenerateOk { id };
        let out_msg = in_msg.to_reply(payload);
        self.serializer
            .send(out_msg)
            .context("failed to serialize reply")?;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let reader = std::io::stdin().lock();
    let writer = std::io::stdout().lock();
    run_node::<UniqueNode<_>, _, _, _>(reader, writer)
}
