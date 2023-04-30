use anyhow::Context;
use maelstrom::{run_node, DeconstructedInMessage, InMessage, MessageSerializer, Node};
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
    W: std::io::Write + Send + Sync + 'static,
{
    node_id: String,
    serializer: MessageSerializer<W>,
}

impl<W> Node<W, InPayload> for UniqueNode<W>
where
    W: std::io::Write + Send + Sync,
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
        let DeconstructedInMessage { partial_in_msg, .. } = in_msg.into();
        let mut out_msg = partial_in_msg.to_out_msg(OutPayload::GenerateOk { id });
        self.serializer
            .send(&mut out_msg)
            .context("failed to serialize reply")?;
        Ok(())
    }

    fn shutdown(self) -> anyhow::Result<()> {
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let reader = std::io::stdin().lock();
    let writer = std::io::stdout();
    run_node::<UniqueNode<_>, _, _, _>(reader, writer)
}
