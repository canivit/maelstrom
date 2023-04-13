use anyhow::Context;
use maelstrom::{run_node, Message, Node, TrailingLineSerializer};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode<W>
where
    W: std::io::Write,
{
    msg_id: usize,
    serializer: TrailingLineSerializer<W>,
}

impl<W> Node<W, EchoPayload> for EchoNode<W>
where
    W: std::io::Write,
{
    fn new(
        _node_id: String,
        _node_ids: Vec<String>,
        msg_id: usize,
        serializer: TrailingLineSerializer<W>,
    ) -> Self {
        Self { msg_id, serializer }
    }

    fn process(&mut self, msg: Message<EchoPayload>) -> anyhow::Result<()> {
        let mut reply = msg.into_reply(self.msg_id);
        match reply.body.payload {
            EchoPayload::Echo { echo } => {
                reply.body.payload = EchoPayload::EchoOk { echo };
                self.serializer
                    .serialize(&reply)
                    .context("failed to serialize echo_ok")?;
                self.msg_id += 1;
                Ok(())
            }
            _ => anyhow::bail!("received unexpected echo_ok message"),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let reader = std::io::stdin().lock();
    let writer = std::io::stdout().lock();
    run_node::<EchoNode<_>, _, _, _>(reader, writer)
}
