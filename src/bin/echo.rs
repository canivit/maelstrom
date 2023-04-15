use anyhow::Context;
use maelstrom::{run_node, Body, InMessage, MessageSerializer, Node, OutMessage};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InPayload {
    Echo { echo: String },
}

#[derive(Copy, Clone, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum OutPayload<'a> {
    EchoOk { echo: &'a str },
}

struct EchoNode<W>
where
    W: std::io::Write,
{
    serializer: MessageSerializer<W>,
}

impl<W> Node<W, InPayload> for EchoNode<W>
where
    W: std::io::Write,
{
    fn new(_node_id: String, _node_ids: Vec<String>, serializer: MessageSerializer<W>) -> Self {
        Self { serializer }
    }

    fn process(&mut self, in_msg: InMessage<InPayload>) -> anyhow::Result<()> {
        let InPayload::Echo { echo } = in_msg.body.payload;
        let out_msg = OutMessage {
            src: &in_msg.dst,
            dst: &in_msg.src,
            body: Body {
                msg_id: None,
                in_reply_to: in_msg.body.msg_id,
                payload: OutPayload::EchoOk { echo: &echo },
            },
        };
        self.serializer
            .send(out_msg)
            .context("failed to serialize echo_ok")?;
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let reader = std::io::stdin().lock();
    let writer = std::io::stdout().lock();
    run_node::<EchoNode<_>, _, _, _>(reader, writer)
}
