use anyhow::Context;
use maelstrom::{run_node, DeconstructedInMessage, InMessage, MessageSerializer, Node};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InPayload {
    Echo { echo: String },
}

#[derive(Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum OutPayload<'a> {
    EchoOk { echo: &'a str },
}

struct EchoNode<W>
where
    W: std::io::Write + Send + Sync + 'static,
{
    serializer: MessageSerializer<W>,
}

impl<W> Node<W, InPayload> for EchoNode<W>
where
    W: std::io::Write + Send + Sync,
{
    fn new(_node_id: String, _node_ids: Vec<String>, serializer: MessageSerializer<W>) -> Self {
        Self { serializer }
    }

    fn process(&mut self, in_msg: InMessage<InPayload>) -> anyhow::Result<()> {
        let DeconstructedInMessage {
            partial_in_msg,
            in_payload,
        } = in_msg.into();
        let InPayload::Echo { echo } = in_payload;
        let out_payload = OutPayload::EchoOk { echo: &echo };
        let mut out_msg = partial_in_msg.to_out_msg(out_payload);
        self.serializer
            .send(&mut out_msg)
            .context("failed to serialize echo_ok")?;
        Ok(())
    }

    fn shutdown(self) -> anyhow::Result<()> {
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let reader = std::io::stdin().lock();
    let writer = std::io::stdout();
    run_node::<EchoNode<_>, _, _, _>(reader, writer)
}
