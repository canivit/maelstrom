use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

#[derive(Serialize, Deserialize)]
struct Message<Payload> {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: Body<Payload>,
}

impl<Payload> Message<Payload> {
    fn into_reply(mut self, msg_id: usize) -> Self {
        self.body.in_reply_to = Some(self.body.msg_id);
        self.body.msg_id = msg_id;
        Self {
            src: self.dst,
            dst: self.src,
            body: self.body,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Body<Payload> {
    msg_id: usize,
    #[serde(default)]
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    payload: Payload,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitPayload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct TrailingLineSerializer<W>
where
    W: std::io::Write,
{
    writer: W,
}

impl<W> TrailingLineSerializer<W>
where
    W: std::io::Write,
{
    fn new(writer: W) -> Self {
        Self { writer }
    }

    fn serialize<V>(&mut self, value: &V) -> anyhow::Result<()>
    where
        V: Serialize + ?Sized,
    {
        serde_json::to_writer(&mut self.writer, value).context("failed to serialize value")?;
        self.writer
            .write_all(b"\n")
            .context("failed to write trailing line")
    }
}

struct Node<W>
where
    W: std::io::Write,
{
    msg_id: usize,
    serializer: TrailingLineSerializer<W>,
}

impl<W> Node<W>
where
    W: std::io::Write,
{
    fn new(msg_id: usize, serializer: TrailingLineSerializer<W>) -> Self
    where
        W: std::io::Write,
    {
        Self { msg_id, serializer }
    }

    fn process(&mut self, msg: Message<EchoPayload>) -> anyhow::Result<()> {
        let mut reply = msg.into_reply(self.msg_id);
        match reply.body.payload {
            EchoPayload::Echo { echo } => {
                reply.body.payload = EchoPayload::EchoOk { echo };
                self.serializer
                    .serialize(&reply)
                    .context("failed to serialize reply")?;
                self.msg_id += 1;
                Ok(())
            }
            _ => anyhow::bail!("received unexpected echo_ok message"),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let stdout = std::io::stdout().lock();
    let mut serializer = TrailingLineSerializer::new(stdout);

    let stdin = std::io::stdin().lock();
    let mut stream = Deserializer::from_reader(stdin).into_iter::<Message<InitPayload>>();

    let init_msg = stream
        .next()
        .ok_or(anyhow::Error::msg("failed to receive init message"))?
        .context("failed to deserialize init message")?;

    let mut reply = init_msg.into_reply(1);
    reply.body.payload = InitPayload::InitOk;
    serializer.serialize(&reply).context("")?;

    drop(stream);
    let stdin = std::io::stdin().lock();
    let stream = Deserializer::from_reader(stdin).into_iter::<Message<EchoPayload>>();
    let mut node = Node::new(2, serializer);
    for msg in stream {
        let message = msg.context("failed to deserialize echo message")?;
        node.process(message)?;
    }
    Ok(())
}
