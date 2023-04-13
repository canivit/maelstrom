use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

#[derive(Serialize, Deserialize)]
pub struct Message<Payload> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<Payload>,
}

impl<Payload> Message<Payload> {
    pub fn into_reply(mut self, msg_id: usize) -> Self {
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
pub struct Body<Payload> {
    pub msg_id: usize,
    #[serde(default)]
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
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
#[serde(untagged)]
enum InitOrRequest<P> {
    Init(Message<InitPayload>),
    Request(Message<P>),
}

pub struct TrailingLineSerializer<W>
where
    W: std::io::Write,
{
    writer: W,
}

impl<W> TrailingLineSerializer<W>
where
    W: std::io::Write,
{
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub fn serialize<V>(&mut self, value: &V) -> anyhow::Result<()>
    where
        V: Serialize + ?Sized,
    {
        serde_json::to_writer(&mut self.writer, value).context("failed to serialize value")?;
        self.writer
            .write_all(b"\n")
            .context("failed to write trailing line")
    }
}

pub trait Node<W, P>
where
    W: std::io::Write,
    P: Serialize + DeserializeOwned,
{
    fn new(
        node_id: String,
        node_ids: Vec<String>,
        msg_id: usize,
        serializer: TrailingLineSerializer<W>,
    ) -> Self;
    fn process(self, msg: Message<P>) -> anyhow::Result<Self>
    where
        Self: Sized;
}

pub fn run_node<N, W, R, P>(reader: R, writer: W) -> anyhow::Result<()>
where
    N: Node<W, P>,
    W: std::io::Write,
    P: Serialize + DeserializeOwned,
    R: std::io::Read,
{
    let mut serializer = TrailingLineSerializer::new(writer);
    let mut stream = Deserializer::from_reader(reader).into_iter::<InitOrRequest<P>>();

    let InitOrRequest::Init(init_msg) = stream
        .next()
        .ok_or(anyhow::Error::msg("failed to receive init message"))?
        .context("failed to deserialize init message")? else {
            anyhow::bail!("received unexpected request before init")
        };

    let mut reply = init_msg.into_reply(1);
    let InitPayload::Init { node_id, node_ids } = reply.body.payload else {
        anyhow::bail!("received unexpected init_ok message")
    };
    reply.body.payload = InitPayload::InitOk;
    serializer
        .serialize(&reply)
        .context("failed to send init_ok reply")?;

    let mut node: N = Node::new(node_id, node_ids, 2, serializer);
    for msg in stream {
        node = match msg.context("failed to deserialize message")? {
            InitOrRequest::Init(_) => anyhow::bail!("received unexpected extra init"),
            InitOrRequest::Request(request) => node
                .process(request)
                .context("failed in node process function")?,
        }
    }
    Ok(())
}
