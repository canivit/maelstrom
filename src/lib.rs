use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

#[derive(Deserialize)]
pub struct InMessage<Payload> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<Payload>,
}

#[derive(Copy, Clone, Serialize)]
pub struct OutMessage<'a, Payload>
where
    Payload: Copy,
{
    pub src: &'a str,
    #[serde(rename = "dest")]
    pub dst: &'a str,
    pub body: Body<Payload>,
}

impl<T> InMessage<T> {
    pub fn to_reply<U>(&self, payload: U) -> OutMessage<U>
    where
        U: Copy + Clone,
    {
        OutMessage {
            src: &self.dst,
            dst: &self.src,
            body: Body {
                msg_id: None,
                in_reply_to: self.body.msg_id,
                payload,
            },
        }
    }
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {
    pub msg_id: Option<usize>,
    #[serde(default)]
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitPayload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
}

#[derive(Copy, Clone, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitOkPayload {
    InitOk,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum InitOrRegular<P> {
    Init(InMessage<InitPayload>),
    Regular(InMessage<P>),
}

pub struct MessageSerializer<W>
where
    W: std::io::Write + Send + Sync,
{
    writer: W,
    msg_id: usize,
}

impl<W> MessageSerializer<W>
where
    W: std::io::Write + Send + Sync,
{
    pub fn new(writer: W) -> Self {
        Self { writer, msg_id: 1 }
    }

    pub fn send<T>(&mut self, mut msg: OutMessage<T>) -> anyhow::Result<()>
    where
        T: Copy + Serialize,
    {
        msg.body.msg_id = Some(self.msg_id);
        serde_json::to_writer(&mut self.writer, &msg).context("failed to serialize msg")?;
        self.writer
            .write_all(b"\n")
            .context("failed to write trailing line")?;
        self.msg_id += 1;
        Ok(())
    }

    pub fn msg_id(&self) -> usize {
        self.msg_id
    }
}

pub trait Node<W, P>
where
    W: std::io::Write + Send + Sync + 'static,
    P: DeserializeOwned,
{
    fn new(node_id: String, node_ids: Vec<String>, serializer: MessageSerializer<W>) -> Self;

    fn process(&mut self, in_msg: InMessage<P>) -> anyhow::Result<()>
    where
        Self: Sized;

    fn shutdown(self) -> anyhow::Result<()>;
}

pub fn run_node<N, W, R, P>(reader: R, writer: W) -> anyhow::Result<()>
where
    N: Node<W, P>,
    W: std::io::Write + Send + Sync + 'static,
    P: DeserializeOwned,
    R: std::io::Read,
{
    let mut sender = MessageSerializer::new(writer);
    let mut in_stream = Deserializer::from_reader(reader).into_iter::<InitOrRegular<P>>();

    let InitOrRegular::Init(init_msg) = in_stream
        .next()
        .ok_or(anyhow::Error::msg("failed to receive init message"))?
        .context("failed to deserialize init message")? else {
            anyhow::bail!("received unexpected request before init")
        };

    let init_ok_msg = init_msg.to_reply(InitOkPayload::InitOk);
    sender
        .send(init_ok_msg)
        .context("failed to send init_ok reply")?;

    let InitPayload::Init { node_id, node_ids } = init_msg.body.payload;
    let mut node: N = Node::new(node_id, node_ids, sender);
    for msg in in_stream {
        match msg.context("failed to deserialize message")? {
            InitOrRegular::Init(_) => anyhow::bail!("received unexpected extra init"),
            InitOrRegular::Regular(request) => node
                .process(request)
                .context("failed in node process function")?,
        }
    }
    node.shutdown()
        .context("failed to gracefully shutdown node")
}
