use anyhow::{anyhow, Context};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::io::{BufRead, BufReader};

#[derive(Deserialize)]
pub struct InMessage<Payload> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<Payload>,
}

pub struct PartialInMessage {
    pub src: String,
    pub dst: String,
    pub msg_id: Option<usize>,
}

pub struct DeconstructedInMessage<Payload> {
    pub partial_in_msg: PartialInMessage,
    pub in_payload: Payload,
}

impl<Payload> From<InMessage<Payload>> for DeconstructedInMessage<Payload> {
    fn from(value: InMessage<Payload>) -> Self {
        Self {
            partial_in_msg: PartialInMessage {
                src: value.src,
                dst: value.dst,
                msg_id: value.body.msg_id,
            },
            in_payload: value.body.payload,
        }
    }
}

#[derive(Serialize)]
pub struct OutMessage<'a, Payload> {
    pub src: &'a str,
    #[serde(rename = "dest")]
    pub dst: &'a str,
    pub body: Body<Payload>,
}

impl PartialInMessage {
    pub fn to_out_msg<Payload>(&self, payload: Payload) -> OutMessage<Payload> {
        OutMessage {
            src: &self.dst,
            dst: &self.src,
            body: Body {
                msg_id: None,
                in_reply_to: self.msg_id,
                payload,
            },
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Body<Payload> {
    pub msg_id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

pub struct SerializableIterator<'a, T>(Box<RefCell<dyn Iterator<Item = T> + 'a>>)
where
    T: Serialize;

impl<'a, T> SerializableIterator<'a, T>
where
    T: Serialize,
{
    pub fn new<I>(iterator: I) -> Self
    where
        I: Iterator<Item = T> + 'a,
    {
        Self(Box::new(RefCell::new(iterator)))
    }
}

impl<'a, T> Serialize for SerializableIterator<'a, T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_seq(self.0.borrow_mut().into_iter())
    }
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

#[derive(Serialize)]
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

    pub fn send<T>(&mut self, msg: &mut OutMessage<T>) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        msg.body.msg_id = Some(self.msg_id);
        serde_json::to_writer(&mut self.writer, msg).context("failed to serialize msg")?;
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
    let mut in_stream = BufReader::new(reader).lines();

    let line = in_stream
        .next()
        .ok_or_else(|| anyhow!("did not receive init message"))?
        .context("failed to read init message")?;
    let init_msg: InMessage<InitPayload> = serde_json::from_str(&line)
        .with_context(|| format!("failed to deserialize {line:?} into init message"))?;

    let DeconstructedInMessage {
        partial_in_msg,
        in_payload: payload,
    } = init_msg.into();
    let mut init_ok_msg = partial_in_msg.to_out_msg(InitOkPayload::InitOk);
    sender
        .send(&mut init_ok_msg)
        .context("failed to send init_ok reply")?;

    let InitPayload::Init { node_id, node_ids } = payload;
    let mut node: N = Node::new(node_id, node_ids, sender);
    for line in in_stream {
        let line = line.context("failed to read the next line from input stream")?;
        let msg: InMessage<P> = serde_json::from_str(&line)
            .with_context(|| format!("failed to deserialize {line:?} into message"))?;
        node.process(msg)
            .context("failed in node process function")?;
    }
    node.shutdown()
        .context("failed to gracefully shutdown node")
}
