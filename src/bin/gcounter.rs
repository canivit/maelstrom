use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::{anyhow, Context};
use maelstrom::{run_node, Body, InMessage, MessageSerializer, Node, OutMessage};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InPayload {
    Add { delta: usize },
    Read,
    Broadcast { sum: usize },
}

#[derive(Copy, Clone, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum OutPayload {
    AddOk,
    ReadOk { value: usize },
    Broadcast { sum: usize },
}

struct CounterNode<W>
where
    W: std::io::Write + Send + Sync + 'static,
{
    node_id: String,
    serializer: Arc<Mutex<MessageSerializer<W>>>,
    map: Arc<Mutex<HashMap<String, usize>>>,
    handle: JoinHandle<anyhow::Result<()>>,
    tx: Sender<bool>,
}

impl<W> Node<W, InPayload> for CounterNode<W>
where
    W: std::io::Write + Send + Sync,
{
    fn new(node_id: String, node_ids: Vec<String>, serializer: MessageSerializer<W>) -> Self {
        let serializer = Arc::new(Mutex::new(serializer));
        let map = Arc::new(Mutex::new(HashMap::from_iter(vec![(node_id.clone(), 0)])));
        let (tx, rx) = mpsc::channel();
        let handle = {
            let node_id = node_id.clone();
            let mut neighbors = HashSet::<String>::from_iter(node_ids);
            neighbors.remove(&node_id);
            let serializer = Arc::clone(&serializer);
            let map = Arc::clone(&map);
            thread::spawn(move || {
                broadcast(
                    node_id,
                    map,
                    serializer,
                    neighbors,
                    rx,
                    CounterNode::<W>::REPLICATE_SLEEP_TIME,
                )
            })
        };
        Self {
            node_id,
            serializer,
            map,
            handle,
            tx,
        }
    }

    fn process(&mut self, in_msg: InMessage<InPayload>) -> anyhow::Result<()> {
        match in_msg.body.payload {
            InPayload::Add { delta } => self.handle_add_msg(in_msg, delta),
            InPayload::Read => self.handle_read_msg(in_msg),
            InPayload::Broadcast { sum } => self.handle_broadcast_msg(in_msg.src, sum),
        }
    }

    fn shutdown(self) -> anyhow::Result<()> {
        self.tx
            .send(true)
            .context("failed to send shutdown signal to broadcast thread")?;
        self.handle
            .join()
            .map_err(|_| anyhow!("failed to join broadcast thread"))?
    }
}

impl<W> CounterNode<W>
where
    W: std::io::Write + Send + Sync,
{
    const REPLICATE_SLEEP_TIME: Duration = Duration::from_millis(5);

    fn handle_add_msg(&mut self, in_msg: InMessage<InPayload>, delta: usize) -> anyhow::Result<()> {
        if let Some(sum) = self.lock_map()?.get_mut(&self.node_id) {
            *sum += delta;
        }
        let out_msg = in_msg.to_reply(OutPayload::AddOk);
        self.lock_serializer()?
            .send(out_msg)
            .context("failed to serialize add_ok message")
    }

    fn handle_read_msg(&self, in_msg: InMessage<InPayload>) -> anyhow::Result<()> {
        let sum = self.lock_map()?.values().sum::<usize>();
        let payload = OutPayload::ReadOk { value: sum };
        let out_msg = in_msg.to_reply(payload);
        self.lock_serializer()?
            .send(out_msg)
            .context("failed to serialize read_ok message")
    }

    fn handle_broadcast_msg(&self, node_id: String, sum: usize) -> anyhow::Result<()> {
        self.lock_map()?.insert(node_id, sum);
        Ok(())
    }

    fn lock_map(&self) -> anyhow::Result<MutexGuard<HashMap<String, usize>>> {
        lock_map(&self.map)
    }

    fn lock_serializer(&self) -> anyhow::Result<MutexGuard<MessageSerializer<W>>> {
        lock_serializer(&self.serializer)
    }
}

/// runs on a seperate thread and informs other nodes about the current sum
fn broadcast<W>(
    node_id: String,
    map: Arc<Mutex<HashMap<String, usize>>>,
    serializer: Arc<Mutex<MessageSerializer<W>>>,
    neighbors: HashSet<String>,
    rx: Receiver<bool>,
    sleep_time: Duration,
) -> anyhow::Result<()>
where
    W: std::io::Write + Send + Sync,
{
    while rx.try_recv().is_err() {
        thread::sleep(sleep_time);
        let sum = *lock_map(&map)?
            .get(&node_id)
            .ok_or_else(|| anyhow!("map does not contain the sum of self node_id: {node_id:?}"))?;
        let mut serializer = lock_serializer(&serializer)?;
        for neighbor in neighbors.iter() {
            let out_msg = OutMessage {
                src: &node_id,
                dst: neighbor,
                body: Body {
                    msg_id: None,
                    in_reply_to: None,
                    payload: OutPayload::Broadcast { sum },
                },
            };
            serializer
                .send(out_msg)
                .context("failed to serialize broadcast message")?;
        }
    }
    Ok(())
}

fn lock_map(
    map: &Arc<Mutex<HashMap<String, usize>>>,
) -> anyhow::Result<MutexGuard<HashMap<String, usize>>> {
    map.lock()
        .map_err(|_| anyhow!("failed to acquire lock for map"))
}

fn lock_serializer<W>(
    serializer: &Arc<Mutex<MessageSerializer<W>>>,
) -> anyhow::Result<MutexGuard<MessageSerializer<W>>>
where
    W: std::io::Write + Send + Sync,
{
    serializer
        .lock()
        .map_err(|_| anyhow!("failed to acquire lock for serializer"))
}

fn main() -> anyhow::Result<()> {
    let reader = std::io::stdin().lock();
    let writer = std::io::stdout();
    run_node::<CounterNode<_>, _, _, _>(reader, writer)
}
