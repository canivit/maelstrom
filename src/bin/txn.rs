use std::collections::HashMap;

use anyhow::Context;
use maelstrom::{DeconstructedInMessage, InMessage, MessageSerializer, Node};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InPayload {
    Txn { txn: Vec<Transaction> },
}

#[derive(Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum OutPayload<'a> {
    TxnOk { txn: &'a [Transaction] },
}

#[derive(Debug, PartialEq, Clone, Copy)]
enum Transaction {
    Read { key: usize, value: Option<usize> },
    Write { key: usize, value: usize },
}

impl<'a> Deserialize<'a> for Transaction {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        #[derive(Deserialize)]
        struct Data(String, usize, Option<usize>);

        let Data(op, key, value) = Data::deserialize(deserializer)?;
        match op.as_str() {
            "r" => Ok(Transaction::Read { key, value }),
            "w" => value
                .ok_or_else(|| serde::de::Error::custom("value is required for write transaction"))
                .map(|v| Transaction::Write { key, value: v }),
            any => Err(serde::de::Error::unknown_variant(any, &["r", "w"])),
        }
    }
}

impl Serialize for Transaction {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Data(&'static str, usize, Option<usize>);
        let data = match self {
            Transaction::Read { key, value } => Data("r", *key, *value),
            Transaction::Write { key, value } => Data("w", *key, Some(*value)),
        };
        data.serialize(serializer)
    }
}

struct KVStore {
    map: HashMap<usize, usize>,
}

impl KVStore {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    fn apply_single(&mut self, transaction: &mut Transaction) {
        match transaction {
            Transaction::Read { key, value } => {
                *value = self.map.get(key).copied();
            }
            Transaction::Write { key, value } => {
                self.map.insert(*key, *value);
            }
        }
    }

    fn apply_multi(&mut self, transactions: &mut [Transaction]) {
        transactions.iter_mut().for_each(|t| self.apply_single(t));
    }
}

struct TxnNode<W>
where
    W: std::io::Write + Send + Sync + 'static,
{
    serializer: MessageSerializer<W>,
    store: KVStore,
}

impl<W> Node<W, InPayload> for TxnNode<W>
where
    W: std::io::Write + Send + Sync + 'static,
{
    fn new(_node_id: String, _node_ids: Vec<String>, serializer: MessageSerializer<W>) -> Self {
        Self {
            serializer,
            store: KVStore::new(),
        }
    }

    fn process(&mut self, in_msg: InMessage<InPayload>) -> anyhow::Result<()>
    where
        Self: Sized,
    {
        let DeconstructedInMessage {
            partial_in_msg,
            in_payload,
        } = in_msg.into();
        let InPayload::Txn {
            txn: mut transactions,
        } = in_payload;
        self.store.apply_multi(&mut transactions);
        let payload = OutPayload::TxnOk { txn: &transactions };
        let mut out_msg = partial_in_msg.to_out_msg(payload);
        self.serializer
            .send(&mut out_msg)
            .context("failed to serialize txn_ok message")
    }

    fn shutdown(self) -> anyhow::Result<()> {
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let reader = std::io::stdin().lock();
    let writer = std::io::stdout();
    maelstrom::run_node::<TxnNode<_>, _, _, _>(reader, writer)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn deserialize_transaction() {
        assert_eq!(
            Transaction::Read {
                key: 1,
                value: None
            },
            serde_json::from_str(r#"["r", 1, null]"#).unwrap()
        );

        assert_eq!(
            Transaction::Read {
                key: 1,
                value: Some(2)
            },
            serde_json::from_str(r#"["r", 1, 2]"#).unwrap()
        );

        assert_eq!(
            Transaction::Write { key: 2, value: 7 },
            serde_json::from_str(r#"["w", 2, 7]"#).unwrap()
        );

        assert!(serde_json::from_str::<Transaction>(r#"["w", 2, null]"#).is_err());
    }

    #[test]
    fn serialize_transaction() {
        assert_eq!(
            serde_json::to_string(&Transaction::Read {
                key: 3,
                value: None
            })
            .unwrap(),
            r#"["r",3,null]"#.to_string()
        );

        assert_eq!(
            serde_json::to_string(&Transaction::Read {
                key: 6,
                value: Some(5)
            })
            .unwrap(),
            r#"["r",6,5]"#.to_string()
        );

        assert_eq!(
            serde_json::to_string(&Transaction::Write { key: 6, value: 5 }).unwrap(),
            r#"["w",6,5]"#.to_string()
        );
    }
}
