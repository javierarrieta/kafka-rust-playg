mod materializer;

use crate::materializer::{materialize, KafkaRecord, MaterlializeError, SideEffect};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use std::fmt::{Display, Formatter};
use std::string::String;

#[derive(Debug)]
struct Key {
    k: String,
}

impl Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.k)
    }
}

#[derive(Debug)]
struct Payload {
    p: String,
}

impl Display for Payload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.p)
    }
}

impl materializer::ByteDeserializer for Key {
    fn deserialize(payload: &[u8]) -> Result<Self, String> {
        let k: String = String::from_utf8_lossy(payload).to_string();
        Ok(Key { k })
    }
}

impl materializer::ByteDeserializer for Payload {
    fn deserialize(payload: &[u8]) -> Result<Self, String> {
        let p = String::from_utf8_lossy(payload).to_string();
        Ok(Payload { p })
    }
}

fn write_batch(records: Vec<KafkaRecord<Key, Payload>>) -> SideEffect {
    println!("{:?}", records);
    Ok(())
}

fn deadletter_handler(errors: Vec<MaterlializeError>) -> SideEffect {
    for e in errors {
        println!("Error: {:?}", e);
    }
    Ok(())
}

fn main() -> SideEffect {
    let consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_topic_partitions("quickstart".to_owned(), &[0])
        .with_fallback_offset(FetchOffset::Earliest)
        .with_group("my-group9".to_owned())
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()?;

    materialize(consumer, write_batch, deadletter_handler)
}
