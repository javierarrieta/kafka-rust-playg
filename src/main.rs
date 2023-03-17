mod materializer;

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use std::string::String;
use crate::materializer::{MaterlializeError, materialize};

#[derive(Debug)]
struct Key {
    k: String,
}

impl Key {
    fn new(k: String) -> Self {
        Key { k }
    }
}

#[derive(Debug)]
struct Payload {
    p: String,
}

impl materializer::ByteDeserializer for Key {
    fn deserialize(payload: &[u8]) -> Result<Self, MaterlializeError> {
        let k: String = String::from_utf8_lossy(payload).to_string();
        return Ok(Key::new(k))
    }
}

impl materializer::ByteDeserializer for Payload {
    fn deserialize(payload: &[u8]) -> Result<Self, MaterlializeError> {
        let p = String::from_utf8_lossy(payload).to_string();
        return Ok(Payload{p})
    }
}

fn write(record: &materializer::KafkaRecord<Key, Payload>) -> Result<(), MaterlializeError> {
    println!("[{:?}]: {:?}", record.key, record.payload);
    return Ok(())
}

fn main() -> Result<(), MaterlializeError>{
    let consumer =
        Consumer::from_hosts(vec!("localhost:9092".to_owned()))
            .with_topic_partitions("quickstart".to_owned(), &[0])
            .with_fallback_offset(FetchOffset::Earliest)
            .with_group("my-group5".to_owned())
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()?;

    return materialize(consumer, write);
}
