mod materializer;

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use std::string::String;
use crate::materializer::{MaterlializeError, materialize, KafkaRecord};

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
    fn deserialize(payload: &[u8]) -> Result<Self, String> {
        let k: String = String::from_utf8_lossy(payload).to_string();
        return Ok(Key::new(k))
    }
}

impl materializer::ByteDeserializer for Payload {
    fn deserialize(payload: &[u8]) -> Result<Self, String> {
        let p = String::from_utf8_lossy(payload).to_string();
        return Ok(Payload{p})
    }
}

fn write_batch(records: Vec<KafkaRecord<Key, Payload>>) -> Result<(), MaterlializeError> {
    println!("{:?}", records);
    return Ok(())
}

fn deadletter_handler(errors: Vec<MaterlializeError>) -> Result<(), MaterlializeError> {
    for e in errors {
        println!("Error: {:?}", e);
    }
    Ok(())
}


fn main() -> Result<(), MaterlializeError>{
    let consumer =
        Consumer::from_hosts(vec!("localhost:9092".to_owned()))
            .with_topic_partitions("quickstart".to_owned(), &[0])
            .with_fallback_offset(FetchOffset::Earliest)
            .with_group("my-group9".to_owned())
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()?;

    return materialize(consumer, write_batch, deadletter_handler);
}
