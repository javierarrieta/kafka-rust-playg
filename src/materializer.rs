use kafka::consumer::{Consumer, Message};
use thiserror::Error;

pub struct KafkaRecord<K,P> {
    pub key: K,
    pub payload: P,
}

impl<K, P> KafkaRecord<K, P> {
    pub fn new(k: K, p: P) -> KafkaRecord<K, P> {
        return KafkaRecord{ key: k, payload: p }
    }
}

#[derive(Error, Debug)]
pub enum MaterlializeError {
    #[error("Error deserializing: {0}")]
    DeserializeError(String),

    #[error["IOError: {0}"]]
    IOError(String),
}

impl From<kafka::Error> for MaterlializeError {
    fn from(value: kafka::Error) -> Self {
        MaterlializeError::IOError(value.to_string())
    }
}

pub trait ByteDeserializer : Sized {
    fn deserialize(payload: &[u8]) -> Result<Self, MaterlializeError>;
}

impl ByteDeserializer for String {
    fn deserialize(payload: &[u8]) -> Result<Self, MaterlializeError> {
        return Ok(String::from_utf8_lossy(payload).to_string())
    }
}

pub trait RecordWriter<Key, Payload>: Sized {
    fn write(record: &KafkaRecord<Key, Payload>) -> Result<(), MaterlializeError>;
}

fn decode<Key: ByteDeserializer, Payload: ByteDeserializer>(msg: &Message) -> Result<KafkaRecord<Key, Payload>, MaterlializeError> {
    let key: Key = Key::deserialize(msg.key)?;
    let payload: Payload = Payload::deserialize(msg.value)?;
    return Ok(KafkaRecord::new(key, payload))
}

pub fn materialize<Key: ByteDeserializer, Payload: ByteDeserializer>(
        mut consumer: Consumer, write: fn(&KafkaRecord<Key, Payload>
    ) -> Result<(), MaterlializeError>) -> Result<(), MaterlializeError> {
    loop {
        for ms in consumer.poll()?.iter() {
            for m in ms.messages() {
                let record: KafkaRecord<Key, Payload> = decode(m)?;
                write(&record)?;
            }
            consumer.consume_messageset(ms)?;
        }
        consumer.commit_consumed()?;
    }
}