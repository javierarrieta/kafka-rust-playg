use kafka::consumer::{Consumer, Message};
use thiserror::Error;
use itertools::{Itertools, Either};

#[derive(Debug)] //FIXME: Remove
pub struct KafkaRecord<K: Sized,P: Sized> {
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
    #[error("Error deserializing offset: {0}, {1}")]
    DeserializeError(i64, String),

    #[error["IOError: {0}"]]
    IOError(String),
}

impl From<kafka::Error> for MaterlializeError {
    fn from(value: kafka::Error) -> Self {
        MaterlializeError::IOError(value.to_string())
    }
}

pub trait ByteDeserializer : Sized {
    fn deserialize(payload: &[u8]) -> Result<Self, String>;
}

impl ByteDeserializer for String {
    fn deserialize(payload: &[u8]) -> Result<Self, String> {
        return Ok(String::from_utf8_lossy(payload).to_string())
    }
}

// pub trait RecordWriter<Key, Payload>: Sized {
//     fn write(record: &[&KafkaRecord<Key, Payload>]) -> Result<(), MaterlializeError>;
// }

fn decode<Key: ByteDeserializer, Payload: ByteDeserializer>(msg: &Message) -> Result<KafkaRecord<Key, Payload>, MaterlializeError> {
    let key: Key = Key::deserialize(msg.key)
        .map_err(|e| MaterlializeError::DeserializeError(msg.offset, e))?;
    let payload: Payload = Payload::deserialize(msg.value)
        .map_err(|e| MaterlializeError::DeserializeError(msg.offset, e))?;
    return Ok(KafkaRecord::new(key, payload))
}

//Fixme: This should be automatically converted, see https://docs.rs/itertools/latest/itertools/enum.Either.html#impl-Into%3CResult%3CR%2C%20L%3E%3E-for-Either%3CL%2C%20R%3E
fn decode_to_either<Key: ByteDeserializer, Payload: ByteDeserializer>(msg: &Message) -> Either<MaterlializeError, KafkaRecord<Key, Payload>> {
    match decode(msg) {
        Ok(r) => Either::Right(r),
        Err(e) => Either::Left(e)
    }
}

pub fn materialize<Key: ByteDeserializer, Payload: ByteDeserializer>(
    mut consumer: Consumer,
    write_batch: fn(Vec<KafkaRecord<Key, Payload>>) -> Result<(), MaterlializeError>,
    deadletter_handler: fn(Vec<MaterlializeError>) -> Result<(), MaterlializeError>
) -> Result<(), MaterlializeError> {
    println!("Starting");
    loop {
        println!("Polling...");
        for ms in consumer.poll()?.iter() {
            println!("Consuming batch from topic {}, partition {}, size {}",
                     ms.topic(), ms.partition(), ms.messages().len());
            let (errors, records): (Vec<_>, Vec<_>) =
                ms.messages().into_iter().partition_map( decode_to_either);
            write_batch(records)?;
            deadletter_handler(errors)?;

            consumer.consume_messageset(ms)?;
        }
        consumer.commit_consumed()?;
    }
}