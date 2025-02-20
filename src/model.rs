use crate::runner;
use std::collections::HashMap;

#[derive(Debug)]
pub struct Processor {
    pub uri: String,
    pub entrypoint: String,
    pub metadata: HashMap<String, String>,
}

impl From<runner::Processor> for Processor {
    fn from(value: runner::Processor) -> Self {
        Self {
            uri: value.uri,
            entrypoint: value.entrypoint,
            metadata: value.metadata,
        }
    }
}

#[derive(Debug)]
pub struct Stage {
    pub uri: String,
    pub processor: Processor,
    pub arguments: HashMap<String, serde_json::Value>,
}

impl TryFrom<runner::Stage> for Stage {
    type Error = ();

    fn try_from(value: runner::Stage) -> Result<Self, Self::Error> {
        let map = HashMap::new();
        // todo: fill map
        Ok(Self {
            uri: value.uri,
            arguments: map,
            processor: value.processor.ok_or(())?.into(),
        })
    }
}

#[derive(Debug)]
pub struct Channel {
    pub uri: String,
}
impl From<runner::Channel> for Channel {
    fn from(value: runner::Channel) -> Self {
        Self { uri: value.uri }
    }
}
impl Into<runner::Channel> for Channel {
    fn into(self) -> runner::Channel {
        runner::Channel { uri: self.uri }
    }
}

#[derive(Debug)]
pub enum Message {
    Data { uri: Channel, data: Vec<u8> },
    Close { uri: Channel },
}

#[derive(Debug)]
pub enum FromMessageError {
    UnknownType(i32),
    NoUriPresent,
    ExpectedData,
}

impl TryFrom<runner::ChannelMessage> for Message {
    type Error = FromMessageError;

    fn try_from(value: runner::ChannelMessage) -> Result<Self, Self::Error> {
        let uri = value.channel.ok_or(FromMessageError::NoUriPresent)?;
        if value.r#type == runner::ChannelMessageType::Data as i32 {
            let data = value.data.ok_or(FromMessageError::ExpectedData)?;
            return Ok(Self::Data {
                uri: uri.into(),
                data: data.bytes,
            });
        }

        if value.r#type == runner::ChannelMessageType::Close as i32 {
            return Ok(Self::Close { uri: uri.into() });
        }

        Err(FromMessageError::UnknownType(value.r#type))
    }
}

impl Into<runner::ChannelMessage> for Message {
    fn into(self) -> runner::ChannelMessage {
        match self {
            Message::Data { uri, data } => runner::ChannelMessage {
                data: Some(runner::ChannelData { bytes: data }),
                r#type: runner::ChannelMessageType::Data as i32,
                channel: Some(uri.into()),
            },
            Message::Close { uri } => runner::ChannelMessage {
                r#type: runner::ChannelMessageType::Data as i32,
                data: None,
                channel: Some(uri.into()),
            },
        }
    }
}
