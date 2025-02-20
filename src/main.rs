use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
};

use model::Message;
use runner::runner_server::{Runner, RunnerServer};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};

pub mod model;
pub mod runner {
    tonic::include_proto!("rdfc");
}

#[derive(Debug, Default)]
pub struct MyRunner {}

#[tonic::async_trait]
impl Runner for MyRunner {
    async fn load(&self, request: Request<runner::Stage>) -> Result<Response<()>, Status> {
        let stage = request.into_inner();
        println!("Loading stage: {:?}", stage);
        Ok(Response::new(()))
    }

    type execStream = ReceiverStream<Result<runner::ChannelMessage, Status>>;

    async fn exec(
        &self,
        request: tonic::Request<tonic::Streaming<runner::ChannelMessage>>,
    ) -> Result<Response<Self::execStream>, Status> {
        let mut stream = request.into_inner();

        let (tx, rx) = mpsc::channel(10);
        tokio::spawn(async move {
            while let Ok(Some(message)) = stream.message().await {
                let my_message: Result<Message, _> = message.try_into();
                if let Ok(Message::Data { uri, data }) = my_message {
                    println!("Got message from {:?} with {}", uri, unsafe {
                        std::str::from_utf8_unchecked(&data)
                    });

                    let channel_message: runner::ChannelMessage =
                        Message::Data { uri, data }.into();
                    let _ = tx.send(Ok(channel_message)).await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let mut host = args.get(1).map(|x| x.as_str()).unwrap_or("0.0.0.0");
    if host == "localhost" {
        host = "127.0.0.1";
    }
    let port = args.get(2).map(|x| x.as_str()).unwrap_or("50051");

    println!("Starting on {}:{}", host, port);

    let addr = SocketAddr::from_str(&format!("{}:{}", host, port))?;
    let runner = MyRunner::default();

    Server::builder()
        .add_service(RunnerServer::new(runner))
        .serve(addr)
        .await?;

    Ok(())
}
