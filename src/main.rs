use tokio::{task, time};

use nix::unistd::gethostname;
use nng::options::protocol::pubsub::Subscribe;
use nng::options::Options;
use nng::{Protocol, Socket};
use rumqttc::Event::Incoming;
use rumqttc::Packet::Publish;
use rumqttc::{self, AsyncClient, EventLoop, MqttOptions, QoS};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

const LEMONBEATD_COMMAND_URI: &str = "ipc:///tmp/lemonbeatd-command.ipc";
const LEMONBEATD_EVENT_URI: &str = "ipc:///tmp/lemonbeatd-event.ipc";
const LWM2MSERVER_COMMAND_URI: &str = "ipc:///tmp/lwm2mserver-command.ipc";
const LWM2MSERVER_EVENT_URI: &str = "ipc:///tmp/lwm2mserver-event.ipc";

const MQTT_TOPIC_COMMAND: &str = "smart-garden/command";
const MQTT_TOPIC_EVENT: &str = "smart-garden/event";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("MQTT topic invalid")]
    BadMqttTopic,
    #[error("Invalid NNG message received")]
    BadNngMsg,
    #[error("Failed to parse hostname")]
    BadHostname,
    #[error("Failed to executed command")]
    CommandFailed,
}

struct NngReq0 {
    lemonbeatd: Socket,
    lwm2mserver: Socket,
}

#[derive(Debug, Deserialize, Serialize)]
struct NngMsgEntity {
    device: String,
    path: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct NngMsg {
    entity: NngMsgEntity,
    metadata: Option<HashMap<String, serde_json::Value>>,
    op: Option<String>,
    payload: Option<HashMap<String, serde_json::Value>>,
    success: Option<bool>,
}

impl NngMsg {
    pub fn new(topic: &str, payload: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let mqtt_path = topic
            .strip_prefix(&format!("{MQTT_TOPIC_COMMAND}/"))
            .ok_or(Error::BadMqttTopic)?;
        let parts = mqtt_path.split("/").collect::<Vec<&str>>();
        let device = parts.get(0).ok_or(Error::BadMqttTopic)?.to_string();
        let nng_path = parts.get(1..).ok_or(Error::BadMqttTopic)?.join("/");
        let nng_payload = serde_json::from_str(payload)?;
        Ok(NngMsg {
            entity: NngMsgEntity {
                device: device,
                path: nng_path,
            },
            op: Some(String::from("write")),
            payload: nng_payload,
            metadata: None,
            success: None,
        })
    }
}

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nng_req0 = NngReq0 {
        lemonbeatd: nng_connect_req0(LEMONBEATD_COMMAND_URI)?,
        lwm2mserver: nng_connect_req0(LWM2MSERVER_COMMAND_URI)?,
    };

    loop {
        match mqtt_init().await {
            Ok((_, mut mqtt_event_loop)) => loop {
                let mqtt_event = mqtt_event_loop.poll().await;
                match &mqtt_event {
                    Ok(v) => {
                        if let Incoming(Publish(p)) = v {
                            let topic = &p.topic;
                            let payload = String::from_utf8_lossy(&p.payload);
                            if topic.starts_with(MQTT_TOPIC_COMMAND) {
                                println!("{topic}: {payload}");
                                if let Err(e) = mqtt_handle_command(&topic, &payload, &nng_req0) {
                                    println!("Failed to handle MQTT command: {e:?}");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("MQTT error: {e:?}");
                        time::sleep(Duration::from_secs(30)).await;
                    }
                }
            },
            Err(e) => {
                println!("{e:?}");
                time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}

async fn mqtt_init() -> Result<(AsyncClient, EventLoop), Box<dyn std::error::Error>> {
    let hn = gethostname()?;
    let hostname = hn.to_str().ok_or(Error::BadHostname)?;
    let mut mqtt_options = MqttOptions::new(hostname, "localhost", 1883);
    mqtt_options.set_keep_alive(Duration::from_secs(5));
    mqtt_options.set_clean_session(false);

    let (mqtt_client, mqtt_event_loop) = AsyncClient::new(mqtt_options, 10);
    {
        let sub = nng_subscribe(LEMONBEATD_EVENT_URI)?;
        task::spawn({
            let c = mqtt_client.clone();
            async move {
                mqtt_publisher(c, sub).await;
            }
        });
    }
    {
        let sub = nng_subscribe(LWM2MSERVER_EVENT_URI)?;
        task::spawn({
            let c = mqtt_client.clone();
            async move {
                mqtt_publisher(c, sub).await;
            }
        });
    }
    mqtt_client
        .subscribe(format!("{MQTT_TOPIC_COMMAND}/#"), QoS::AtMostOnce)
        .await?;
    Ok((mqtt_client, mqtt_event_loop))
}

async fn mqtt_publisher(mqtt_client: AsyncClient, nng_sub: Socket) {
    loop {
        match nng_sub.try_recv() {
            Ok(data) => {
                if let Err(e) = mqtt_publish(&mqtt_client, data).await {
                    println!("Failed to publish MQTT message: {e:?}");
                }
            }
            Err(e) => match e {
                nng::Error::TryAgain => {}
                _ => {
                    println!("NNG error: {e:?}");
                }
            },
        }
        time::sleep(Duration::from_millis(100)).await;
    }
}

async fn mqtt_publish(
    mqtt_client: &AsyncClient,
    data: nng::Message,
) -> Result<(), Box<dyn std::error::Error>> {
    let nng_msg_str = String::from_utf8_lossy(&data);

    println!("NNG message: {nng_msg_str}");

    let nng_msgs = serde_json::from_str::<Vec<NngMsg>>(&nng_msg_str)?;
    let nng_msg = nng_msgs.get(0).ok_or(Error::BadNngMsg)?;
    let device = &nng_msg.entity.device;
    let path = &nng_msg.entity.path;
    let payload = serde_json::to_string(&nng_msg.payload)?;
    mqtt_client
        .publish(
            format!("{MQTT_TOPIC_EVENT}/{device}/{path}"),
            QoS::ExactlyOnce,
            false,
            payload.as_bytes(),
        )
        .await?;
    Ok(())
}

fn mqtt_handle_command(
    topic: &str,
    payload: &str,
    nng_req0: &NngReq0,
) -> Result<(), Box<dyn std::error::Error>> {
    let nng_msg = NngMsg::new(&topic, &payload)?;
    if nng_msg.entity.path.contains("lemonbeat") {
        nng_send(&nng_req0.lemonbeatd, &nng_msg)?;
    } else {
        nng_send(&nng_req0.lwm2mserver, &nng_msg)?;
    }
    Ok(())
}

fn nng_subscribe(uri: &str) -> Result<Socket, nng::Error> {
    let socket = Socket::new(Protocol::Sub0)?;
    socket.dial(uri)?;
    let all_topics = vec![];
    socket.set_opt::<Subscribe>(all_topics)?;
    Ok(socket)
}

fn nng_connect_req0(uri: &str) -> Result<Socket, nng::Error> {
    let socket = Socket::new(Protocol::Req0)?;
    socket.dial(uri)?;
    Ok(socket)
}

fn nng_send(socket: &Socket, msg: &NngMsg) -> Result<(), Box<dyn std::error::Error>> {
    let msgs = [msg];
    if let Err((_, e)) = socket.send(serde_json::to_string(&msgs)?.as_bytes()) {
        return Err(Box::new(e));
    }
    let resp = socket.recv()?;
    let resp_str = String::from_utf8_lossy(resp.as_slice());
    let nng_msgs = serde_json::from_str::<Vec<NngMsg>>(&resp_str)?;
    if !nng_msgs
        .get(0)
        .ok_or(Error::BadNngMsg)?
        .success
        .ok_or(Error::BadNngMsg)?
    {
        return Err(Box::new(Error::CommandFailed));
    }
    Ok(())
}
