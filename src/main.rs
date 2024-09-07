use tokio::{task, time};

use config::Config;
use env_logger::Env;
use log::{debug, error, info};
use nix::unistd::gethostname;
use nng::options::protocol::pubsub::Subscribe;
use nng::options::Options;
use nng::{Protocol, Socket};
use rumqttc::Event::Incoming;
use rumqttc::Packet::Publish;
use rumqttc::{self, AsyncClient, EventLoop, MqttOptions, QoS};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

const CONFIG_PATH: &str = "/etc/sg_mqtt_client.conf";
const LEMONBEATD_COMMAND_URI: &str = "ipc:///tmp/lemonbeatd-command.ipc";
const LEMONBEATD_EVENT_URI: &str = "ipc:///tmp/lemonbeatd-event.ipc";
const LWM2MSERVER_COMMAND_URI: &str = "ipc:///tmp/lwm2mserver-command.ipc";
const LWM2MSERVER_EVENT_URI: &str = "ipc:///tmp/lwm2mserver-event.ipc";

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
    pub fn new(
        topic: &str,
        payload: &str,
        topic_base: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mqtt_path = topic
            .strip_prefix(&format!("{topic_base}/"))
            .ok_or(Error::BadMqttTopic)?;
        let parts = mqtt_path.split('/').collect::<Vec<&str>>();
        let device = (*parts.first().ok_or(Error::BadMqttTopic)?).to_string();
        let nng_path = parts.get(1..).ok_or(Error::BadMqttTopic)?.join("/");
        let nng_payload = serde_json::from_str(payload)?;
        Ok(NngMsg {
            entity: NngMsgEntity {
                device,
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
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let config = load_config()?;

    let nng_req0 = NngReq0 {
        lemonbeatd: nng_connect_req0(LEMONBEATD_COMMAND_URI)?,
        lwm2mserver: nng_connect_req0(LWM2MSERVER_COMMAND_URI)?,
    };

    loop {
        match mqtt_init(&config).await {
            Ok((_, mut mqtt_event_loop)) => loop {
                let mqtt_event = mqtt_event_loop.poll().await;
                match &mqtt_event {
                    Ok(v) => {
                        if let Incoming(Publish(p)) = v {
                            let topic = &p.topic;
                            let payload = String::from_utf8_lossy(&p.payload);
                            if topic.starts_with(&config.get::<String>("mqtt_topics.command")?) {
                                debug!("{topic}: {payload}");
                                if let Err(e) =
                                    mqtt_handle_command(topic, &payload, &nng_req0, &config)
                                {
                                    error!("Failed to handle MQTT command: {e:?}");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("MQTT error: {e:?}");
                        time::sleep(Duration::from_secs(30)).await;
                    }
                }
            },
            Err(e) => {
                error!("{e:?}");
                time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}

fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    let mut conf_builder = Config::builder()
        .set_default("mqtt_broker.host", "localhost")?
        .set_default("mqtt_broker.port", 1883)?
        .set_default("mqtt_topics.command", "smart-garden/command")?
        .set_default("mqtt_topics.event", "smart-garden/event")?;
    let config_file = Path::new(CONFIG_PATH);
    if config_file.exists() {
        conf_builder = conf_builder
            .add_source(config::File::with_name(CONFIG_PATH).format(config::FileFormat::Ini));
    }
    // Search for config file in current directory
    let config_file_name = config_file.file_name().unwrap();
    if Path::new(config_file_name).exists() {
        conf_builder = conf_builder.add_source(
            config::File::with_name(config_file_name.to_str().unwrap())
                .format(config::FileFormat::Ini),
        );
    }
    conf_builder =
        conf_builder.add_source(config::Environment::with_prefix("SG_MQTT_CLIENT").separator("__"));

    let config = conf_builder.build()?;
    Ok(config)
}

async fn mqtt_init(
    config: &Config,
) -> Result<(AsyncClient, EventLoop), Box<dyn std::error::Error>> {
    let hn = gethostname()?;
    let hostname = hn.to_str().ok_or(Error::BadHostname)?;
    let broker_host = config.get::<String>("mqtt_broker.host")?;
    let broker_port = config.get::<u16>("mqtt_broker.port")?;

    info!("Connecting to MQTT broker: {broker_host}:{broker_port}");

    let mut mqtt_options = MqttOptions::new(hostname, broker_host, broker_port);
    mqtt_options.set_keep_alive(Duration::from_secs(5));
    mqtt_options.set_clean_session(false);

    let (mqtt_client, mqtt_event_loop) = AsyncClient::new(mqtt_options, 10);
    {
        let sub = nng_subscribe(LEMONBEATD_EVENT_URI)?;
        task::spawn({
            let mc = mqtt_client.clone();
            let c = config.clone();
            async move {
                mqtt_publisher(mc, sub, c).await;
            }
        });
    }
    {
        let sub = nng_subscribe(LWM2MSERVER_EVENT_URI)?;
        task::spawn({
            let mc = mqtt_client.clone();
            let c = config.clone();
            async move {
                mqtt_publisher(mc, sub, c).await;
            }
        });
    }
    mqtt_client
        .subscribe(
            format!("{}/#", config.get::<String>("mqtt_topics.command")?),
            QoS::AtMostOnce,
        )
        .await?;
    Ok((mqtt_client, mqtt_event_loop))
}

async fn mqtt_publisher(mqtt_client: AsyncClient, nng_sub: Socket, config: Config) {
    loop {
        match nng_sub.try_recv() {
            Ok(data) => {
                if let Err(e) = mqtt_publish(&mqtt_client, data, &config).await {
                    error!("Failed to publish MQTT message: {e:?}");
                }
            }
            Err(e) => match e {
                nng::Error::TryAgain => {}
                _ => {
                    error!("NNG error: {e:?}");
                }
            },
        }
        time::sleep(Duration::from_millis(100)).await;
    }
}

async fn mqtt_publish(
    mqtt_client: &AsyncClient,
    data: nng::Message,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let nng_msg_str = String::from_utf8_lossy(&data);

    debug!("NNG message: {nng_msg_str}");

    let nng_msgs = serde_json::from_str::<Vec<NngMsg>>(&nng_msg_str)?;
    let nng_msg = nng_msgs.first().ok_or(Error::BadNngMsg)?;
    let device = &nng_msg.entity.device;
    let path = &nng_msg.entity.path;
    let payload = serde_json::to_string(&nng_msg.payload)?;
    let topic_base = config.get::<String>("mqtt_topics.event")?;
    mqtt_client
        .publish(
            format!("{topic_base}/{device}/{path}"),
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
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    let topic_base = config.get::<String>("mqtt_topics.command")?;
    let nng_msg = NngMsg::new(topic, payload, &topic_base)?;
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
        .first()
        .ok_or(Error::BadNngMsg)?
        .success
        .ok_or(Error::BadNngMsg)?
    {
        return Err(Box::new(Error::CommandFailed));
    }
    Ok(())
}
