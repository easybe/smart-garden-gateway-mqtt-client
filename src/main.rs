use tokio::{task, time};

use nng::options::protocol::pubsub::Subscribe;
use nng::options::Options;
use nng::{Protocol, Socket};
use rumqttc::Event::Incoming;
use rumqttc::Packet::Publish;
use rumqttc::{self, AsyncClient, EventLoop, MqttOptions, QoS};
use std::error::Error;
use std::time::Duration;

const LEMONBEATD_EVENT_URI: &str = "ipc:///tmp/lemonbeatd-event.ipc";
const LWM2MSERVER_EVENT_URI: &str = "ipc:///tmp/lwm2mserver-event.ipc";

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    loop {
        match mqtt_init().await {
            Ok((_, mut mqtt_event_loop)) => loop {
                let mqtt_event = mqtt_event_loop.poll().await;
                match &mqtt_event {
                    Ok(v) => {
                        if let Incoming(Publish(p)) = v {
                            println!("{:?}", p.payload);
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
    let mut mqtt_options = MqttOptions::new("test", "localhost", 1883);
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
    mqtt_client.subscribe("/test/in", QoS::AtMostOnce).await?;
    Ok((mqtt_client, mqtt_event_loop))
}

async fn mqtt_publisher(mqtt_client: AsyncClient, nng_sub: Socket) {
    loop {
        match nng_sub.try_recv() {
            Ok(data) => {
                let msg = String::from_utf8_lossy(&data);

                println!("NNG msg: {msg:?}");

                if let Err(e) = mqtt_client
                    .publish("/test/out", QoS::ExactlyOnce, false, msg.as_bytes())
                    .await
                {
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

fn nng_subscribe(uri: &str) -> Result<Socket, nng::Error> {
    let socket = Socket::new(Protocol::Sub0)?;
    socket.dial(uri)?;
    let all_topics = vec![];
    socket.set_opt::<Subscribe>(all_topics)?;
    Ok(socket)
}
