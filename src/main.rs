use tokio::{task, time};

use rumqttc::Event::Incoming;
use rumqttc::Packet::Publish;
use rumqttc::{self, AsyncClient, MqttOptions, QoS};
use std::error::Error;
use std::time::Duration;

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut mqtt_options = MqttOptions::new("test", "localhost", 1883);
    mqtt_options.set_keep_alive(Duration::from_secs(5));
    mqtt_options.set_clean_session(false);

    let (mqtt_client, mut mqtt_event_loop) = AsyncClient::new(mqtt_options, 10);

    mqtt_client
        .subscribe("/test/in", QoS::AtMostOnce)
        .await
        .unwrap();

    task::spawn(async move {
        mqtt_publisher(mqtt_client).await;
    });

    loop {
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
    }
}

async fn mqtt_publisher(mqtt_client: AsyncClient) {
    for i in 1..=10 {
        mqtt_client
            .publish("/test/out", QoS::ExactlyOnce, false, format!("Hello {}", i))
            .await
            .unwrap();

        time::sleep(Duration::from_secs(10)).await;
    }
}
