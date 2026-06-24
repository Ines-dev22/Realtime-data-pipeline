use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use std::time::Duration;
use chrono::Utc;
use rand::Rng;

#[derive(Serialize)]
struct OrderEvent {
    order_id: String,
    user_id: u32,
    product_id: String,
    price: f64,
    quantity: u32,
    timestamp: String,
}

fn generate_fake_order() -> OrderEvent {
    let mut rng = rand::thread_rng();
    let products = vec!["laptop", "smartphone", "headphones", "keyboard", "monitor"];
    
    OrderEvent {
        order_id: format!("ORD-{}", rng.gen_range(100000..999999)),
        user_id: rng.gen_range(1000..9999),
        product_id: products[rng.gen_range(0..products.len())].to_string(),
        price: rng.gen_range(15.0..1200.0),
        quantity: rng.gen_range(1..4),
        timestamp: Utc::now().to_rfc3339(),
    }
}

#[tokio::main]
async fn main() {
    // En local, on vise localhost:19092 (port externe exposé par Docker)
    let broker = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:19092".to_string());
    
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &broker)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Erreur lors de la création du producteur Kafka");

    println!("🚀 Producteur Rust démarré. Connexion à: {}", broker);

    loop {
        let event = generate_fake_order();
        let payload = serde_json::to_string(&event).unwrap();

        let record = FutureRecord::to("ecommerce-orders")
            .key(&event.order_id)
            .payload(&payload);

        match producer.send(record, Duration::from_secs(0)).await {
            Ok(delivery) => println!("✅ Message envoyé avec succès: {:?}", delivery),
            Err((e, _)) => eprintln!("❌ Échec de l'envoi du message: {:?}", e),
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}