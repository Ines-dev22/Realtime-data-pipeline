import json
import os
from confluent_kafka import Consumer, KafkaError
import pandas as pd

def main():
    # Configuration du consommateur
    broker = os.getenv("KAFKA_BROKERS", "localhost:19092")
    conf = {
        'bootstrap.servers': broker,
        'group.id': 'analytics-consumer-group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe(['ecommerce-orders'])

    print(f"🚀 Consommateur Python démarré. Écoute sur le broker : {broker}")

    buffer = []
    batch_size = 5  # On traite les données par paquets de 5 messages

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Erreur Kafka : {msg.error()}")
                    break

            # Déserialisation du message JSON
            data = json.loads(msg.value().decode('utf-8'))
            buffer.append(data)

            # Quand le batch est plein, on passe dans Pandas
            if len(buffer) >= batch_size:
                df = pd.DataFrame(buffer)
                
                # --- TRAITEMENT / ANALYSE (Exemple simple) ---
                # On calcule le montant total de chaque ligne (prix * quantité)
                df['total_amount'] = df['price'] * df['quantity']
                
                print("\n📊 --- Nouveau Batch Traité par Pandas ---")
                print(df[['order_id', 'product_id', 'total_amount']])
                print("-------------------------------------------\n")
                
                # On vide le buffer pour le prochain batch
                buffer = []

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == '__main__':
    main()