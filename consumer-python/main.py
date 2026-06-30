import json
import os
from confluent_kafka import Consumer, KafkaError
import pandas as pd
import duckdb

def main():
    # Configuration du chemin de la base DuckDB (dans le volume partagé Docker)
    db_path = "/app/data/ecommerce_analytics.db"
    
    # Initialisation de la connexion DuckDB
    con = duckdb.connect(db_path)
    print(f"Base DuckDB initialisée à l'emplacement : {db_path}")
    
    # Configuration du consommateur Kafka
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
    batch_size = 10  # On traite les données par paquets de 10 messages

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
                
                # Transformation rapide : calcul du montant total
                df['total_amount'] = df['price'] * df['quantity']
                # Conversion du timestamp en type datetime pour DuckDB
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                
                # Insertion directe du DataFrame Pandas dans DuckDB
                # 'append=True' crée la table si elle n'existe pas, sinon ajoute les lignes
                con.execute("INSERT INTO orders SELECT * FROM df") if con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'").fetchone() else con.execute("CREATE TABLE orders AS SELECT * FROM df")
                
                # Petit log de contrôle
                row_count = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
                total_revenue = con.execute("SELECT SUM(total_amount) FROM orders").fetchone()[0]
                
                print(f"📊 [DuckDB] Batch inséré. Total lignes en base : {row_count} | Chiffre d'Affaires Cumulé : {total_revenue:.2f}€")
                # On vide le buffer pour le prochain batch
                buffer = []

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == '__main__':
    main()