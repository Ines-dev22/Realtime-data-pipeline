# Real-Time Data Pipeline: E-Commerce Analytics

Ce projet implémente une architecture de pipeline de données en temps réel de bout en bout. L'objectif est de simuler des flux de commandes e-commerce à haute fréquence, de les diffuser via un message broker, puis de les consommer par batchs pour de l'analyse décisionnelle (BI).

Toute l'infrastructure est entièrement conteneurisée et déployable en une seule commande.



##  Architecture du Projet

Le pipeline est découpé en plusieurs briques technologiques optimisées pour la performance :

* **Producteur (Rust)** : Une application ultra-rapide développée en Rust (`rdkafka`) qui génère et pousse en continu des événements de commandes e-commerce au format JSON.
* **Message Broker (Redpanda)** : Une alternative moderne, performante et compatible avec l'écosystème Apache Kafka, chargée de distribuer les flux de données.
* **Consommateur & Transformation (Python / Pandas)** : Un script Python (`confluent-kafka`) qui consomme les messages en temps réel, regroupe les données par micro-batchs de 10 messages, calcule le montant total (`price * quantity`) et structure le tout via des DataFrames Pandas.
* **Stockage Analytique (DuckDB)** : Les données nettoyées et transformées sont persistées localement dans une base de données DuckDB, optimisée pour les requêtes analytiques et le requêtage colonnaire rapide.
* **Surveillance (Redpanda Console)** : Une interface web pour visualiser l'état du broker et inspecter les messages du topic en direct.

---

## Stack Technique

* **Languages** : Rust, Python
* **Streaming & Messaging** : Redpanda (Kafka API), `rdkafka`, `confluent-kafka`
* **Data Processing** : Pandas
* **Storage** : DuckDB (Format `.db` persistant)
* **DevOps & Infra** : Docker, Docker Compose

---

##  Lancement Rapide (Production-Ready)

### Prérequis
* Docker et Docker Compose installés sur votre machine.

### Déploiement de l'infrastructure
Pour compiler le producteur Rust, installer les dépendances Python et lancer tout l'écosystème, exécutez la commande suivante à la racine du projet :

```bash
docker compose up --build

