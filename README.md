# Dinkisa_Worku_Thesis_2025_files

# NoSQL Databases in Stream Data Processing for Real-Time Anomaly Detection in Payment Card Transactions

This repository contains the supplementary materials for the MSc thesis by **Dinkisa Demeke Worku** (Warsaw University of Technology, 2025).  
The project demonstrates a real-time fraud detection pipeline using **Apache Kafka, Apache Flink, MongoDB, and PostgreSQL**, with monitoring provided by **Prometheus** and **Grafana**.

---

## 📂 Repository Contents

- **Simulation_transactions.py** – Python simulator that generates synthetic payment card transactions (normal + anomalous) and publishes them to Kafka.
- **fraud_detection_flink.py** – PyFlink job that consumes Kafka transactions, applies anomaly detection, and writes results to MongoDB and PostgreSQL.
- **Kafka_to_Mongo_integration.py** – Kafka consumer that writes transactions into MongoDB.
- **Kafka_to_postgresql_integ.py** – Kafka consumer that writes transactions into PostgreSQL.
- **Accuracy_evaluation.py** – Script to compute precision, recall, and F1-score using ground-truth vs predicted anomalies.
- **prometheus_grafana_config/** – Example configuration files for Prometheus and Grafana dashboards.
- **requirements.txt** – Python dependencies required to run the pipeline.
- **README.md** – This documentation.

---

## 🔧 System Requirements

To reproduce the experiments, the following services must be installed and running:

- **Apache Kafka** (>= 3.6.0) – for message ingestion
- **Apache Flink** (>= 1.17.2) – for stream processing
- **MongoDB** (>= 6.0) – NoSQL database for anomaly storage
- **PostgreSQL** (>= 14) – relational database for comparison
- **Prometheus** (>= 2.50.0) – system metrics collection
- **Grafana** (>= 10.0.0) – visualization dashboards

**Recommended setup:**
- OS: Ubuntu 20.04 / 22.04
- RAM: 8 GB minimum
- Java: OpenJDK 11 or later (required for Flink & Kafka)
- Python: 3.9+

---

## 📦 Python Dependencies

Install Python dependencies from `requirements.txt`:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
````

Minimal dependencies include:

* apache-flink
* kafka-python
* pymongo
* psycopg2-binary
* pandas, numpy
* scikit-learn
* matplotlib
* prometheus-client

---

## 🚀 How to Run

### 1. Start Services

#### Kafka

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties &
bin/kafka-server-start.sh config/server.properties &
bin/kafka-topics.sh --create --topic transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

#### Flink

```bash
./bin/start-cluster.sh
```

#### MongoDB

```bash
sudo systemctl start mongod
```

#### PostgreSQL

```bash
sudo systemctl start postgresql
```

#### Prometheus

```bash
./prometheus --config.file=prometheus.yml
```

#### Grafana

Grafana can be started via system service or Docker:

```bash
sudo systemctl start grafana-server
```

---

### 2. Run the Pipeline

1. **Generate transactions**

   ```bash
   python Simulation_transactions.py
   ```

2. **Run Flink anomaly detection**

   ```bash
   flink run -py fraud_detection_flink.py
   ```

3. **(Optional) Direct database consumers**

   ```bash
   python Kafka_to_Mongo_integration.py
   python Kafka_to_postgresql_integ.py
   ```

4. **Evaluate accuracy**

   ```bash
   python Accuracy_evaluation.py
   ```

---

## 📊 Monitoring

The repository includes example configs for monitoring with Prometheus and Grafana:

* **Prometheus** scrapes metrics from Kafka, Flink, MongoDB, and PostgreSQL exporters.
* **Grafana** dashboards visualize:

  * Throughput
  * Latency
  * Resource utilization
  * Anomaly counts

To use:

* Import the provided Grafana JSON dashboard (`prometheus_grafana_config/grafana_dashboard.json`).
* Start Prometheus with the provided config (`prometheus_grafana_config/prometheus.yml`).

---

## 📖 Thesis Reference

This repository is supplementary to the MSc Thesis:

*NoSQL Databases in Stream Data Processing for Real-Time Anomaly Detection in Payment Card Transactions*
Warsaw University of Technology, Faculty of Electronics and Information Technology, 2025.

---

## 📧 Contact

For questions regarding these materials:
**Dinkisa Demeke Worku**
[GitHub Profile](https://github.com/Dinku143)

```

---

✅ This README covers:  
- Project overview  
- Repo contents  
- System requirements (Kafka, Flink, MongoDB, PostgreSQL, Prometheus, Grafana)  
- Python dependencies (`requirements.txt`)  
- How to start services + run pipeline  
- Monitoring with Grafana/Prometheus  
- Thesis reference + contact  

 
