# RabbitMQ & Kafka: Messaging Patterns in .NET

A comprehensive implementation and comparison of **RabbitMQ** and **Apache Kafka** using .NET. This project demonstrates how to build resilient, scalable, and decoupled microservices using industry-standard message brokers.

---

## 📖 Project Purpose
The goal of this project is to provide a side-by-side implementation of two different messaging paradigms:
* **RabbitMQ:** Best for complex routing, task distribution, and traditional request/reply patterns.
* **Apache Kafka:** Best for high-throughput event streaming, log-keeping, and real-time data processing.



## 🛠️ Tech Stack
* **Runtime:** .NET 8 / .NET Core
* **Brokers:** RabbitMQ (Management Plugin enabled) & Apache Kafka (Zookeeper/Kraft)
* **Infrastructure:** Docker & Docker Compose
* **Client Libraries:** `RabbitMQ.Client`, `Confluent.Kafka`

---

## 🏗️ Architecture & Flow
1.  **Producers:** Send messages/events containing data (e.g., JSON payloads).
2.  **Exchange/Topic:** The entry point in the broker where data is categorized.
3.  **Consumers:** Background workers that "listen" and process the incoming data.



## 🚀 Getting Started

### 1. Prerequisites
* Install [Docker Desktop](https://www.docker.com/products/docker-desktop/).
* Install [.NET SDK](https://dotnet.microsoft.com/download).

### 2. Launch the Brokers
Run the following command in the root folder to start the containers:
```bash
docker-compose up -d

RabbitMQ Management UI: http://localhost:15672 (Guest/Guest)

Kafka Bootstrap Server: localhost:9092

3. Run the Projects
Open the solution (.sln) in Visual Studio.

Set Multiple Startup Projects: Start both a Producer and a Consumer.

Watch the console windows to see messages being sent and received.

Feature	RabbitMQ	Apache Kafka	
Model	Smart Broker / Dumb Consumer	Dumb Broker / Smart Consumer	
Storage	Messages deleted after consumption	Messages retained (Log-based)	
Best Use Case	Work Queues, RPC	Event Sourcing, Data Streaming	
Scaling	Vertical & Horizontal	Highly Horizontal (Partitions)	

Follow the "MeterSender\README.md" for more details. 