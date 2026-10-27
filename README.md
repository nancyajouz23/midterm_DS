

\# Distributed Systems Midterm



\## Project Overview

Environment setup and practical exercises for the Distributed Systems midterm.



\## Technologies

\- Python 3.11.9

\- Git and GitHub

\- Docker Desktop

\- Kubernetes (kind, v1.31.1)

\- RabbitMQ

\- Apache Kafka 4.4.0

\- Prometheus

\- Grafana



\## Environment Setup



\### Python Virtual Environment

Create:

```powershell

python -m venv .venv

```



Activate:

```powershell

.\\.venv\\Scripts\\Activate.ps1

```



\### Docker Services

Start existing containers:

```powershell

docker start ds-midterm-rabbitmq ds-midterm-kafka ds-midterm-prometheus ds-midterm-grafana

```



Check running containers:

```powershell

docker ps

```



\### Kubernetes

Check cluster status:

```powershell

kubectl get nodes

kubectl get pods

```



\## Service Ports



| Service | Port |

|---------|------|

| RabbitMQ Messaging | 5672 |

| RabbitMQ Management | 15672 |

| Kafka | 9092 |

| Prometheus | 9090 |

| Grafana | 3000 |



\## Monitoring

\- Prometheus collects system metrics.

\- Grafana uses Prometheus as a data source.

\- Grafana dashboard: Distributed Systems Midterm Monitoring

\- PromQL test query: `up`



\## Kafka Test

A topic named `midterm-test` was created.



A producer published the message:

`Hello from Distributed Systems Midterm!`



A consumer successfully retrieved the message.



\## Notes

Services currently run as standalone Docker containers.

Kubernetes runs separately through Docker Desktop.

Docker containers and local settings are not stored in Git.



