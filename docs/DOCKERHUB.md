# kafpeek

**Peek inside your Kafka topics.** A lightweight UI for Apache Kafka: a single Go binary with the interface
embedded, in a ~11 MB distroless image, using ~18–21 MB of RAM when idle or with live tail. Server-side message
search with CEL filters, live tail, and AWS MSK IAM support.

![kafpeek: message search with a CEL filter](https://raw.githubusercontent.com/stolato/kafpeek/main/docs/screenshot.png)

**Source, full documentation, and issues:** [github.com/stolato/kafpeek](https://github.com/stolato/kafpeek)

## Features

- **[CEL](https://cel.dev) filters on the server**: only matching messages travel to the browser.
  Example: `json.status == "ERROR" && json.amount > 500`
- **Streaming search** over the most recent messages, a time range, or the **entire topic**, with real progress
- **Live tail** in real time with the same filters
- Topics, partitions, configs, and consumer groups with lag
- **Produce** messages with production protection, or read-only mode
- **Non-intrusive**: reads never use a consumer group or commit offsets
- PLAINTEXT, TLS, mTLS, SASL/PLAIN, SCRAM, and **AWS MSK IAM**
- Login (Basic Auth), CSRF and DNS rebinding protection, strict CSP, non-root user

## Tags

| Tag | Description |
|---|---|
| `latest` | Latest stable version |
| `X.Y.Z` (e.g. `0.1.1`) | Exact version (recommended in production) |
| `X.Y` (e.g. `0.1`) | Latest patch of the series |

Architectures: `linux/amd64` and `linux/arm64`. Also available at `ghcr.io/stolato/kafpeek`.

## Quick start

```bash
docker run -d --name kafpeek \
  -p 127.0.0.1:8090:8090 \
  -e KAFKA_CLUSTER_NAME=my-cluster \
  -e KAFKA_BROKERS=broker1:9092,broker2:9092 \
  stolato/kafpeek:latest
```

Open **http://localhost:8090**.

### AWS MSK with IAM

```bash
docker run -d --name kafpeek -p 127.0.0.1:8090:8090 \
  -e KAFKA_CLUSTER_NAME=msk-prod \
  -e KAFKA_AUTH=aws-iam \
  -e KAFKA_BROKERS=b-1.mycluster.xxxx.c3.kafka.sa-east-1.amazonaws.com:9098 \
  -e KAFKA_READ_ONLY=true \
  -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... \
  stolato/kafpeek:latest
```

On ECS, EKS, or EC2, omit the `AWS_*` variables: the task, pod, or instance role is used automatically.
For a profile/SSO, mount `~/.aws` at `/home/nonroot/.aws` and set `AWS_PROFILE`. The region is read from the
MSK broker hostname; set `AWS_REGION` only if your brokers use custom names without the region.

### SASL/SCRAM (MSK, Aiven, Redpanda...)

```bash
docker run -d --name kafpeek -p 127.0.0.1:8090:8090 \
  -e KAFKA_BROKERS=broker:9096 \
  -e KAFKA_AUTH=scram-sha-512 \
  -e KAFKA_USERNAME=... -e KAFKA_PASSWORD=... \
  stolato/kafpeek:latest
```

### Exposed on the network (with login)

```bash
docker run -d --name kafpeek -p 8090:8090 \
  -e KAFKA_BROKERS=broker:9092 \
  -e UI_USERNAME=admin -e UI_PASSWORD=a-strong-password \
  -e ALLOWED_HOSTS=kafpeek.mycompany.internal \
  stolato/kafpeek:latest
```

Put HTTPS in front (load balancer, Ingress, or proxy): Basic Auth sends the password with every request.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Brokers as `host:port`, comma-separated |
| `KAFKA_CLUSTER_NAME` | `default` | Name shown in the UI. If it contains `prod`, Produce asks for extra confirmation |
| `KAFKA_AUTH` | `none` | `none`, `plain`, `scram-sha-256`, `scram-sha-512`, `aws-iam` |
| `KAFKA_USERNAME` / `KAFKA_PASSWORD` | — | Credentials for `plain` and `scram-*` |
| `KAFKA_TLS` | automatic | Turns on automatically with SASL or certificates |
| `KAFKA_TLS_CA_FILE` | — | Extra PEM CA(s) to validate the broker |
| `KAFKA_TLS_CERT_FILE` / `KAFKA_TLS_KEY_FILE` | — | Client PEM certificate and key (mTLS) |
| `KAFKA_READ_ONLY` | `false` | Disables all writes (recommended in production) |
| `UI_USERNAME` / `UI_PASSWORD` | — | Enable login (both together) |
| `ALLOWED_HOSTS` | `localhost,127.0.0.1,::1` | Hostnames/IPs used to access the UI |
| `TRUST_PROXY` | `false` | Uses `X-Forwarded-For` (only behind a proxy) |
| `LOG_LEVEL` | `info` | `debug` for connection details |
| `AWS_*` | — | AWS credentials and region for `aws-iam` (`AWS_REGION` only as a fallback when the broker hostname has no region) |

The full list, connection scenarios (TLS, mTLS, Confluent Cloud, Azure Event Hubs...), and the MSK IAM policy are in the
[GitHub README](https://github.com/stolato/kafpeek#readme). A Portuguese version is available at
[README.pt-BR.md](https://github.com/stolato/kafpeek/blob/main/README.pt-BR.md).

## About the image

- **Distroless** base (`gcr.io/distroless/static-debian12:nonroot`): no shell, with public CAs included
- Runs as a **non-root** user (`65532`) and does not write to disk (compatible with `read_only: true`)
- Port **8090**; built-in `HEALTHCHECK` (`/kafpeek healthcheck`)
- `docker run --rm stolato/kafpeek version` shows the version and commit

## License

[MIT](https://github.com/stolato/kafpeek/blob/main/LICENSE). Apache Kafka is a registered trademark of the Apache
Software Foundation; kafpeek is an independent project.
