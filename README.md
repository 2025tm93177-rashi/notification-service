# Notification Service

Node.js microservice for banking alerts and notification logs.

## Features

- Accepts event webhooks from the transaction and account services
- Generates email and SMS alerts for:
  - high-value transactions
  - account status updates
- Stores delivery history in SQLite
- Exposes list and detail APIs for notification logs
- Provides structured JSON logs and Prometheus-style metrics

## Quick start

```bash
npm run seed
npm start
```

Default port: `4104`

## Endpoints

- `GET /health`
- `GET /ready`
- `GET /metrics`
- `GET /docs`
- `GET /openapi.yaml`
- `POST /notifications`
- `POST /notifications/high-value`
- `POST /notifications/account-status`
- `GET /notifications`
- `GET /notifications/:notificationId`

## Transaction service integration

Set the transaction service variable:

```bash
NOTIFICATION_SERVICE_URL=http://localhost:4104/notifications
```

The transaction service can then POST transfer and deposit alerts directly to this service.

## Seed data

Seed scripts use the provided banking CSVs to generate demo notification logs.

## Docker

```bash
docker compose up --build
```

