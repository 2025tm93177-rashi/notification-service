FROM node:24-alpine

WORKDIR /app

ENV PORT=4104
ENV HOST=0.0.0.0
ENV DB_PATH=/app/data/notification-service.db
ENV SEED_DIR=/app/seed-data
ENV TIMEZONE=Asia/Kolkata
ENV HIGH_VALUE_ALERT_THRESHOLD_RUPEES=50000
ENV DEFAULT_NOTIFICATION_CHANNELS=EMAIL,SMS
ENV NOTIFICATION_DELIVERY_MODE=SIMULATED

COPY package.json ./
COPY src ./src
COPY scripts ./scripts
COPY docs ./docs
COPY seed-data ./seed-data

RUN mkdir -p /app/data

EXPOSE 4104

CMD ["node", "src/server.js"]
