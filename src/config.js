const path = require("path");

function parseInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function loadConfig() {
  const rootDir = path.resolve(__dirname, "..");
  const highValueRupees = parseInteger(
    process.env.HIGH_VALUE_ALERT_THRESHOLD_RUPEES,
    50000,
  );

  return {
    rootDir,
    serviceName: process.env.SERVICE_NAME || "notification-service",
    port: parseInteger(process.env.PORT, 4104),
    host: process.env.HOST || "127.0.0.1",
    dbPath:
      process.env.DB_PATH || path.join(rootDir, "data", "notification-service.db"),
    seedDir: process.env.SEED_DIR || path.join(rootDir, "seed-data"),
    timezone: process.env.TIMEZONE || "Asia/Kolkata",
    deliveryMode: process.env.NOTIFICATION_DELIVERY_MODE || "SIMULATED",
    defaultChannels: (process.env.DEFAULT_NOTIFICATION_CHANNELS || "EMAIL,SMS")
      .split(",")
      .map((channel) => channel.trim().toUpperCase())
      .filter(Boolean),
    highValueThresholdPaise: highValueRupees * 100,
    smtp: {
      host: process.env.SMTP_HOST || "",
      port: parseInteger(process.env.SMTP_PORT, 587),
      user: process.env.SMTP_USER || "",
      pass: process.env.SMTP_PASS || "",
    },
    sms: {
      provider: process.env.SMS_PROVIDER || "",
      apiKey: process.env.SMS_API_KEY || "",
    },
  };
}

module.exports = {
  loadConfig,
};
