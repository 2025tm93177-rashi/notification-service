const fs = require("fs");
const path = require("path");
const { DatabaseSync } = require("node:sqlite");
const {
  formatMoney,
  formatTimestamp,
  fromPaise,
  parseCsv,
  toPaise,
} = require("./utils");

class NotificationDatabase {
  constructor(dbPath) {
    this.dbPath = dbPath;
    fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    this.db = new DatabaseSync(dbPath);
    this.db.exec("PRAGMA foreign_keys = ON");
    this.db.exec("PRAGMA journal_mode = WAL");
    this.db.exec("PRAGMA synchronous = NORMAL");
    this.db.exec("PRAGMA busy_timeout = 5000");
  }

  close() {
    this.db.close();
  }

  withTransaction(work) {
    this.db.exec("BEGIN IMMEDIATE TRANSACTION");
    try {
      const result = work();
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      try {
        this.db.exec("ROLLBACK");
      } catch (rollbackError) {
        error.rollbackError = rollbackError;
      }
      throw error;
    }
  }

  ensureSchema() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS notifications_log (
        notification_id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        source_service TEXT NOT NULL,
        notification_type TEXT NOT NULL,
        target_role TEXT NOT NULL,
        target_key TEXT NOT NULL,
        channel TEXT NOT NULL CHECK (channel IN ('EMAIL', 'SMS')),
        recipient_name TEXT NOT NULL,
        recipient_email TEXT,
        recipient_phone TEXT,
        title TEXT NOT NULL,
        message TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('QUEUED', 'SENT', 'FAILED', 'SKIPPED')),
        provider TEXT NOT NULL,
        correlation_id TEXT,
        idempotency_key TEXT,
        error_message TEXT,
        created_at TEXT NOT NULL,
        delivered_at TEXT
      );

      CREATE UNIQUE INDEX IF NOT EXISTS idx_notifications_unique_event_channel_target
        ON notifications_log(event_id, channel, target_key);

      CREATE INDEX IF NOT EXISTS idx_notifications_created_at
        ON notifications_log(created_at DESC, notification_id DESC);

      CREATE INDEX IF NOT EXISTS idx_notifications_event_type
        ON notifications_log(event_type);
    `);
  }

  isSeeded() {
    return Boolean(this.getMeta("seed_version"));
  }

  setMeta(key, value) {
    this.db.prepare(
      `
        INSERT INTO meta(key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
      `,
    ).run(key, value);
  }

  getMeta(key) {
    return this.db.prepare("SELECT value FROM meta WHERE key = ?").get(key)?.value ?? null;
  }

  insertNotification(record) {
    const createdAt = record.created_at || formatTimestamp();
    const payloadJson =
      record.payload_json ||
      JSON.stringify(record.payload || {}, null, 0);
    const result = this.db.prepare(
      `
        INSERT OR IGNORE INTO notifications_log (
          event_id,
          event_type,
          source_service,
          notification_type,
          target_role,
          target_key,
          channel,
          recipient_name,
          recipient_email,
          recipient_phone,
          title,
          message,
          payload_json,
          status,
          provider,
          correlation_id,
          idempotency_key,
          error_message,
          created_at,
          delivered_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
    ).run(
      record.event_id,
      record.event_type,
      record.source_service,
      record.notification_type,
      record.target_role,
      record.target_key,
      record.channel,
      record.recipient_name,
      record.recipient_email ?? null,
      record.recipient_phone ?? null,
      record.title,
      record.message,
      payloadJson,
      record.status,
      record.provider,
      record.correlation_id ?? null,
      record.idempotency_key ?? null,
      record.error_message ?? null,
      createdAt,
      record.delivered_at ?? null,
    );

    const notification = this.db.prepare(
      `
        SELECT *
        FROM notifications_log
        WHERE event_id = ? AND channel = ? AND target_key = ?
      `,
    ).get(record.event_id, record.channel, record.target_key);

    return {
      inserted: result.changes > 0,
      notification,
    };
  }

  getNotification(notificationId) {
    return (
      this.db.prepare(
        `
          SELECT *
          FROM notifications_log
          WHERE notification_id = ?
        `,
      ).get(notificationId) || null
    );
  }

  count(table) {
    return this.db.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get().count;
  }

  countNotifications(filters = {}) {
    const { where, params } = this.buildFilterClause(filters);
    return this.db.prepare(
      `SELECT COUNT(*) AS count FROM notifications_log ${where}`,
    ).get(...params).count;
  }

  listNotifications(filters = {}) {
    const { where, params } = this.buildFilterClause(filters);
    const limit = Number.isFinite(filters.limit) ? filters.limit : 50;
    const offset = Number.isFinite(filters.offset) ? filters.offset : 0;

    return this.db.prepare(
      `
        SELECT *
        FROM notifications_log
        ${where}
        ORDER BY created_at DESC, notification_id DESC
        LIMIT ? OFFSET ?
      `,
    ).all(...params, limit, offset);
  }

  buildFilterClause(filters = {}) {
    const clauses = [];
    const params = [];

    if (filters.eventType) {
      clauses.push("event_type = ?");
      params.push(filters.eventType);
    }

    if (filters.notificationType) {
      clauses.push("notification_type = ?");
      params.push(filters.notificationType);
    }

    if (filters.channel) {
      clauses.push("channel = ?");
      params.push(filters.channel);
    }

    if (filters.status) {
      clauses.push("status = ?");
      params.push(filters.status);
    }

    if (filters.targetRole) {
      clauses.push("target_role = ?");
      params.push(filters.targetRole);
    }

    if (filters.accountId) {
      clauses.push("payload_json LIKE ?");
      params.push(`%"accountId":${Number(filters.accountId)}%`);
    }

    if (filters.customerId) {
      clauses.push("payload_json LIKE ?");
      params.push(`%"customerId":${Number(filters.customerId)}%`);
    }

    if (filters.search) {
      clauses.push("(recipient_name LIKE ? OR title LIKE ? OR message LIKE ?)");
      params.push(`%${filters.search}%`, `%${filters.search}%`, `%${filters.search}%`);
    }

    if (filters.from) {
      clauses.push("created_at >= ?");
      params.push(filters.from);
    }

    if (filters.to) {
      clauses.push("created_at <= ?");
      params.push(filters.to);
    }

    const where = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
    return { where, params };
  }

  getHealthSnapshot() {
    return {
      notifications: this.count("notifications_log"),
      sent: this.db.prepare(
        "SELECT COUNT(*) AS count FROM notifications_log WHERE status = 'SENT'",
      ).get().count,
      failed: this.db.prepare(
        "SELECT COUNT(*) AS count FROM notifications_log WHERE status = 'FAILED'",
      ).get().count,
      seedVersion: this.getMeta("seed_version"),
      seededAt: this.getMeta("seeded_at"),
    };
  }

  seedFromDirectory(seedDir, options = {}) {
    const customersPath = path.join(seedDir, "bank_customers.csv");
    const accountsPath = path.join(seedDir, "bank_accounts.csv");
    const transactionsPath = path.join(seedDir, "bank_transactions.csv");

    if (
      !fs.existsSync(customersPath) ||
      !fs.existsSync(accountsPath) ||
      !fs.existsSync(transactionsPath)
    ) {
      throw new Error(`Seed data not found in ${seedDir}`);
    }

    const thresholdPaise = Number(options.thresholdPaise || toPaise("50000"));
    const customers = parseCsv(fs.readFileSync(customersPath, "utf8"));
    const accounts = parseCsv(fs.readFileSync(accountsPath, "utf8"));
    const transactions = parseCsv(fs.readFileSync(transactionsPath, "utf8"));

    const customerById = new Map(
      customers.map((customer) => [Number(customer.customer_id), customer]),
    );
    const accountById = new Map(
      accounts.map((account) => [Number(account.account_id), account]),
    );

    let insertedCount = 0;

    this.withTransaction(() => {
      this.db.exec(`
        DELETE FROM notifications_log;
      `);

      const now = formatTimestamp();

      for (const account of accounts) {
        if (account.status === "ACTIVE") {
          continue;
        }

        const customer = customerById.get(Number(account.customer_id));
        const eventId = `ACCOUNT-${account.account_id}-${account.status}`;
        const payload = {
          kind: "ACCOUNT_STATUS_UPDATE",
          account,
          customer,
        };

        for (const channel of ["EMAIL", "SMS"]) {
          const title = "Account status update";
          const message = `Hi ${customer?.name || "Customer"}, your account ${account.account_number} status is ${account.status}.`;
          const result = this.insertNotification({
            event_id: eventId,
            event_type: "ACCOUNT_STATUS_UPDATE",
            source_service: "seed-data",
            notification_type: "ACCOUNT_STATUS_UPDATE",
            target_role: "ACCOUNT_HOLDER",
            target_key: `ACCOUNT:${account.account_number}`,
            channel,
            recipient_name: customer?.name || `Customer ${account.customer_id}`,
            recipient_email: customer?.email || null,
            recipient_phone: customer?.phone || null,
            title,
            message,
            payload,
            status: "SENT",
            provider: `SIMULATED_${channel}`,
            correlation_id: eventId,
            created_at: now,
            delivered_at: now,
          });
          insertedCount += result.inserted ? 1 : 0;
        }
      }

      for (const transaction of transactions) {
        const amountPaise = toPaise(transaction.amount);
        if (amountPaise < thresholdPaise) {
          continue;
        }

        const account = accountById.get(Number(transaction.account_id));
        const customer = account ? customerById.get(Number(account.customer_id)) : null;
        const eventId = `TXN-${transaction.txn_id}-HIGH-VALUE`;
        const payload = {
          kind: "HIGH_VALUE_TRANSACTION",
          transaction,
          account,
          customer,
        };

        for (const channel of ["EMAIL", "SMS"]) {
          const title = "High-value transaction alert";
          const message = `Hi ${customer?.name || "Customer"}, a ${transaction.txn_type.toLowerCase()} of ${formatMoney(amountPaise)} was recorded on account ${account?.account_number || transaction.account_id}.`;
          const result = this.insertNotification({
            event_id: eventId,
            event_type: "HIGH_VALUE_TRANSACTION",
            source_service: "seed-data",
            notification_type: "HIGH_VALUE_TRANSACTION",
            target_role: "ACCOUNT_HOLDER",
            target_key: `ACCOUNT:${account?.account_number || transaction.account_id}`,
            channel,
            recipient_name: customer?.name || `Customer ${transaction.account_id}`,
            recipient_email: customer?.email || null,
            recipient_phone: customer?.phone || null,
            title,
            message,
            payload,
            status: "SENT",
            provider: `SIMULATED_${channel}`,
            correlation_id: eventId,
            created_at: now,
            delivered_at: now,
          });
          insertedCount += result.inserted ? 1 : 0;
        }
      }

      this.setMeta("seed_version", "1");
      this.setMeta("seeded_at", now);
      this.setMeta(
        "seed_counts",
        JSON.stringify({
          customers: customers.length,
          accounts: accounts.length,
          transactions: transactions.length,
          notifications: insertedCount,
        }),
      );
    });

    return {
      customers: customers.length,
      accounts: accounts.length,
      transactions: transactions.length,
      notifications: insertedCount,
    };
  }
}

module.exports = {
  NotificationDatabase,
};
