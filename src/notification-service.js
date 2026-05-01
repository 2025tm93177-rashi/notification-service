const {
  buildFingerprint,
  formatMoney,
  formatTimestamp,
  fromPaise,
  maskEmail,
  maskPhone,
  normalizeChannels,
  randomToken,
  toPaise,
} = require("./utils");

class ApiError extends Error {
  constructor(statusCode, code, message, details = null) {
    super(message);
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }
}

function makeError(statusCode, code, message, details = null) {
  return new ApiError(statusCode, code, message, details);
}

function normalizeId(value, fieldName) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw makeError(400, "VALIDATION_ERROR", `${fieldName} must be a positive integer`);
  }
  return parsed;
}

function normalizeEventType(value) {
  return String(value || "")
    .trim()
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/[^A-Za-z0-9]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toUpperCase();
}

function normalizeTarget(rawTarget = {}) {
  return {
    role: rawTarget.role || rawTarget.targetRole || "ACCOUNT_HOLDER",
    customerName:
      rawTarget.customerName ||
      rawTarget.name ||
      rawTarget.recipientName ||
      "Customer",
    accountId: rawTarget.accountId ? Number(rawTarget.accountId) : null,
    accountNumber:
      rawTarget.accountNumber || rawTarget.account_number || rawTarget.accountNo || null,
    accountStatus:
      rawTarget.accountStatus || rawTarget.status || rawTarget.account_status || null,
    email: rawTarget.email || rawTarget.recipientEmail || null,
    phone: rawTarget.phone || rawTarget.recipientPhone || null,
  };
}

function targetKey(target) {
  return `${target.role}:${target.accountNumber || target.accountId || target.customerName}`;
}

function extractAmountPaise(event) {
  const candidates = [
    event.amount,
    event.payload?.amount,
    event.payload?.transaction?.amount,
    event.payload?.transaction?.amount?.paise,
    event.payload?.debitTransaction?.amount,
    event.payload?.creditTransaction?.amount,
    event.payload?.debitTransaction?.amount?.paise,
    event.payload?.creditTransaction?.amount?.paise,
  ];

  for (const candidate of candidates) {
    if (candidate === null || candidate === undefined || candidate === "") {
      continue;
    }

    if (typeof candidate === "number") {
      return candidate > 100000 ? candidate : candidate * 100;
    }

    if (typeof candidate === "object") {
      if (candidate.paise !== undefined) {
        return Number(candidate.paise);
      }
      if (candidate.rupees !== undefined) {
        return toPaise(candidate.rupees);
      }
      if (candidate.amount !== undefined) {
        return toPaise(candidate.amount);
      }
    }

    try {
      return toPaise(candidate);
    } catch (error) {
      continue;
    }
  }

  return 0;
}

function extractTransactionType(event) {
  return normalizeEventType(
    event.transactionType ||
      event.payload?.transactionType ||
      event.payload?.transaction?.transactionType ||
      event.payload?.debitTransaction?.transactionType ||
      event.payload?.creditTransaction?.transactionType ||
      event.payload?.transaction?.type ||
      "TRANSACTION",
  );
}

function extractReference(event) {
  return (
    event.reference ||
    event.payload?.reference ||
    event.payload?.transaction?.reference ||
    event.payload?.debitTransaction?.reference ||
    event.payload?.creditTransaction?.reference ||
    null
  );
}

function resolveTargets(event) {
  const payload = event.payload || {};
  const accounts = payload.accounts || {};
  const targets = [];

  const sourceCandidate = accounts.source || payload.debitTransaction || payload.transaction;
  const destinationCandidate = accounts.destination || payload.creditTransaction || null;

  const transferLike =
    Boolean(sourceCandidate && destinationCandidate) ||
    normalizeEventType(event.transactionType).includes("TRANSFER") ||
    normalizeEventType(event.eventType).includes("TRANSFER");

  if (transferLike && sourceCandidate) {
    targets.push(
      normalizeTarget({
        ...sourceCandidate,
        role: "SOURCE_ACCOUNT_HOLDER",
      }),
    );
  }

  if (transferLike && destinationCandidate) {
    targets.push(
      normalizeTarget({
        ...destinationCandidate,
        role: "DESTINATION_ACCOUNT_HOLDER",
      }),
    );
  }

  if (!targets.length && payload.account) {
    targets.push(normalizeTarget({ ...payload.account, role: "ACCOUNT_HOLDER" }));
  }

  if (!targets.length && payload.transaction) {
    targets.push(normalizeTarget({ ...payload.transaction, role: "ACCOUNT_HOLDER" }));
  }

  if (!targets.length && event.account) {
    targets.push(normalizeTarget({ ...event.account, role: "ACCOUNT_HOLDER" }));
  }

  if (!targets.length) {
    targets.push(
      normalizeTarget({
        role: "ACCOUNT_HOLDER",
        customerName: event.customerName || event.recipientName || "Customer",
        accountNumber: event.accountNumber || null,
        email: event.recipientEmail || event.email || null,
        phone: event.recipientPhone || event.phone || null,
        accountStatus: event.accountStatus || null,
      }),
    );
  }

  return targets;
}

function composeNotification(event, target, channel, config) {
  const eventType = normalizeEventType(event.eventType || event.type || "NOTIFICATION");
  const highValue =
    Boolean(event.highValue) || extractAmountPaise(event) >= config.highValueThresholdPaise;
  const amountPaise = extractAmountPaise(event);
  const transactionType = extractTransactionType(event);
  const reference = extractReference(event);
  const accountNumber = target.accountNumber || "N/A";
  const customerName = target.customerName || "Customer";
  const isAccountStatusUpdate =
    eventType.includes("ACCOUNT_STATUS") ||
    eventType.includes("STATUS_UPDATE") ||
    event.notificationType === "ACCOUNT_STATUS_UPDATE";

  let notificationType;
  let title;
  let message;

  if (isAccountStatusUpdate) {
    notificationType = "ACCOUNT_STATUS_UPDATE";
    const status = target.accountStatus || event.accountStatus || event.status || "UPDATED";
    title = "Account status update";
    message = `Hi ${customerName}, your account ${accountNumber} status is now ${status}.`;
  } else {
    notificationType = highValue ? "HIGH_VALUE_TRANSACTION" : "TRANSACTION_ALERT";
    title = highValue ? "High-value transaction alert" : "Transaction alert";

    let actionVerb = "recorded";
    if (transactionType.includes("TRANSFER")) {
      actionVerb =
        target.role === "DESTINATION_ACCOUNT_HOLDER" ? "credited by transfer" : "debited for transfer";
    } else if (transactionType.includes("WITHDRAW")) {
      actionVerb = "withdrawn";
    } else if (transactionType.includes("DEPOSIT")) {
      actionVerb = "deposited";
    }

    const amountText = amountPaise > 0 ? formatMoney(amountPaise) : "a transaction";
    const referenceText = reference ? ` Reference: ${reference}.` : "";
    message = `Hi ${customerName}, ${amountText} was ${actionVerb} on account ${accountNumber}.${referenceText}`;
  }

  return {
    notificationType,
    title,
    message,
    amountPaise,
    reference,
  };
}

class NotificationService {
  constructor({ db, config, metrics, logger }) {
    this.db = db;
    this.config = config;
    this.metrics = metrics;
    this.logger = logger;
  }

  health() {
    return {
      service: this.config.serviceName,
      status: "ok",
      timestamp: formatTimestamp(new Date(), this.config.timezone),
      database: this.db.getHealthSnapshot(),
    };
  }

  listNotifications(query = {}) {
    const filters = this.normalizeQueryFilters(query);
    const total = this.db.countNotifications(filters);
    const rows = this.db.listNotifications(filters).map((row) => this.formatNotification(row));

    return {
      items: rows,
      page: {
        limit: filters.limit,
        offset: filters.offset,
        total,
      },
    };
  }

  getNotification(notificationId) {
    const row = this.db.getNotification(normalizeId(notificationId, "notificationId"));
    if (!row) {
      throw makeError(404, "NOTIFICATION_NOT_FOUND", "Notification was not found");
    }
    return {
      notification: this.formatNotification(row),
    };
  }

  processEvent(body, options = {}) {
    const event = this.normalizeEvent(body, options);
    const targets = resolveTargets(event);
    const channels = normalizeChannels(
      event.channels || options.channels || this.config.defaultChannels,
      this.config.defaultChannels,
    );
    const createdAt = formatTimestamp(new Date(), this.config.timezone);
    const notifications = [];
    let insertedCount = 0;

    for (const target of targets) {
      for (const channel of channels) {
        const blueprint = composeNotification(event, target, channel, this.config);
        const delivery = this.deliver(channel, blueprint);
        const eventId = event.eventId;
        const rowResult = this.db.insertNotification({
          event_id: eventId,
          event_type: event.eventType,
          source_service: event.sourceService,
          notification_type: blueprint.notificationType,
          target_role: target.role,
          target_key: targetKey(target),
          channel,
          recipient_name: target.customerName,
          recipient_email: target.email,
          recipient_phone: target.phone,
          title: blueprint.title,
          message: blueprint.message,
          payload: {
            event,
            target,
            blueprint,
            channel,
          },
          status: delivery.status,
          provider: delivery.provider,
          correlation_id: event.correlationId,
          idempotency_key: event.idempotencyKey,
          error_message: delivery.errorMessage,
          created_at: createdAt,
          delivered_at: delivery.deliveredAt,
        });

        insertedCount += rowResult.inserted ? 1 : 0;
        const formatted = this.formatNotification(rowResult.notification);
        notifications.push({
          ...formatted,
          inserted: rowResult.inserted,
        });

        this.logger.info({
          message: "Notification processed",
          eventId,
          eventType: event.eventType,
          notificationId: rowResult.notification.notification_id,
          channel,
          targetRole: target.role,
          recipientEmail: maskEmail(target.email),
          recipientPhone: maskPhone(target.phone),
          status: delivery.status,
          durationMs: delivery.durationMs,
          correlationId: event.correlationId,
        });

        this.metrics.inc("notifications_total", {
          type: blueprint.notificationType,
          channel,
          status: delivery.status,
        });
        this.metrics.observe("notification_delivery_latency_ms", delivery.durationMs, {
          channel,
          status: delivery.status,
        });

        if (delivery.status === "FAILED") {
          this.metrics.inc("failed_notifications_total", { channel });
        }
      }
    }

    return {
      success: true,
      replay: insertedCount === 0,
      event: {
        eventId: event.eventId,
        eventType: event.eventType,
        sourceService: event.sourceService,
      },
      createdCount: insertedCount,
      totalNotifications: notifications.length,
      notifications,
    };
  }

  processHighValueEvent(body, options = {}) {
    return this.processEvent(
      {
        ...body,
        eventType: body.eventType || "HIGH_VALUE_TRANSACTION",
        highValue: true,
      },
      options,
    );
  }

  processAccountStatusEvent(body, options = {}) {
    return this.processEvent(
      {
        ...body,
        eventType: body.eventType || "ACCOUNT_STATUS_UPDATE",
      },
      options,
    );
  }

  normalizeEvent(body, options = {}) {
    const payload = body && typeof body === "object" ? body : {};
    const eventType = normalizeEventType(
      payload.eventType || payload.notificationType || payload.type || options.eventType || "NOTIFICATION_EVENT",
    );
    const sourceService = String(
      payload.sourceService || payload.service || options.sourceService || "unknown-service",
    );
    const correlationId =
      payload.correlationId ||
      payload.requestId ||
      options.correlationId ||
      randomToken(12);
    const idempotencyKey =
      payload.idempotencyKey ||
      payload.eventId ||
      payload.transferGroupId ||
      options.idempotencyKey ||
      null;
    const eventId =
      payload.eventId ||
      idempotencyKey ||
      buildFingerprint("POST", "/notifications", {
        eventType,
        sourceService,
        payload,
      });

    return {
      ...payload,
      payload: payload.payload || payload,
      eventType,
      sourceService,
      correlationId,
      idempotencyKey,
      eventId,
    };
  }

  deliver(channel, blueprint) {
    const startedAt = Date.now();
    const provider = channel === "EMAIL" ? "SIMULATED_EMAIL" : "SIMULATED_SMS";

    if (this.config.deliveryMode === "FAIL") {
      return {
        status: "FAILED",
        provider,
        deliveredAt: null,
        errorMessage: "Delivery mode configured to FAIL",
        durationMs: Date.now() - startedAt,
      };
    }

    return {
      status: "SENT",
      provider,
      deliveredAt: formatTimestamp(new Date(), this.config.timezone),
      errorMessage: null,
      durationMs: Date.now() - startedAt,
    };
  }

  normalizeQueryFilters(query = {}) {
    const limit = Math.min(
      Math.max(Number.parseInt(query.limit || "50", 10) || 50, 1),
      200,
    );
    const offset = Math.max(Number.parseInt(query.offset || "0", 10) || 0, 0);
    const eventType = query.eventType ? normalizeEventType(query.eventType) : null;
    const notificationType = query.notificationType
      ? normalizeEventType(query.notificationType)
      : null;
    const channel = query.channel ? normalizeEventType(query.channel) : null;
    const status = query.status ? normalizeEventType(query.status) : null;
    const targetRole = query.targetRole ? normalizeEventType(query.targetRole) : null;
    const accountId = query.accountId ? Number.parseInt(query.accountId, 10) : null;
    const customerId = query.customerId ? Number.parseInt(query.customerId, 10) : null;
    const search = query.search ? String(query.search).trim() : null;
    const from = query.from ? `${query.from} 00:00:00` : null;
    const to = query.to ? `${query.to} 23:59:59` : null;

    return {
      limit,
      offset,
      eventType,
      notificationType,
      channel,
      status,
      targetRole,
      accountId,
      customerId,
      search,
      from,
      to,
    };
  }

  formatNotification(row) {
    const payload = safeJsonParse(row.payload_json, {});
    const recipientEmail = row.recipient_email ?? null;
    const recipientPhone = row.recipient_phone ?? null;

    return {
      notificationId: Number(row.notification_id),
      eventId: row.event_id,
      eventType: row.event_type,
      sourceService: row.source_service,
      notificationType: row.notification_type,
      targetRole: row.target_role,
      targetKey: row.target_key,
      channel: row.channel,
      recipient: {
        name: row.recipient_name,
        email: recipientEmail ? maskEmail(recipientEmail) : null,
        phone: recipientPhone ? maskPhone(recipientPhone) : null,
      },
      title: row.title,
      message: row.message,
      payload,
      status: row.status,
      provider: row.provider,
      correlationId: row.correlation_id,
      idempotencyKey: row.idempotency_key,
      errorMessage: row.error_message,
      createdAt: row.created_at,
      deliveredAt: row.delivered_at,
    };
  }
}

function safeJsonParse(text, fallback) {
  try {
    return JSON.parse(text);
  } catch (error) {
    return fallback;
  }
}

module.exports = {
  ApiError,
  NotificationService,
  makeError,
};
