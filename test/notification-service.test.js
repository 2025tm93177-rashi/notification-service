const assert = require("assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const test = require("node:test");

const { loadConfig } = require("../src/config");
const { NotificationDatabase } = require("../src/database");
const { NotificationService } = require("../src/notification-service");

function createService() {
  const config = loadConfig();
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "notification-service-test-"));
  const dbPath = path.join(tempDir, "notification.db");
  const db = new NotificationDatabase(dbPath);
  db.ensureSchema();

  const metrics = {
    inc() {},
    observe() {},
    render() {
      return "";
    },
  };

  const logger = {
    info() {},
    warn() {},
    error() {},
  };

  const service = new NotificationService({
    db,
    config: {
      ...config,
      dbPath,
      defaultChannels: ["EMAIL", "SMS"],
    },
    metrics,
    logger,
  });

  return {
    db,
    service,
    cleanup() {
      db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
    },
  };
}

test("processes a transfer event into email and sms notifications", () => {
  const ctx = createService();

  try {
    const result = ctx.service.processEvent({
      eventId: "evt-001",
      eventType: "TransferCompleted",
      sourceService: "transaction-service",
      transactionType: "TRANSFER",
      highValue: true,
      payload: {
        transferGroupId: "TRF-123",
        accounts: {
          source: {
            accountId: 1,
            accountNumber: "222121684341",
            customerName: "Vivaan Khan",
            email: "vivaan.khan90@inbox.com",
            phone: "9288353015",
          },
          destination: {
            accountId: 3,
            accountNumber: "073431162417",
            customerName: "Aarav Kulkarni",
            email: "aarav.kulkarni793@mail.com",
            phone: "9830710619",
          },
        },
        debitTransaction: {
          transactionType: "TRANSFER_OUT",
          amount: { paise: 100000, rupees: "1000.00" },
          reference: "Rent payment",
        },
        creditTransaction: {
          transactionType: "TRANSFER_IN",
          amount: { paise: 100000, rupees: "1000.00" },
          reference: "Rent payment",
        },
      },
    });

    assert.equal(result.success, true);
    assert.equal(result.createdCount, 4);
    assert.equal(ctx.db.count("notifications_log"), 4);
    const replay = ctx.service.processEvent({
      eventId: "evt-001",
      eventType: "TransferCompleted",
      sourceService: "transaction-service",
      transactionType: "TRANSFER",
      highValue: true,
      payload: {
        transferGroupId: "TRF-123",
        accounts: {
          source: {
            accountId: 1,
            accountNumber: "222121684341",
            customerName: "Vivaan Khan",
          },
          destination: {
            accountId: 3,
            accountNumber: "073431162417",
            customerName: "Aarav Kulkarni",
          },
        },
      },
    });

    assert.equal(replay.createdCount, 0);
    assert.equal(ctx.db.count("notifications_log"), 4);
  } finally {
    ctx.cleanup();
  }
});

test("processes account status updates", () => {
  const ctx = createService();

  try {
    const result = ctx.service.processAccountStatusEvent({
      eventId: "evt-002",
      eventType: "AccountStatusUpdated",
      sourceService: "account-service",
      payload: {
        account: {
          accountId: 8,
          accountNumber: "546293361680",
          customerName: "Ishaan Mehra",
          accountStatus: "FROZEN",
        },
      },
    });

    assert.equal(result.createdCount, 2);
    assert.equal(ctx.db.count("notifications_log"), 2);
    const entry = ctx.service.getNotification(1).notification;
    assert.equal(entry.notificationType, "ACCOUNT_STATUS_UPDATE");
    assert.equal(entry.channel, "EMAIL");
  } finally {
    ctx.cleanup();
  }
});
