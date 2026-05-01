const fs = require("fs");
const path = require("path");
const { loadConfig } = require("../src/config");
const { NotificationDatabase } = require("../src/database");

function deleteDatabaseFiles(dbPath) {
  const resolved = path.resolve(dbPath);
  const candidates = [resolved, `${resolved}-wal`, `${resolved}-shm`];
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      fs.unlinkSync(candidate);
    }
  }
}

function main() {
  const config = loadConfig();
  deleteDatabaseFiles(config.dbPath);
  const db = new NotificationDatabase(config.dbPath);
  db.ensureSchema();
  db.seedFromDirectory(config.seedDir, {
    thresholdPaise: config.highValueThresholdPaise,
  });
  console.log(JSON.stringify({ success: true, message: "Database reset and reseeded" }, null, 2));
  db.close();
}

if (require.main === module) {
  main();
}
