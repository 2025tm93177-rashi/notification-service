const path = require("path");
const { loadConfig } = require("../src/config");
const { NotificationDatabase } = require("../src/database");

function main() {
  const config = loadConfig();
  const db = new NotificationDatabase(config.dbPath);
  db.ensureSchema();
  const counts = db.seedFromDirectory(config.seedDir, {
    thresholdPaise: config.highValueThresholdPaise,
  });

  console.log(
    JSON.stringify(
      {
        success: true,
        message: "Notification database seeded",
        dbPath: path.resolve(config.dbPath),
        seedDir: path.resolve(config.seedDir),
        counts,
      },
      null,
      2,
    ),
  );

  db.close();
}

if (require.main === module) {
  main();
}
