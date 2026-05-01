const crypto = require("crypto");

function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  for (let index = 0; index < text.length; index += 1) {
    const ch = text[index];
    const next = text[index + 1];

    if (inQuotes) {
      if (ch === '"') {
        if (next === '"') {
          field += '"';
          index += 1;
        } else {
          inQuotes = false;
        }
      } else {
        field += ch;
      }
      continue;
    }

    if (ch === '"') {
      inQuotes = true;
      continue;
    }

    if (ch === ",") {
      row.push(field);
      field = "";
      continue;
    }

    if (ch === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      continue;
    }

    if (ch === "\r") {
      continue;
    }

    field += ch;
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }

  if (!rows.length) {
    return [];
  }

  const headers = rows[0].map((header) => header.trim());
  return rows
    .slice(1)
    .filter((currentRow) => currentRow.some((cell) => cell !== ""))
    .map((currentRow) =>
      headers.reduce((acc, header, index) => {
        acc[header] = (currentRow[index] ?? "").trim();
        return acc;
      }, {}),
    );
}

function stableStringify(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(",")}]`;
  }

  const keys = Object.keys(value).sort();
  return `{${keys
    .map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`)
    .join(",")}}`;
}

function sha256(text) {
  return crypto.createHash("sha256").update(String(text)).digest("hex");
}

function randomToken(length = 12) {
  return crypto
    .randomBytes(Math.ceil(length / 2))
    .toString("hex")
    .slice(0, length)
    .toUpperCase();
}

function toPaise(value) {
  if (value === null || value === undefined || value === "") {
    throw new Error("Amount is required");
  }

  const normalized =
    typeof value === "number" ? value.toFixed(2) : String(value).trim();
  if (!/^-?\d+(\.\d{1,2})?$/.test(normalized)) {
    throw new Error(`Invalid amount: ${value}`);
  }

  const sign = normalized.startsWith("-") ? -1 : 1;
  const unsigned = normalized.replace(/^-/, "");
  const [wholePart, fractionPart = ""] = unsigned.split(".");
  const whole = Number.parseInt(wholePart, 10);
  const fraction = Number.parseInt((fractionPart + "00").slice(0, 2), 10);
  return sign * (whole * 100 + fraction);
}

function fromPaise(value) {
  return (Number(value) / 100).toFixed(2);
}

function formatMoney(value) {
  return new Intl.NumberFormat("en-IN", {
    style: "currency",
    currency: "INR",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Number(value) / 100);
}

function formatTimestamp(date = new Date(), timeZone = "Asia/Kolkata") {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).formatToParts(date);

  const lookup = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${lookup.year}-${lookup.month}-${lookup.day} ${lookup.hour}:${lookup.minute}:${lookup.second}`;
}

function normalizeChannels(value, fallback = ["EMAIL", "SMS"]) {
  const list = Array.isArray(value)
    ? value
    : String(value || "")
        .split(",")
        .map((item) => item.trim());
  const normalized = list
    .map((item) => item.toUpperCase())
    .filter((item) => item === "EMAIL" || item === "SMS");
  return normalized.length ? [...new Set(normalized)] : fallback;
}

function maskEmail(value = "") {
  const text = String(value);
  const atIndex = text.indexOf("@");
  if (atIndex <= 1) {
    return "***";
  }
  return `${text.slice(0, 1)}***${text.slice(atIndex)}`;
}

function maskPhone(value = "") {
  const text = String(value);
  if (text.length <= 4) {
    return "***";
  }
  return `${text.slice(0, 2)}******${text.slice(-2)}`;
}

function buildFingerprint(method, path, body) {
  return sha256(
    stableStringify({
      method,
      path,
      body,
    }),
  );
}

module.exports = {
  buildFingerprint,
  formatMoney,
  formatTimestamp,
  fromPaise,
  maskEmail,
  maskPhone,
  normalizeChannels,
  parseCsv,
  randomToken,
  sha256,
  stableStringify,
  toPaise,
};
