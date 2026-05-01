const fs = require("fs");
const http = require("http");
const path = require("path");
const { URL } = require("url");
const { loadConfig } = require("./config");
const { NotificationDatabase } = require("./database");
const { ApiError, NotificationService, makeError } = require("./notification-service");
const { MetricsRegistry } = require("./metrics");
const { formatTimestamp, randomToken } = require("./utils");

function createLogger(serviceName) {
  return {
    info(payload) {
      console.log(JSON.stringify({ level: "info", service: serviceName, ...payload }));
    },
    warn(payload) {
      console.warn(JSON.stringify({ level: "warn", service: serviceName, ...payload }));
    },
    error(payload) {
      console.error(JSON.stringify({ level: "error", service: serviceName, ...payload }));
    },
  };
}

function splitPath(pathname) {
  return pathname.split("/").filter(Boolean);
}

function matchRoute(route, method, pathname) {
  if (route.method !== method) {
    return null;
  }

  const parts = splitPath(pathname);
  if (parts.length !== route.segments.length) {
    return null;
  }

  const params = {};
  for (let index = 0; index < route.segments.length; index += 1) {
    const template = route.segments[index];
    const value = parts[index];
    if (template.startsWith(":")) {
      params[template.slice(1)] = decodeURIComponent(value);
      continue;
    }

    if (template !== value) {
      return null;
    }
  }

  return params;
}

function replyJson(res, statusCode, body, headers = {}) {
  const payload = JSON.stringify(body);
  res.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(payload),
    ...headers,
  });
  res.end(payload);
}

function replyText(res, statusCode, body, headers = {}) {
  res.writeHead(statusCode, {
    "content-type": "text/plain; charset=utf-8",
    "content-length": Buffer.byteLength(body),
    ...headers,
  });
  res.end(body);
}

async function readJsonBody(req) {
  const chunks = [];
  let total = 0;

  for await (const chunk of req) {
    chunks.push(chunk);
    total += chunk.length;
    if (total > 1_000_000) {
      throw makeError(413, "PAYLOAD_TOO_LARGE", "Request body is too large");
    }
  }

  if (!chunks.length) {
    return {};
  }

  const raw = Buffer.concat(chunks).toString("utf8");
  if (!raw.trim()) {
    return {};
  }

  try {
    return JSON.parse(raw);
  } catch (error) {
    throw makeError(400, "INVALID_JSON", "Request body must be valid JSON");
  }
}

function sendError(res, error, requestId) {
  const statusCode = error.statusCode || 500;
  const code = error.code || "INTERNAL_SERVER_ERROR";
  const message = error.message || "Unexpected error";
  replyJson(
    res,
    statusCode,
    {
      success: false,
      error: {
        code,
        message,
        details: error.details || null,
        requestId,
      },
    },
    {
      "x-request-id": requestId,
    },
  );
}

function createRouteTable() {
  return [
    { method: "GET", path: "/", handler: handleRoot },
    { method: "GET", path: "/health", handler: handleHealth },
    { method: "GET", path: "/ready", handler: handleReady },
    { method: "GET", path: "/metrics", handler: handleMetrics },
    { method: "GET", path: "/openapi.yaml", handler: handleOpenApi },
    { method: "GET", path: "/docs", handler: handleDocs },
    { method: "GET", path: "/notifications", handler: handleListNotifications },
    {
      method: "GET",
      path: "/notifications/:notificationId",
      handler: handleGetNotification,
    },
    { method: "POST", path: "/notifications", handler: handleNotification },
    {
      method: "POST",
      path: "/notifications/high-value",
      handler: handleHighValueNotification,
    },
    {
      method: "POST",
      path: "/notifications/account-status",
      handler: handleAccountStatusNotification,
    },
  ].map((route) => ({
    ...route,
    segments: splitPath(route.path),
  }));
}

async function handleRoot(context) {
  return {
    statusCode: 200,
    body: {
      success: true,
      service: context.config.serviceName,
      version: "1.0.0",
      documentation: {
        docs: "/docs",
        openapi: "/openapi.yaml",
        health: "/health",
        ready: "/ready",
        metrics: "/metrics",
      },
    },
  };
}

async function handleHealth(context) {
  return {
    statusCode: 200,
    body: context.service.health(),
  };
}

async function handleReady(context) {
  return {
    statusCode: 200,
    body: context.service.ready(),
  };
}

async function handleMetrics(context) {
  return {
    statusCode: 200,
    body: context.metrics.render(),
    text: true,
  };
}

async function handleOpenApi(context) {
  const specPath = path.join(context.config.rootDir, "docs", "openapi.yaml");
  return {
    statusCode: 200,
    body: fs.readFileSync(specPath, "utf8"),
    text: true,
  };
}

async function handleDocs(context) {
  return {
    statusCode: 200,
    body: `<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>${context.config.serviceName} docs</title>
    <style>
      body { font-family: system-ui, sans-serif; margin: 40px; line-height: 1.5; }
      code, a { background: #f5f5f5; padding: 0.15rem 0.35rem; border-radius: 4px; }
      ul { padding-left: 20px; }
    </style>
  </head>
  <body>
    <h1>${context.config.serviceName}</h1>
    <p>OpenAPI spec: <a href="/openapi.yaml">/openapi.yaml</a></p>
    <ul>
      <li><code>POST /notifications</code></li>
      <li><code>POST /notifications/high-value</code></li>
      <li><code>POST /notifications/account-status</code></li>
      <li><code>GET /notifications</code></li>
    </ul>
  </body>
</html>`,
    text: true,
  };
}

async function handleListNotifications(context, _params, query) {
  return {
    statusCode: 200,
    body: context.service.listNotifications(query),
  };
}

async function handleGetNotification(context, params) {
  return {
    statusCode: 200,
    body: context.service.getNotification(params.notificationId),
  };
}

async function handleNotification(context, _params, _query, body, req) {
  return {
    statusCode: 201,
    body: context.service.processEvent(body, {
      correlationId:
        req.headers["x-correlation-id"] || req.headers["x-request-id"] || null,
      sourceService: body.sourceService || body.service || req.headers["x-service-name"] || null,
      idempotencyKey: req.headers["idempotency-key"] || null,
    }),
  };
}

async function handleHighValueNotification(context, _params, _query, body, req) {
  return {
    statusCode: 201,
    body: context.service.processHighValueEvent(body, {
      correlationId:
        req.headers["x-correlation-id"] || req.headers["x-request-id"] || null,
      sourceService: body.sourceService || body.service || req.headers["x-service-name"] || null,
      idempotencyKey: req.headers["idempotency-key"] || null,
    }),
  };
}

async function handleAccountStatusNotification(context, _params, _query, body, req) {
  return {
    statusCode: 201,
    body: context.service.processAccountStatusEvent(body, {
      correlationId:
        req.headers["x-correlation-id"] || req.headers["x-request-id"] || null,
      sourceService: body.sourceService || body.service || req.headers["x-service-name"] || null,
      idempotencyKey: req.headers["idempotency-key"] || null,
    }),
  };
}

function createServer() {
  const config = loadConfig();
  const logger = createLogger(config.serviceName);
  const metrics = new MetricsRegistry();
  metrics.registerCounter("http_requests_total", "Total HTTP requests");
  metrics.registerHistogram("http_request_duration_ms", "HTTP request duration in milliseconds");
  metrics.registerCounter("notifications_total", "Total notification deliveries");
  metrics.registerCounter("failed_notifications_total", "Failed notification deliveries");
  metrics.registerHistogram(
    "notification_delivery_latency_ms",
    "Notification delivery latency in milliseconds",
  );

  const db = new NotificationDatabase(config.dbPath);
  db.ensureSchema();
  if (!db.isSeeded() || process.env.RESET_DB === "true") {
    db.seedFromDirectory(config.seedDir, {
      thresholdPaise: config.highValueThresholdPaise,
    });
  }

  const service = new NotificationService({
    db,
    config: {
      ...config,
      defaultChannels: config.defaultChannels.length
        ? config.defaultChannels
        : ["EMAIL", "SMS"],
    },
    metrics,
    logger,
  });

  const routes = createRouteTable();

  const server = http.createServer(async (req, res) => {
    const startedAt = Date.now();
    const requestId = req.headers["x-request-id"] || randomToken(10);
    const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
    const query = Object.fromEntries(url.searchParams.entries());

    res.setHeader("x-request-id", requestId);

    const route = routes.find((candidate) => matchRoute(candidate, req.method, url.pathname));
    const params = route ? matchRoute(route, req.method, url.pathname) : null;

    try {
      let body = {};
      if (["POST", "PUT", "PATCH"].includes(req.method)) {
        body = await readJsonBody(req);
      }

      if (!route) {
        throw makeError(404, "NOT_FOUND", `No route matches ${req.method} ${url.pathname}`);
      }

      const result = await route.handler(
        { config, db, metrics, service, logger, requestId, req, res },
        params,
        query,
        body,
        req,
        res,
      );

      if (!result) {
        throw makeError(500, "EMPTY_RESPONSE", "Route handler returned no response");
      }

      if (result.text) {
        replyText(res, result.statusCode || 200, result.body, {
          "x-request-id": requestId,
        });
      } else {
        replyJson(res, result.statusCode || 200, result.body, {
          "x-request-id": requestId,
        });
      }

      const durationMs = Date.now() - startedAt;
      const routeName = route.path;
      metrics.inc("http_requests_total", {
        method: req.method,
        route: routeName,
        status: String(result.statusCode || 200),
      });
      metrics.observe("http_request_duration_ms", durationMs, {
        method: req.method,
        route: routeName,
        status: String(result.statusCode || 200),
      });

      logger.info({
        requestId,
        method: req.method,
        route: routeName,
        path: url.pathname,
        statusCode: result.statusCode || 200,
        durationMs,
        timestamp: formatTimestamp(new Date(), config.timezone),
      });
    } catch (error) {
      const durationMs = Date.now() - startedAt;
      const routeName = route ? route.path : "unmatched";
      const statusCode = error.statusCode || 500;

      metrics.inc("http_requests_total", {
        method: req.method,
        route: routeName,
        status: String(statusCode),
      });
      metrics.observe("http_request_duration_ms", durationMs, {
        method: req.method,
        route: routeName,
        status: String(statusCode),
      });

      logger.error({
        requestId,
        method: req.method,
        route: routeName,
        path: url.pathname,
        statusCode,
        durationMs,
        errorCode: error.code || "INTERNAL_SERVER_ERROR",
        message: error.message,
        timestamp: formatTimestamp(new Date(), config.timezone),
      });

      sendError(res, error, requestId);
    }
  });

  return {
    config,
    db,
    logger,
    metrics,
    service,
    server,
  };
}

if (require.main === module) {
  const { config, logger, server } = createServer();
  server.listen(config.port, config.host, () => {
    logger.info({
      message: "Notification service started",
      port: config.port,
      host: config.host,
      dbPath: config.dbPath,
      seedDir: config.seedDir,
      timezone: config.timezone,
    });
  });
}

module.exports = {
  createServer,
};
