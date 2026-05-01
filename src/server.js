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
    { method: "GET", path: "/health", handler: handleHealth },
    { method: "GET", path: "/metrics", handler: handleMetrics },
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

async function handleHealth(context) {
  return {
    statusCode: 200,
    body: context.service.health(),
  };
}

async function handleMetrics(context) {
  return {
    statusCode: 200,
    body: context.metrics.render(),
    text: true,
  };
}

async function handleDocs(context) {
  const specPath = path.join(context.config.rootDir, "docs", "openapi.yaml");
  const specText = JSON.stringify(
    fs.readFileSync(specPath, "utf8").replace(/<\/script/gi, "<\\/script"),
  );
  return {
    statusCode: 200,
    body: `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${context.config.serviceName} Swagger UI</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
    <style>
      html {
        box-sizing: border-box;
        overflow-y: scroll;
      }

      *, *::before, *::after {
        box-sizing: inherit;
      }

      body {
        margin: 0;
        min-height: 100vh;
        background:
          radial-gradient(circle at top left, rgba(59, 130, 246, 0.16), transparent 28%),
          radial-gradient(circle at top right, rgba(15, 23, 42, 0.1), transparent 22%),
          linear-gradient(180deg, #f8fafc 0%, #e2e8f0 100%);
        font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        color: #0f172a;
      }

      .swagger-shell {
        max-width: 1480px;
        margin: 0 auto;
        padding: 28px 20px 40px;
      }

      .swagger-card {
        background: rgba(255, 255, 255, 0.84);
        backdrop-filter: blur(14px);
        border: 1px solid rgba(148, 163, 184, 0.22);
        border-radius: 24px;
        box-shadow: 0 24px 60px rgba(15, 23, 42, 0.14);
        overflow: hidden;
      }

      .swagger-header {
        padding: 28px 28px 8px;
      }

      .swagger-header h1 {
        margin: 0;
        font-size: clamp(1.7rem, 2.8vw, 2.6rem);
        line-height: 1.1;
      }

      .swagger-header p {
        margin: 10px 0 0;
        color: #475569;
        max-width: 60rem;
      }

      .swagger-header a {
        color: #0f172a;
      }

      #swagger-ui {
        padding: 0 18px 18px;
      }
    </style>
  </head>
  <body>
    <main class="swagger-shell">
      <section class="swagger-card">
        <header class="swagger-header">
          <h1>${context.config.serviceName} API</h1>
          <p>Interactive Swagger UI for the notification service.</p>
        </header>
        <div id="swagger-ui"></div>
      </section>
    </main>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-standalone-preset.js"></script>
    <script src="https://unpkg.com/js-yaml@4/dist/js-yaml.min.js"></script>
    <script>
      window.onload = function () {
        const specText = ${specText};
        window.ui = SwaggerUIBundle({
          spec: jsyaml.load(specText),
          dom_id: "#swagger-ui",
          deepLinking: true,
          displayRequestDuration: true,
          presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
          layout: "BaseLayout"
        });
      };
    </script>
  </body>
</html>`,
    text: true,
    headers: {
      "content-type": "text/html; charset=utf-8",
    },
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
          ...(result.headers || {}),
        });
      } else {
        replyJson(res, result.statusCode || 200, result.body, {
          "x-request-id": requestId,
          ...(result.headers || {}),
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
