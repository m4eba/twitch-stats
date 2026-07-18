import process from 'process';
import http from 'node:http';
import pino, { Logger } from 'pino';
import client from 'prom-client';

export { client as metrics };

export function startMetricsServer(port: number): http.Server {
  client.collectDefaultMetrics();
  const server = http.createServer((req, res) => {
    if (req.url === '/metrics') {
      client.register.metrics().then(
        (m) => {
          res.setHeader('Content-Type', client.register.contentType);
          res.end(m);
        },
        () => {
          res.statusCode = 500;
          res.end();
        }
      );
    } else {
      res.statusCode = 404;
      res.end();
    }
  });
  server.listen(port);
  return server;
}

export function initLogger(module: string): Logger {
  // eslint-disable-next-line dot-notation
  let level = process.env['LOG_LEVEL'];
  if (level === undefined) {
    level = 'info';
  }
  return pino({ level }).child({
    module,
  });
}
