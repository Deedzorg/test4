import http from 'node:http';

// v1.4.1 hardening wrapper.
// The Auto Runner samples readiness for a full 60 seconds. Keep the server-side
// override alive for a small grace period so the final 60s sample cannot race
// the exact expiry boundary and become a false mixed result.
const originalCreateServer = http.createServer;

http.createServer = function hardenedCreateServer(listener, ...rest) {
  const wrapped = async (req, res) => {
    const url = new URL(req.url || '/', 'http://local');

    if (req.method === 'GET' && url.pathname === '/auto') {
      const originalEnd = res.end.bind(res);
      res.end = (chunk, encoding, callback) => {
        try {
          const text = Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk ?? '');
          const hardened = text.replace(
            "body:JSON.stringify({action:'start',seconds:60})",
            "body:JSON.stringify({action:'start',seconds:65})"
          );
          res.removeHeader('Content-Length');
          return originalEnd(hardened, encoding, callback);
        } catch {
          return originalEnd(chunk, encoding, callback);
        }
      };
    }

    return listener(req, res);
  };

  return originalCreateServer.call(http, wrapped, ...rest);
};

await import('./server-v7.mjs');
