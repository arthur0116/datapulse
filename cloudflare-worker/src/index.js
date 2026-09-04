const MANIFEST_KEY = "sales/latest-manifest.json";
const TARGETS_KEY = "targets/latest.json";
const MAX_CHUNK_BYTES = 10 * 1024 * 1024;
const MAX_ROWS = 250000;
const MAX_CHUNKS = 64;
const ACCESS_TEAM = "https://fancy-union-e3ee.cloudflareaccess.com";
const ACCESS_AUD = "9a6fca88e4e387b80553e87d9fcfe7c6aae9f54ea824f1def5eba1c40dbfcde4";

const responseHeaders = {
  "content-type": "application/json; charset=utf-8",
  "cache-control": "no-store",
  "x-content-type-options": "nosniff",
};

const json = (body, status = 200) => new Response(JSON.stringify(body), {status, headers: responseHeaders});

const decodePart = (value) => {
  const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized + "=".repeat((4 - normalized.length % 4) % 4);
  return JSON.parse(new TextDecoder().decode(Uint8Array.from(atob(padded), c => c.charCodeAt(0))));
};

async function verifyAccess(request) {
  const token = request.headers.get("cf-access-jwt-assertion");
  if (!token) return null;
  const parts = token.split(".");
  if (parts.length !== 3) return null;
  try {
    const header = decodePart(parts[0]);
    const payload = decodePart(parts[1]);
    const audiences = Array.isArray(payload.aud) ? payload.aud : [payload.aud];
    if (!audiences.includes(ACCESS_AUD) || Number(payload.exp || 0) <= Math.floor(Date.now() / 1000)) return null;
    const certs = await fetch(`${ACCESS_TEAM}/cdn-cgi/access/certs`, {cf: {cacheTtl: 3600, cacheEverything: true}}).then(r => r.json());
    const jwk = (certs.keys || []).find(key => key.kid === header.kid);
    if (!jwk) return null;
    const key = await crypto.subtle.importKey("jwk", jwk, {name: "RSASSA-PKCS1-v1_5", hash: "SHA-256"}, false, ["verify"]);
    const signatureText = parts[2].replace(/-/g, "+").replace(/_/g, "/");
    const signature = Uint8Array.from(atob(signatureText + "=".repeat((4 - signatureText.length % 4) % 4)), c => c.charCodeAt(0));
    const valid = await crypto.subtle.verify("RSASSA-PKCS1-v1_5", key, signature, new TextEncoder().encode(`${parts[0]}.${parts[1]}`));
    return valid ? payload : null;
  } catch {
    return null;
  }
}

const datePattern = /^\d{4}-\d{2}-\d{2}$/;
const validRows = rows => Array.isArray(rows) && rows.length > 0 && rows.every(row => datePattern.test(row.d) && typeof row.s === "string" && Number.isFinite(Number(row.u)) && Number.isFinite(Number(row.q)) && Number.isFinite(Number(row.a)));
const validRanges = ranges => Array.isArray(ranges) && ranges.length > 0 && ranges.every(item => datePattern.test(item.start) && datePattern.test(item.end) && item.start <= item.end);
const targetMonthPattern = /^\d{4}-\d{2}$/;
const validTargetRows = rows => Array.isArray(rows) && rows.length > 0 && rows.length <= 10000 && rows.every(row => targetMonthPattern.test(row.d) && typeof row.s === "string" && Number.isFinite(Number(row.u)) && Number.isFinite(Number(row.a)));
const validUploadId = value => /^[a-zA-Z0-9_-]{8,80}$/.test(value || "");
const chunkKey = (uploadId, index) => `sales/uploads/${uploadId}/chunk-${String(index).padStart(3, "0")}.json`;

const manifestLayers = manifest => {
  if (!manifest) return [];
  if (Array.isArray(manifest.layers)) return manifest.layers;
  if (Array.isArray(manifest.chunks)) {
    return [{
      updatedAt: manifest.updatedAt,
      updatedBy: manifest.updatedBy,
      ranges: manifest.ranges || [],
      files: manifest.files || [],
      totalRows: manifest.totalRows || 0,
      chunks: manifest.chunks,
    }];
  }
  return [];
};

const rowCoveredByRanges = (row, ranges) => ranges.some(range => row.d >= range.start && row.d <= range.end);

async function readJson(request, maxBytes) {
  const declaredLength = Number(request.headers.get("content-length") || 0);
  if (declaredLength > maxBytes) throw new Error("请求数据过大");
  const text = await request.text();
  if (new TextEncoder().encode(text).byteLength > maxBytes) throw new Error("请求数据过大");
  return JSON.parse(text);
}

async function streamLatest(env, manifest) {
  if (!manifest) return json({ok: true, data: null});
  const layers = manifestLayers(manifest);
  const publicManifest = {
    version: 1,
    updatedAt: manifest.updatedAt,
    updatedBy: manifest.updatedBy,
    ranges: layers.flatMap(layer => layer.ranges || []),
    files: manifest.files || [],
    totalRows: manifest.totalRows,
  };
  const encoder = new TextEncoder();
  const stream = new ReadableStream({
    async start(controller) {
      try {
        const prefix = `{"ok":true,"data":${JSON.stringify(publicManifest).slice(0, -1)},"rows":[`;
        controller.enqueue(encoder.encode(prefix));
        let hasRows = false;
        for (let layerIndex = 0; layerIndex < layers.length; layerIndex++) {
          const layer = layers[layerIndex];
          const newerRanges = layers.slice(layerIndex + 1).flatMap(item => item.ranges || []);
          for (const key of layer.chunks || []) {
            const rows = await env.SALES_KV.get(key, "json");
            if (!Array.isArray(rows)) throw new Error("共享数据分片缺失");
            const visibleRows = newerRanges.length ? rows.filter(row => !rowCoveredByRanges(row, newerRanges)) : rows;
            if (!visibleRows.length) continue;
            const inner = JSON.stringify(visibleRows).slice(1, -1);
            if (hasRows) controller.enqueue(encoder.encode(","));
            controller.enqueue(encoder.encode(inner));
            hasRows = true;
          }
        }
        controller.enqueue(encoder.encode("]}}"));
        controller.close();
      } catch (error) {
        controller.error(error);
      }
    },
  });
  return new Response(stream, {headers: responseHeaders});
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/api/health") return json({ok: true, service: "datapulse-shared-sales", storage: "chunked-kv"});
    if (!["/api/sales", "/api/sales/chunk", "/api/sales/commit", "/api/targets"].includes(url.pathname)) return json({ok: false, error: "Not found"}, 404);

    const identity = await verifyAccess(request);
    if (!identity) return json({ok: false, error: "Cloudflare Access authentication required"}, 401);

    if (url.pathname === "/api/sales" && request.method === "GET") {
      return streamLatest(env, await env.SALES_KV.get(MANIFEST_KEY, "json"));
    }

    if (url.pathname === "/api/targets" && request.method === "GET") {
      return json({ok: true, data: await env.SALES_KV.get(TARGETS_KEY, "json")});
    }

    if (url.pathname === "/api/targets" && request.method === "POST") {
      try {
        const payload = await readJson(request, 2 * 1024 * 1024);
        if (!validTargetRows(payload.rows)) return json({ok: false, error: "目标数据校验失败"}, 400);
        const data = {
          version: 1,
          rows: payload.rows,
          meta: payload.meta && typeof payload.meta === "object" ? payload.meta : {},
          updatedAt: new Date().toISOString(),
          updatedBy: identity.email || identity.sub || "Access user",
        };
        await env.SALES_KV.put(TARGETS_KEY, JSON.stringify(data));
        return json({ok: true, updatedAt: data.updatedAt, rows: data.rows.length});
      } catch (error) {
        return json({ok: false, error: error.message === "请求数据过大" ? error.message : "请求不是有效 JSON"}, 400);
      }
    }

    if (url.pathname === "/api/sales/chunk" && request.method === "POST") {
      try {
        const payload = await readJson(request, MAX_CHUNK_BYTES);
        const {uploadId, index, total, rows} = payload;
        if (!validUploadId(uploadId) || !Number.isInteger(index) || index < 0 || !Number.isInteger(total) || total < 1 || total > MAX_CHUNKS || index >= total || !validRows(rows) || rows.length > 15000) {
          return json({ok: false, error: "销售数据分片校验失败"}, 400);
        }
        await env.SALES_KV.put(chunkKey(uploadId, index), JSON.stringify(rows));
        return json({ok: true, index, rows: rows.length});
      } catch (error) {
        return json({ok: false, error: error.message === "请求数据过大" ? error.message : "请求不是有效 JSON"}, 400);
      }
    }

    if (url.pathname === "/api/sales/commit" && request.method === "POST") {
      try {
        const payload = await readJson(request, 1024 * 1024);
        const {uploadId, totalChunks, totalRows, ranges, files} = payload;
        if (!validUploadId(uploadId) || !Number.isInteger(totalChunks) || totalChunks < 1 || totalChunks > MAX_CHUNKS || !Number.isInteger(totalRows) || totalRows < 1 || totalRows > MAX_ROWS || !validRanges(ranges)) {
          return json({ok: false, error: "销售数据提交校验失败"}, 400);
        }
        const chunks = Array.from({length: totalChunks}, (_, index) => chunkKey(uploadId, index));
        const checks = await Promise.all(chunks.map(key => env.SALES_KV.get(key, {type: "text", cacheTtl: 60})));
        if (checks.some(value => !value)) return json({ok: false, error: "销售数据分片尚未完整上传"}, 409);
        const previous = await env.SALES_KV.get(MANIFEST_KEY, "json");
        const layers = manifestLayers(previous);
        if (layers.length >= 64) return json({ok: false, error: "增量版本过多，请联系管理员执行数据整理"}, 409);
        const layer = {updatedAt: new Date().toISOString(), updatedBy: identity.email || identity.sub || "Access user", ranges, files: Array.isArray(files) ? files.slice(0, 20) : [], totalRows, chunks};
        const manifest = {
          version: 2,
          updatedAt: layer.updatedAt,
          updatedBy: layer.updatedBy,
          files: [...new Set([...layers.flatMap(item => item.files || []), ...layer.files])].slice(-200),
          totalRows: (previous?.totalRows || 0) + totalRows,
          layers: [...layers, layer],
        };
        await env.SALES_KV.put(MANIFEST_KEY, JSON.stringify(manifest));
        return json({ok: true, updatedAt: manifest.updatedAt, rows: totalRows, ranges: ranges.length, mode: "incremental"});
      } catch (error) {
        return json({ok: false, error: error.message === "请求数据过大" ? error.message : "请求不是有效 JSON"}, 400);
      }
    }

    return json({ok: false, error: "Method not allowed"}, 405);
  },
};
