# DataPulse shared sales API

Cloudflare Worker used by the GitHub Pages dashboard.

- `GET /api/sales`: returns the latest shared browser-upload payload.
- `POST /api/sales`: validates and stores the latest payload in Workers KV.
- `GET /api/health`: health check.
- All sales endpoints verify the existing Cloudflare Access JWT.

Binding: `SALES_KV`. Route: `datapulse.cc.cd/api/*`.
