# Kiki Nightwear Vote Endpoint

This service provides an app-proxy endpoint that:

- finds or creates a customer by email
- records one Nightwear vote per customer
- returns `already_voted` when a vote already exists
- optionally sends a confirmation email (subject: `demo confirmation email`) via Resend

## Endpoint

- `POST /proxy/nightwear-vote`

Expected JSON body:

```json
{
  "email": "customer@example.com",
  "set_key": "helix"
}
```

Supported `set_key` values:

- `helix`
- `contour-set`
- `signature-slip`
- `v-lace-set`

## Tags and metafield written

- `list_founders_circle`
- `nightwear_i_voter`
- `vote_nightwear_i_<set_key>`
- Customer metafield: `custom.nightwear_i_vote = <set_key>`

## Local setup

1. Copy `.env.example` to `.env` and fill:
   - `SHOPIFY_API_SECRET`
   - `SHOPIFY_ADMIN_ACCESS_TOKEN`
2. Install packages:
   - `npm install`
3. Run:
   - `npm run dev`

## App proxy setup in Shopify

In `shopify.app.toml`, replace:

- `https://REPLACE_WITH_YOUR_APP_DOMAIN`

with your deployed app domain and deploy your app.

Then ensure app proxy points to:

- Prefix: `apps`
- Subpath: `nightwear-vote`
- Proxy URL: `https://<your-app-domain>/proxy/nightwear-vote`

## Response examples

Success:

```json
{
  "status": "ok",
  "voted_for": "The Helix",
  "set_key": "helix",
  "email_sent": true
}
```

Already voted:

```json
{
  "status": "already_voted",
  "voted_for": "The Helix",
  "set_key": "helix",
  "email_sent": false
}
```
