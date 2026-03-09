require('dotenv').config();

const crypto = require('crypto');
const express = require('express');

const app = express();

app.use(express.json({ limit: '128kb' }));
app.use(express.urlencoded({ extended: false }));

const PORT = Number(process.env.PORT || 3000);
const API_VERSION = process.env.SHOPIFY_API_VERSION || '2026-01';
const CLIENT_ID = process.env.SHOPIFY_CLIENT_ID || process.env.SHOPIFY_API_KEY || '';
const SHARED_SECRET =
  process.env.SHOPIFY_API_SECRET ||
  process.env.SHOPIFY_API_SECRET_KEY ||
  process.env.SHOPIFY_CLIENT_SECRET ||
  '';
const DEFAULT_SHOP = process.env.SHOPIFY_SHOP_DOMAIN || 'kiki-20245.myshopify.com';
const ADMIN_TOKEN = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN || '';
const ALLOW_UNSIGNED_PROXY_REQUESTS = process.env.ALLOW_UNSIGNED_PROXY_REQUESTS === '1';
const STATS_CACHE_TTL_MS = Math.max(5000, Number(process.env.NIGHTWEAR_STATS_CACHE_MS || 15000));

const SET_LABELS = {
  helix: 'The Helix',
  'contour-set': 'The Contour Set',
  'signature-slip': 'The Signature Slip',
  'v-lace-set': 'The V-Lace Set'
};

const SET_KEYS = Object.keys(SET_LABELS);
const BASE_TAGS = ['list_founders_circle', 'nightwear_i_voter'];
const VOTE_TAG_PREFIX = 'vote_nightwear_i_';
const STATS_METAOBJECT_TYPE = 'nightwear_signal_counts';
const STATS_METAOBJECT_HANDLE = 'live';

const statsCache = new Map();
const shopIdCache = new Map();
const metaobjectDefinitionReady = new Map();
const tokenCache = new Map();

function toSlug(value) {
  return String(value || '')
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function normalizeSetKey(raw) {
  const slug = toSlug(raw);
  if (slug === 'contour' || slug === 'contour-set') return 'contour-set';
  if (slug === 'signature' || slug === 'signature-slip') return 'signature-slip';
  if (slug === 'v-lace' || slug === 'v-lace-set' || slug === 'vlace-set') return 'v-lace-set';
  if (slug === 'helix') return 'helix';
  return '';
}

function setLabelForKey(setKey) {
  return SET_LABELS[setKey] || 'Nightwear I';
}

function voteTagForSet(setKey) {
  return VOTE_TAG_PREFIX + setKey;
}

function extractExistingVote(tags, metafieldValue) {
  if (Array.isArray(tags)) {
    const voteTag = tags.find((tag) => String(tag).toLowerCase().startsWith(VOTE_TAG_PREFIX));
    if (voteTag) {
      return normalizeSetKey(String(voteTag).replace(VOTE_TAG_PREFIX, ''));
    }
  }
  return normalizeSetKey(metafieldValue || '');
}

function sanitizeShop(shop) {
  const raw = String(shop || DEFAULT_SHOP).trim().toLowerCase();
  return raw.replace(/^https?:\/\//, '').replace(/\/+$/, '');
}

function verifyProxySignature(query) {
  if (!SHARED_SECRET) return false;
  const signature = query.signature;
  if (!signature) return false;

  const sorted = Object.keys(query)
    .filter((key) => key !== 'signature')
    .sort()
    .map((key) => {
      const value = query[key];
      if (Array.isArray(value)) return `${key}=${value.join(',')}`;
      return `${key}=${value}`;
    })
    .join('');

  const digest = crypto.createHmac('sha256', SHARED_SECRET).update(sorted).digest('hex');

  try {
    return crypto.timingSafeEqual(Buffer.from(digest, 'utf8'), Buffer.from(String(signature), 'utf8'));
  } catch (_error) {
    return false;
  }
}

function isProxyRequestAuthorized(req) {
  const isSigned = verifyProxySignature(req.query || {});
  if (isSigned) return true;
  return ALLOW_UNSIGNED_PROXY_REQUESTS;
}

function clearStatsCache(shop) {
  statsCache.delete(shop);
}

function readCachedStats(shop) {
  const cached = statsCache.get(shop);
  if (!cached) return null;
  if (cached.expiresAt <= Date.now()) {
    statsCache.delete(shop);
    return null;
  }
  return cached.payload;
}

function writeCachedStats(shop, payload) {
  statsCache.set(shop, {
    payload,
    expiresAt: Date.now() + STATS_CACHE_TTL_MS
  });
}

function normalizeDateLabel(date) {
  try {
    return new Intl.DateTimeFormat('en-AU', {
      day: '2-digit',
      month: 'short',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
      timeZone: 'Australia/Perth'
    }).format(date);
  } catch (_error) {
    return date.toISOString();
  }
}

function asInteger(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 0;
  return Math.round(parsed);
}

function parseTimestamp(value) {
  const stamp = Date.parse(String(value || ''));
  return Number.isFinite(stamp) ? stamp : null;
}

function calculateDecisionWindowHours(timestamps) {
  if (!Array.isArray(timestamps) || timestamps.length < 2) {
    return 0;
  }

  const sorted = timestamps.slice().sort((a, b) => a - b);
  let totalHours = 0;
  let intervalCount = 0;

  for (let index = 1; index < sorted.length; index += 1) {
    const diffMs = sorted[index] - sorted[index - 1];
    if (diffMs <= 0) continue;
    totalHours += diffMs / 3600000;
    intervalCount += 1;
  }

  if (!intervalCount) return 0;
  return asInteger(totalHours / intervalCount);
}

async function adminGraphql(shop, query, variables) {
  const token = await getAdminToken(shop);

  const response = await fetch(`https://${shop}/admin/api/${API_VERSION}/graphql.json`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Shopify-Access-Token': token
    },
    body: JSON.stringify({
      query,
      variables: variables || {}
    })
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const detail = payload && payload.errors ? JSON.stringify(payload.errors) : response.statusText;
    throw new Error(`admin_http_${response.status}:${detail}`);
  }

  if (payload.errors && payload.errors.length) {
    throw new Error(`admin_graphql:${JSON.stringify(payload.errors)}`);
  }

  return payload.data;
}

function readCachedToken(shop) {
  const entry = tokenCache.get(shop);
  if (!entry) return '';
  if (entry.expiresAt <= Date.now()) {
    tokenCache.delete(shop);
    return '';
  }
  return entry.token || '';
}

function writeCachedToken(shop, token, expiresInSeconds) {
  const ttlMs = Math.max(60, Number(expiresInSeconds || 3600)) * 1000;
  const safetyWindowMs = 60 * 1000;
  tokenCache.set(shop, {
    token,
    expiresAt: Date.now() + Math.max(30 * 1000, ttlMs - safetyWindowMs)
  });
}

async function fetchClientCredentialsToken(shop) {
  if (!CLIENT_ID || !SHARED_SECRET) {
    throw new Error('missing_client_credentials');
  }

  const response = await fetch(`https://${shop}/admin/oauth/access_token`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      client_id: CLIENT_ID,
      client_secret: SHARED_SECRET,
      grant_type: 'client_credentials'
    })
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const detail = payload && payload.error_description ? payload.error_description : response.statusText;
    throw new Error(`token_http_${response.status}:${detail}`);
  }

  const token = payload && payload.access_token ? String(payload.access_token) : '';
  if (!token) {
    throw new Error('token_missing_access_token');
  }

  writeCachedToken(shop, token, payload.expires_in);
  return token;
}

async function getAdminToken(shop) {
  if (ADMIN_TOKEN) return ADMIN_TOKEN;
  const cached = readCachedToken(shop);
  if (cached) return cached;
  return fetchClientCredentialsToken(shop);
}

function assertMutationUserErrors(result, context) {
  const userErrors = result && result.userErrors ? result.userErrors : [];
  if (userErrors.length) {
    throw new Error(`${context}:${JSON.stringify(userErrors)}`);
  }
}

async function getShopId(shop) {
  if (shopIdCache.has(shop)) {
    return shopIdCache.get(shop);
  }

  const query = `
    query ShopId {
      shop {
        id
      }
    }
  `;

  const data = await adminGraphql(shop, query, {});
  const shopId = data && data.shop && data.shop.id;
  if (!shopId) {
    throw new Error('shop_id_missing');
  }

  shopIdCache.set(shop, shopId);
  return shopId;
}

async function getCustomerByEmail(shop, email) {
  const query = `
    query FindCustomerByEmail($search: String!) {
      customers(first: 1, query: $search) {
        nodes {
          id
          email
          tags
          metafield(namespace: "custom", key: "nightwear_i_vote") {
            value
          }
        }
      }
    }
  `;

  const data = await adminGraphql(shop, query, { search: `email:${email}` });
  return (data && data.customers && data.customers.nodes && data.customers.nodes[0]) || null;
}

async function createCustomer(shop, email, setKey) {
  const mutation = `
    mutation CreateCustomer($input: CustomerInput!) {
      customerCreate(input: $input) {
        customer {
          id
          email
          tags
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const tags = BASE_TAGS.concat([voteTagForSet(setKey)]);
  const attempts = [
    {
      email,
      acceptsMarketing: true,
      tags
    },
    {
      email,
      tags
    }
  ];

  let lastError = null;
  for (const input of attempts) {
    const data = await adminGraphql(shop, mutation, { input });
    const result = data && data.customerCreate;
    if (!result) {
      lastError = 'customer_create_missing_result';
      continue;
    }

    if (!result.userErrors || !result.userErrors.length) {
      return result.customer;
    }

    lastError = `customer_create_error:${JSON.stringify(result.userErrors)}`;
  }

  throw new Error(lastError || 'customer_create_failed');
}

async function addTags(shop, customerId, tags) {
  const mutation = `
    mutation AddTags($id: ID!, $tags: [String!]!) {
      tagsAdd(id: $id, tags: $tags) {
        node {
          id
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const data = await adminGraphql(shop, mutation, { id: customerId, tags });
  const result = data && data.tagsAdd;
  if (!result) throw new Error('tags_add_missing_result');
  assertMutationUserErrors(result, 'tags_add_error');
}

async function setVoteMetafield(shop, customerId, setKey) {
  const mutation = `
    mutation SetVoteMetafield($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields {
          id
          key
          value
        }
        userErrors {
          field
          message
          code
        }
      }
    }
  `;

  const metafields = [
    {
      ownerId: customerId,
      namespace: 'custom',
      key: 'nightwear_i_vote',
      type: 'single_line_text_field',
      value: setKey
    }
  ];

  const data = await adminGraphql(shop, mutation, { metafields });
  const result = data && data.metafieldsSet;
  if (!result) throw new Error('metafields_set_missing_result');
  assertMutationUserErrors(result, 'metafields_set_error');
}

async function collectNightwearSignals(shop) {
  const counts = {
    helix: 0,
    'contour-set': 0,
    'signature-slip': 0,
    'v-lace-set': 0
  };

  const voteTimestamps = [];
  let cursor = null;
  let safetyPage = 0;

  const query = `
    query NightwearSignals($after: String) {
      customers(first: 250, after: $after, query: "tag:nightwear_i_voter") {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          id
          tags
          updatedAt
          metafield(namespace: "custom", key: "nightwear_i_vote") {
            value
          }
        }
      }
    }
  `;

  while (safetyPage < 60) {
    const data = await adminGraphql(shop, query, { after: cursor });
    const connection = data && data.customers;
    const nodes = (connection && connection.nodes) || [];

    nodes.forEach((customer) => {
      const voteKey = extractExistingVote(customer.tags, customer.metafield && customer.metafield.value);
      if (!voteKey || !Object.prototype.hasOwnProperty.call(counts, voteKey)) return;

      counts[voteKey] += 1;

      const updatedAt = parseTimestamp(customer.updatedAt);
      if (updatedAt) {
        voteTimestamps.push(updatedAt);
      }
    });

    if (!connection || !connection.pageInfo || !connection.pageInfo.hasNextPage) {
      break;
    }

    cursor = connection.pageInfo.endCursor;
    if (!cursor) break;
    safetyPage += 1;
  }

  const foundersTotal = SET_KEYS.reduce((sum, key) => sum + (counts[key] || 0), 0);

  const looks = {};
  SET_KEYS.forEach((key) => {
    const count = counts[key] || 0;
    looks[key] = foundersTotal > 0 ? asInteger((count / foundersTotal) * 100) : 0;
  });

  let leaderKey = 'helix';
  let leaderVotes = -1;
  SET_KEYS.forEach((key) => {
    const nextVotes = counts[key] || 0;
    if (nextVotes > leaderVotes) {
      leaderVotes = nextVotes;
      leaderKey = key;
    }
  });

  if (leaderVotes < 0) {
    leaderVotes = 0;
    leaderKey = 'helix';
  }

  const now = new Date();
  const nextUpdateDate = new Date(now.getTime() + STATS_CACHE_TTL_MS);
  const snapshot = {
    founders_total: foundersTotal,
    black_share: foundersTotal > 0 ? looks[leaderKey] : 0,
    decision_window_hours: calculateDecisionWindowHours(voteTimestamps),
    next_update: normalizeDateLabel(nextUpdateDate),
    looks,
    votes: counts,
    leader: {
      key: leaderKey,
      label: setLabelForKey(leaderKey),
      votes: leaderVotes,
      pct: foundersTotal > 0 ? looks[leaderKey] : 0
    },
    updated_at: now.toISOString()
  };

  return snapshot;
}

async function syncShopSignalMetafields(shop, snapshot) {
  const shopId = await getShopId(shop);

  const metafields = [
    {
      ownerId: shopId,
      namespace: 'nightwear',
      key: 'founders_total',
      type: 'number_integer',
      value: String(asInteger(snapshot.founders_total))
    },
    {
      ownerId: shopId,
      namespace: 'nightwear',
      key: 'black_share',
      type: 'number_integer',
      value: String(asInteger(snapshot.black_share))
    },
    {
      ownerId: shopId,
      namespace: 'nightwear',
      key: 'decision_window_hours',
      type: 'number_integer',
      value: String(asInteger(snapshot.decision_window_hours))
    },
    {
      ownerId: shopId,
      namespace: 'nightwear',
      key: 'next_update',
      type: 'single_line_text_field',
      value: String(snapshot.next_update || '')
    },
    {
      ownerId: shopId,
      namespace: 'nightwear',
      key: 'helix_pct',
      type: 'number_integer',
      value: String(asInteger(snapshot.looks.helix || 0))
    },
    {
      ownerId: shopId,
      namespace: 'nightwear',
      key: 'contour_set_pct',
      type: 'number_integer',
      value: String(asInteger(snapshot.looks['contour-set'] || 0))
    },
    {
      ownerId: shopId,
      namespace: 'nightwear',
      key: 'signature_slip_pct',
      type: 'number_integer',
      value: String(asInteger(snapshot.looks['signature-slip'] || 0))
    },
    {
      ownerId: shopId,
      namespace: 'nightwear',
      key: 'v_lace_set_pct',
      type: 'number_integer',
      value: String(asInteger(snapshot.looks['v-lace-set'] || 0))
    },
    {
      ownerId: shopId,
      namespace: 'nightwear',
      key: 'helix_votes',
      type: 'number_integer',
      value: String(asInteger(snapshot.votes.helix || 0))
    },
    {
      ownerId: shopId,
      namespace: 'nightwear',
      key: 'contour_set_votes',
      type: 'number_integer',
      value: String(asInteger(snapshot.votes['contour-set'] || 0))
    },
    {
      ownerId: shopId,
      namespace: 'nightwear',
      key: 'signature_slip_votes',
      type: 'number_integer',
      value: String(asInteger(snapshot.votes['signature-slip'] || 0))
    },
    {
      ownerId: shopId,
      namespace: 'nightwear',
      key: 'v_lace_set_votes',
      type: 'number_integer',
      value: String(asInteger(snapshot.votes['v-lace-set'] || 0))
    },
    {
      ownerId: shopId,
      namespace: 'nightwear',
      key: 'leader_key',
      type: 'single_line_text_field',
      value: String(snapshot.leader.key || '')
    },
    {
      ownerId: shopId,
      namespace: 'nightwear',
      key: 'leader_label',
      type: 'single_line_text_field',
      value: String(snapshot.leader.label || '')
    },
    {
      ownerId: shopId,
      namespace: 'nightwear',
      key: 'updated_at',
      type: 'date_time',
      value: String(snapshot.updated_at || new Date().toISOString())
    }
  ];

  const mutation = `
    mutation SyncNightwearShopMetafields($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields {
          id
          key
        }
        userErrors {
          field
          message
          code
        }
      }
    }
  `;

  const data = await adminGraphql(shop, mutation, { metafields });
  const result = data && data.metafieldsSet;
  if (!result) throw new Error('metafields_set_missing_result');
  assertMutationUserErrors(result, 'metafields_set_error');
}

async function ensureMetaobjectDefinition(shop) {
  if (metaobjectDefinitionReady.get(shop) === true) {
    return true;
  }

  const checkQuery = `
    query NightwearDefinition($type: String!) {
      metaobjectDefinitionByType(type: $type) {
        id
        type
      }
    }
  `;

  try {
    const existing = await adminGraphql(shop, checkQuery, { type: STATS_METAOBJECT_TYPE });
    if (existing && existing.metaobjectDefinitionByType && existing.metaobjectDefinitionByType.id) {
      metaobjectDefinitionReady.set(shop, true);
      return true;
    }
  } catch (_error) {
    return false;
  }

  const createMutation = `
    mutation CreateNightwearDefinition($definition: MetaobjectDefinitionCreateInput!) {
      metaobjectDefinitionCreate(definition: $definition) {
        metaobjectDefinition {
          id
          type
        }
        userErrors {
          field
          message
          code
        }
      }
    }
  `;

  const definition = {
    name: 'Nightwear Signal Counts',
    type: STATS_METAOBJECT_TYPE,
    fieldDefinitions: [
      { key: 'founders_total', name: 'Founders total', type: 'number_integer', required: false },
      { key: 'black_share', name: 'Leader share', type: 'number_integer', required: false },
      { key: 'decision_window_hours', name: 'Decision window hours', type: 'number_integer', required: false },
      { key: 'helix_votes', name: 'Helix votes', type: 'number_integer', required: false },
      { key: 'contour_set_votes', name: 'Contour set votes', type: 'number_integer', required: false },
      { key: 'signature_slip_votes', name: 'Signature slip votes', type: 'number_integer', required: false },
      { key: 'v_lace_set_votes', name: 'V-lace set votes', type: 'number_integer', required: false },
      { key: 'helix_pct', name: 'Helix percent', type: 'number_integer', required: false },
      { key: 'contour_set_pct', name: 'Contour set percent', type: 'number_integer', required: false },
      { key: 'signature_slip_pct', name: 'Signature slip percent', type: 'number_integer', required: false },
      { key: 'v_lace_set_pct', name: 'V-lace set percent', type: 'number_integer', required: false },
      { key: 'leader_key', name: 'Leader key', type: 'single_line_text_field', required: false },
      { key: 'leader_label', name: 'Leader label', type: 'single_line_text_field', required: false },
      { key: 'next_update', name: 'Next update', type: 'single_line_text_field', required: false },
      { key: 'updated_at', name: 'Updated at', type: 'date_time', required: false }
    ]
  };

  try {
    const created = await adminGraphql(shop, createMutation, { definition });
    const result = created && created.metaobjectDefinitionCreate;
    if (!result) {
      return false;
    }

    if (result.userErrors && result.userErrors.length) {
      const combined = JSON.stringify(result.userErrors).toLowerCase();
      if (combined.includes('taken') || combined.includes('already')) {
        metaobjectDefinitionReady.set(shop, true);
        return true;
      }
      return false;
    }

    metaobjectDefinitionReady.set(shop, true);
    return true;
  } catch (_error) {
    return false;
  }
}

function buildMetaobjectFields(snapshot) {
  return [
    { key: 'founders_total', value: String(asInteger(snapshot.founders_total)) },
    { key: 'black_share', value: String(asInteger(snapshot.black_share)) },
    { key: 'decision_window_hours', value: String(asInteger(snapshot.decision_window_hours)) },
    { key: 'helix_votes', value: String(asInteger(snapshot.votes.helix || 0)) },
    { key: 'contour_set_votes', value: String(asInteger(snapshot.votes['contour-set'] || 0)) },
    { key: 'signature_slip_votes', value: String(asInteger(snapshot.votes['signature-slip'] || 0)) },
    { key: 'v_lace_set_votes', value: String(asInteger(snapshot.votes['v-lace-set'] || 0)) },
    { key: 'helix_pct', value: String(asInteger(snapshot.looks.helix || 0)) },
    { key: 'contour_set_pct', value: String(asInteger(snapshot.looks['contour-set'] || 0)) },
    { key: 'signature_slip_pct', value: String(asInteger(snapshot.looks['signature-slip'] || 0)) },
    { key: 'v_lace_set_pct', value: String(asInteger(snapshot.looks['v-lace-set'] || 0)) },
    { key: 'leader_key', value: String(snapshot.leader.key || '') },
    { key: 'leader_label', value: String(snapshot.leader.label || '') },
    { key: 'next_update', value: String(snapshot.next_update || '') },
    { key: 'updated_at', value: String(snapshot.updated_at || new Date().toISOString()) }
  ];
}

async function upsertSignalMetaobject(shop, snapshot) {
  const definitionReady = await ensureMetaobjectDefinition(shop);
  if (!definitionReady) {
    return false;
  }

  const byHandleQuery = `
    query NightwearSignalByHandle($type: String!, $handle: String!) {
      metaobjectByHandle(handle: { type: $type, handle: $handle }) {
        id
      }
    }
  `;

  const fields = buildMetaobjectFields(snapshot);
  const current = await adminGraphql(shop, byHandleQuery, {
    type: STATS_METAOBJECT_TYPE,
    handle: STATS_METAOBJECT_HANDLE
  });
  const existingId = current && current.metaobjectByHandle && current.metaobjectByHandle.id;

  if (existingId) {
    const updateMutation = `
      mutation UpdateNightwearSignalMetaobject($id: ID!, $metaobject: MetaobjectUpdateInput!) {
        metaobjectUpdate(id: $id, metaobject: $metaobject) {
          metaobject {
            id
          }
          userErrors {
            field
            message
            code
          }
        }
      }
    `;

    const updated = await adminGraphql(shop, updateMutation, {
      id: existingId,
      metaobject: {
        fields
      }
    });

    const result = updated && updated.metaobjectUpdate;
    if (!result) throw new Error('metaobject_update_missing_result');
    assertMutationUserErrors(result, 'metaobject_update_error');
    return true;
  }

  const createMutation = `
    mutation CreateNightwearSignalMetaobject($metaobject: MetaobjectCreateInput!) {
      metaobjectCreate(metaobject: $metaobject) {
        metaobject {
          id
          handle
        }
        userErrors {
          field
          message
          code
        }
      }
    }
  `;

  const created = await adminGraphql(shop, createMutation, {
    metaobject: {
      type: STATS_METAOBJECT_TYPE,
      handle: STATS_METAOBJECT_HANDLE,
      fields
    }
  });

  const result = created && created.metaobjectCreate;
  if (!result) throw new Error('metaobject_create_missing_result');
  assertMutationUserErrors(result, 'metaobject_create_error');
  return true;
}

async function syncSignalStores(shop, snapshot) {
  const tasks = [syncShopSignalMetafields(shop, snapshot), upsertSignalMetaobject(shop, snapshot)];
  await Promise.allSettled(tasks);
}

async function refreshSignals(shop) {
  const snapshot = await collectNightwearSignals(shop);
  writeCachedStats(shop, snapshot);
  await syncSignalStores(shop, snapshot);
  return snapshot;
}

async function sendConfirmationEmail(email, setLabel) {
  const resendKey = process.env.RESEND_API_KEY || '';
  const from = process.env.VOTE_EMAIL_FROM || '';
  if (!resendKey || !from) {
    return { sent: false, reason: 'not_configured' };
  }

  const html = `
    <div style="font-family:Arial,Helvetica,sans-serif;line-height:1.6;color:#111">
      <h2 style="margin:0 0 12px">KIKI - Founder confirmation</h2>
      <p style="margin:0 0 12px">You are in. Your Nightwear I preference has been recorded.</p>
      <p style="margin:0 0 12px"><strong>Selected set:</strong> ${setLabel}</p>
      <p style="margin:0">You will receive release updates and founder access notices by email.</p>
    </div>
  `;

  const response = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${resendKey}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      from,
      to: [email],
      subject: 'demo confirmation email',
      html
    })
  });

  if (!response.ok) {
    const text = await response.text().catch(() => '');
    return { sent: false, reason: `resend_error:${response.status}:${text}` };
  }

  return { sent: true };
}

async function handleVoteRequest(req, res) {
  if (!isProxyRequestAuthorized(req)) {
    return res.status(401).json({ status: 'error', message: 'invalid_proxy_signature' });
  }

  const shop = sanitizeShop((req.query && req.query.shop) || DEFAULT_SHOP);
  const email = String((req.body && req.body.email) || '')
    .trim()
    .toLowerCase();
  const setKey = normalizeSetKey(
    (req.body && (req.body.set_key || req.body.selected_set || req.body.choice || req.body.setKey)) || ''
  );

  if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    return res.status(400).json({ status: 'error', message: 'invalid_email' });
  }

  if (!setKey) {
    return res.status(400).json({ status: 'error', message: 'invalid_set' });
  }

  const setLabel = setLabelForKey(setKey);

  try {
    let customer = await getCustomerByEmail(shop, email);

    if (!customer) {
      customer = await createCustomer(shop, email, setKey);
      await setVoteMetafield(shop, customer.id, setKey);
      const emailResult = await sendConfirmationEmail(email, setLabel);

      clearStatsCache(shop);
      refreshSignals(shop).catch(() => null);

      return res.json({
        status: 'ok',
        voted_for: setLabel,
        set_key: setKey,
        email_sent: Boolean(emailResult.sent)
      });
    }

    const existingVote = extractExistingVote(
      customer.tags,
      customer.metafield && customer.metafield.value
    );

    if (existingVote) {
      return res.json({
        status: 'already_voted',
        voted_for: setLabelForKey(existingVote),
        set_key: existingVote,
        email_sent: false
      });
    }

    await addTags(shop, customer.id, BASE_TAGS.concat([voteTagForSet(setKey)]));
    await setVoteMetafield(shop, customer.id, setKey);
    const emailResult = await sendConfirmationEmail(email, setLabel);

    clearStatsCache(shop);
    refreshSignals(shop).catch(() => null);

    return res.json({
      status: 'ok',
      voted_for: setLabel,
      set_key: setKey,
      email_sent: Boolean(emailResult.sent)
    });
  } catch (error) {
    return res.status(500).json({
      status: 'error',
      message: error && error.message ? error.message : 'vote_upsert_failed'
    });
  }
}

async function handleStatsRequest(req, res) {
  if (!isProxyRequestAuthorized(req)) {
    return res.status(401).json({ status: 'error', message: 'invalid_proxy_signature' });
  }

  const shop = sanitizeShop((req.query && req.query.shop) || DEFAULT_SHOP);
  const forceRefresh = String((req.query && req.query.refresh) || '') === '1';

  try {
    if (!forceRefresh) {
      const cached = readCachedStats(shop);
      if (cached) {
        return res.json(cached);
      }
    }

    const payload = await refreshSignals(shop);
    return res.json(payload);
  } catch (error) {
    return res.status(500).json({
      status: 'error',
      message: error && error.message ? error.message : 'stats_refresh_failed'
    });
  }
}

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    api_version: API_VERSION,
    proxy_signature_required: !ALLOW_UNSIGNED_PROXY_REQUESTS,
    has_admin_token: Boolean(ADMIN_TOKEN),
    has_shared_secret: Boolean(SHARED_SECRET),
    stats_cache_ttl_ms: STATS_CACHE_TTL_MS
  });
});

app.get('/proxy/nightwear-vote', (_req, res) => {
  res.json({ ok: true, endpoint: 'nightwear-vote' });
});

app.post('/proxy/nightwear-vote', handleVoteRequest);
app.get('/proxy/nightwear-vote/stats', handleStatsRequest);
app.get('/proxy/nightwear-signals/stats', handleStatsRequest);

app.listen(PORT, () => {
  // eslint-disable-next-line no-console
  console.log(`Kiki vote endpoint listening on :${PORT}`);
});
