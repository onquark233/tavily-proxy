/**
 * Key pool management for Tavily API keys stored in Cloudflare KV.
 *
 * KV schema: key = Tavily API key (e.g. "tvly-xxx"), value = remaining credit (number as string).
 */

export interface KeyInfo {
  apiKey: string;
  remainingCredit: number;
}

interface UsageResponse {
  key: {
    usage: number;
    limit: number | null;
  };
  account: {
    plan_limit: number;
    plan_usage: number;
  };
}

export interface SyncAllKeyUsageResult {
  updated: number;
  failed: number;
}

function requireFiniteNumber(value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`Invalid Tavily usage response: ${field} is not a finite number`);
  }
  return value;
}

/**
 * Query the Tavily /usage endpoint to get remaining credits for a key.
 */
export async function queryRemainingCredit(apiKey: string): Promise<number> {
  const keyPrefix = apiKey.substring(0, 13);
  const res = await fetch("https://api.tavily.com/usage", {
    headers: {
      Authorization: `Bearer ${apiKey}`,
    },
  });

  if (!res.ok) {
    const text = await res.text();
    console.log(`[usage] key=${keyPrefix}... response=${res.status} ${text}`);
    throw new Error(`Failed to query usage for key: ${res.status} ${text}`);
  }

  const data = (await res.json()) as UsageResponse;

  console.log(`[usage] key=${keyPrefix}... response=${JSON.stringify(data)}`);

  let limit: number;
  let usage: number;
  if (data.key.limit === null) {
    limit = requireFiniteNumber(data.account.plan_limit, "account.plan_limit");
    usage = requireFiniteNumber(data.account.plan_usage, "account.plan_usage");
  } else {
    limit = requireFiniteNumber(data.key.limit, "key.limit");
    usage = requireFiniteNumber(data.key.usage, "key.usage");
  }

  return Math.max(0, limit - usage);
}

/**
 * List all keys from KV using cached values (no Tavily API call).
 */
async function listKeysFromCache(kv: KVNamespace): Promise<KeyInfo[]> {
  const keys: KeyInfo[] = [];
  let cursor: string | undefined;

  do {
    const result = await kv.list({ cursor });
    for (const key of result.keys) {
      const value = await kv.get(key.name);
      keys.push({
        apiKey: key.name,
        remainingCredit: value ? Number(value) : 0,
      });
    }
    cursor = result.list_complete ? undefined : result.cursor;
  } while (cursor);

  return keys;
}

/**
 * List all keys from KV with their remaining credits (reads from cache only).
 */
export async function listKeys(kv: KVNamespace): Promise<KeyInfo[]> {
  return listKeysFromCache(kv);
}

/**
 * Pick a random key from all keys with remaining credit > 0.
 * Returns null if no keys are available.
 */
export async function pickBestKey(kv: KVNamespace): Promise<string | null> {
  const keys = await listKeysFromCache(kv);
  const candidates = keys.filter(k => k.remainingCredit > 0);
  if (candidates.length === 0) return null;

  const picked = candidates[Math.floor(Math.random() * candidates.length)];
  return picked.apiKey;
}

/**
 * Add or update a key in KV. Queries the Tavily API for current remaining credit.
 */
export async function addKey(kv: KVNamespace, apiKey: string): Promise<KeyInfo> {
  const remaining = await queryRemainingCredit(apiKey);
  await kv.put(apiKey, String(remaining));
  return { apiKey, remainingCredit: remaining };
}

/**
 * Delete a key from KV.
 */
export async function deleteKey(kv: KVNamespace, apiKey: string): Promise<void> {
  await kv.delete(apiKey);
}

/**
 * Deduct credit from a key after a request.
 * This is a best-effort local update; we periodically re-sync from the API.
 */
export async function deductCredit(kv: KVNamespace, apiKey: string, amount: number): Promise<void> {
  const current = await kv.get(apiKey);
  if (current !== null) {
    const newVal = Math.max(0, Number(current) - amount);
    await kv.put(apiKey, String(newVal));
  }
}

/**
 * Set a key's remaining credit to 0 in KV (e.g. on 401/403 errors).
 */
export async function invalidateKey(kv: KVNamespace, apiKey: string): Promise<void> {
  console.log(`[pool] Invalidating key ${apiKey.substring(0, 13)}... (setting credit to -1000)`);
  await kv.put(apiKey, "-1000");
}

/**
 * Refresh every key from Tavily in bounded batches. Failed queries retain their
 * existing cached values so a transient usage API failure cannot disable a key.
 */
export async function syncAllKeyUsage(kv: KVNamespace): Promise<SyncAllKeyUsageResult> {
  const concurrency = 5;
  let cursor: string | undefined;
  let updated = 0;
  let failed = 0;

  do {
    const page = await kv.list({ cursor });

    for (let i = 0; i < page.keys.length; i += concurrency) {
      const batch = page.keys.slice(i, i + concurrency);
      const results = await Promise.allSettled(
        batch.map(async ({ name: apiKey }) => {
          const remaining = await queryRemainingCredit(apiKey);
          await kv.put(apiKey, String(remaining));
        })
      );

      results.forEach((result, index) => {
        if (result.status === "fulfilled") {
          updated++;
          return;
        }

        failed++;
        const apiKey = batch[index].name;
        console.error(
          `[cron] Failed to sync key ${apiKey.substring(0, 13)}..., keeping cached value:`,
          result.reason
        );
      });
    }

    cursor = page.list_complete ? undefined : page.cursor;
  } while (cursor);

  console.log(`[cron] Usage sync complete: updated=${updated}, failed=${failed}`);
  return { updated, failed };
}
