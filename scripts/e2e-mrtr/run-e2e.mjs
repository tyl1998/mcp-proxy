#!/usr/bin/env node
/**
 * MRTR E2E 全链路驱动（B7）
 *
 * 链路：驱动(模拟 Java streamable client) → proxy /mcp/stream/proxy/{key}/mcp
 *       → streamable 腿(vendored rmcp-soddygo: params 顶层 inputResponses 透传
 *       + MrtrHeaderClient 三头注入) → mock 后端(真实契约)。
 *
 * 覆盖：
 *  T1 initialize（legacy 版本回答）+ tools/list
 *  T2 第一回合：envelope 无 inputResponses → input_required + 嵌套 params 表单
 *  T3 第二回合 accept（action 字段）→ deleted:true
 *  T4 第二回合 decline → in-band 拒绝终态(1003)
 *  T5 第二回合旧字段名 kind → 拒绝（契约回归：kind 在真实 wire 上不存在）
 *  T6 三头断言：mock 收到的 envelope 请求均带 Mcp-Method/Mcp-Name/
 *     MCP-Protocol-Version（B3 + vendored 补丁的联合验证）
 *
 * 前置：mcp-proxy（含 Phase B 改动）运行于 $PROXY_URL（默认 http://localhost:8020）。
 * 用法：node run-e2e.mjs
 */
import { spawn } from 'node:child_process';
import fs from 'node:fs';

const PROXY_URL = process.env.PROXY_URL || 'http://localhost:8020';
const MOCK_PORT = Number(process.env.MRTR_MOCK_PORT || 3100);
const HEADER_LOG = process.env.MRTR_HEADER_LOG || '/tmp/mrtr-mock-headers.jsonl';
const MCP_ID = 'e2e-mrtr-' + Date.now();

const ENVELOPE = {
  'io.modelcontextprotocol/protocolVersion': '2026-07-28',
  'io.modelcontextprotocol/clientCapabilities': { elicitation: {} },
};

let passed = 0, failed = 0;
const ok = (name, cond, detail = '') => {
  if (cond) { passed++; console.log(`  ✅ ${name}`); }
  else { failed++; console.log(`  ❌ ${name} ${detail}`); }
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let sessionId = null;

async function postProxy(body, opts = {}) {
  const serverConfig = JSON.stringify({
    mcpServers: {
      [MCP_ID]: {
        url: `http://host.docker.internal:${MOCK_PORT}/mcp`,
        type: 'stream',
      },
    },
  });
  const headers = {
    'content-type': 'application/json',
    accept: 'application/json, text/event-stream',
    'x-mcp-json': Buffer.from(serverConfig).toString('base64'),
    'x-mcp-type': 'OneShot',
  };
  // proxy 的 stream 服务是 stateful 模式：initialize 后带 Mcp-Session-Id
  if (sessionId && !opts.noSession) headers['mcp-session-id'] = sessionId;
  const res = await fetch(`${PROXY_URL}/mcp/stream/proxy/${MCP_ID}/mcp`, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
  const newSession = res.headers.get('mcp-session-id');
  if (newSession) sessionId = newSession;
  const text = await res.text();
  let parsed = null;
  try {
    parsed = JSON.parse(text);
  } catch {
    // SSE 响应：按事件块（空行分隔）拼接 data: 行，取第一个有效 JSON-RPC 消息
    // （首个 data: 行可能是空 keepalive，不能只取第一行）
    let event = [];
    const flush = () => {
      if (parsed || event.length === 0) { event = []; return; }
      const data = event.join('\n');
      event = [];
      if (!data.trim()) return;
      try {
        const msg = JSON.parse(data);
        if (msg && (msg.result !== undefined || msg.error !== undefined)) parsed = msg;
      } catch {}
    };
    for (const line of text.split('\n')) {
      if (line === '') { flush(); continue; }
      if (line.startsWith('data:')) event.push(line.slice(5).replace(/^ /, ''));
    }
    flush();
  }
  return { status: res.status, body: parsed, raw: text };
}

const req = (id, method, params) => ({ jsonrpc: '2.0', id, method, params });

async function main() {
  console.log(`E2E MRTR against proxy=${PROXY_URL} mcpId=${MCP_ID}`);
  try { fs.rmSync(HEADER_LOG, { force: true }); } catch {}

  // 0. 拉起 mock 后端（宿主机）
  const mock = spawn('node', [new URL('./mock-server.mjs', import.meta.url).pathname, String(MOCK_PORT)], {
    env: { ...process.env, MRTR_HEADER_LOG: HEADER_LOG },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  mock.stdout.on('data', (d) => process.stdout.write('   [mock] ' + d));
  mock.stderr.on('data', (d) => process.stderr.write('   [mock!] ' + d));
  const cleanup = () => { try { mock.kill(); } catch {} };
  process.on('exit', cleanup);
  process.on('SIGINT', () => { cleanup(); process.exit(1); });
  await sleep(600);

  // 1. initialize（模拟 Java transport 握手；proxy 对后端也会自行握手）
  console.log('\n[T1] initialize + tools/list');
  let r = await postProxy(req(1, 'initialize', {
    protocolVersion: '2025-11-25',
    capabilities: {},
    clientInfo: { name: 'e2e-driver', version: '1.0' },
  }));
  ok('initialize HTTP 200', r.status === 200, `status=${r.status} raw=${r.raw?.slice(0, 200)}`);
  ok('initialize answers legacy version', r.body?.result?.protocolVersion === '2025-11-25',
    `got=${r.body?.result?.protocolVersion}`);
  r = await postRpcNotification('notifications/initialized');

  r = await postProxy(req(2, 'tools/list', {}));
  ok('tools/list returns delete_environment',
    (r.body?.result?.tools || []).some((t) => t.name === 'delete_environment'),
    `raw=${r.raw?.slice(0, 300)}`);

  // 2. 第一回合：envelope、无 inputResponses
  console.log('\n[T2] first round → input_required (result 顶层 resultType，真实 2026 codec 形状)');
  const arguments_ = { environmentId: 'staging' };
  r = await postRpcCall(10, arguments_, undefined);
  const result1 = r.body?.result;
  ok('HTTP 200', r.status === 200, `status=${r.status}`);
  ok('result.resultType=input_required（顶层）', result1?.resultType === 'input_required',
    `result=${JSON.stringify(result1)?.slice(0, 200)}`);
  const entry = result1?.inputRequests?.confirm;
  ok('inputRequests.confirm exists（顶层）', !!entry);
  ok('form nested under params (embedded request shape)',
    !!entry?.params && typeof entry.params.message === 'string' && !!entry.params.requestedSchema,
    `entry=${JSON.stringify(entry)?.slice(0, 200)}`);
  ok('message carries consequence list', /Delete environment/.test(entry?.params?.message || ''));

  // 3. 第二回合 accept（action 字段名；arguments 必须与第一回合完全一致）
  console.log('\n[T3] second round accept (action) → deleted');
  r = await postRpcCall(11, arguments_, {
    confirm: { action: 'accept', content: { confirm: true } },
  });
  const result2 = r.body?.result;
  ok('deleted:true', result2?.structuredContent?.deleted === true && result2?.resultType === 'complete', `result=${JSON.stringify(result2)?.slice(0, 250)}`);

  // 4. 第二回合 decline
  console.log('\n[T4] second round decline → refused terminal state');
  r = await postRpcCall(12, arguments_, { confirm: { action: 'decline' } });
  ok('isError in-band refusal', r.body?.result?.isError === true, `body=${JSON.stringify(r.body)?.slice(0, 300)}`);
  ok('refusal mentions 1003', /1003/.test(r.body?.result?.content?.[0]?.text || ''));

  // 5. 旧字段名 kind（Phase A 错误契约回归：应被视为拒绝，不得误删）
  console.log('\n[T5] second round with legacy `kind` field → refused (regression)');
  r = await postRpcCall(13, arguments_, {
    confirm: { kind: 'accept', content: { confirm: true } },
  });
  ok('kind is not accepted (refused)', r.body?.result?.isError === true, `body=${JSON.stringify(r.body)?.slice(0, 300)}`);

  // 6. 三头断言（B3 + vendored 补丁联合验证）
  console.log('\n[T6] Mcp-* header assertions (from mock header log)');
  await sleep(300);
  let headerEntries = [];
  try {
    headerEntries = fs.readFileSync(HEADER_LOG, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l));
  } catch (e) {
    ok('header log readable', false, String(e));
  }
  // 注意：proxy→mock 的 envelope 请求 = 每个 tools/call（T2/T3/T4/T5 各一）；
  // initialize/tools/list 是 proxy 自己发起的（非 envelope，无三头，正常）
  const envelopeSeen = headerEntries.filter((h) => h.protocolVersionHeader !== null || h.mcpMethodHeader !== null);
  const toolsCallSeen = envelopeSeen.filter((h) => h.mcpMethodHeader === 'tools/call');
  ok('proxy forwarded 4 envelope tools/call rounds', toolsCallSeen.length === 4,
    `count=${toolsCallSeen.length}, all=${JSON.stringify(headerEntries)}`);
  ok('all envelope rounds carry Mcp-Method: tools/call',
    toolsCallSeen.every((h) => h.mcpMethodHeader === 'tools/call'));
  ok('all envelope rounds carry Mcp-Name: delete_environment',
    toolsCallSeen.every((h) => h.mcpNameHeader === 'delete_environment'),
    `names=${toolsCallSeen.map((h) => h.mcpNameHeader).join(',')}`);
  ok('all envelope rounds carry MCP-Protocol-Version: 2026-07-28',
    toolsCallSeen.every((h) => h.protocolVersionHeader === '2026-07-28'),
    `versions=${toolsCallSeen.map((h) => h.protocolVersionHeader).join(',')}`);

  console.log(`\n===== E2E result: ${passed} passed, ${failed} failed =====`);
  cleanup();
  process.exit(failed === 0 ? 0 : 1);
}

async function postRpcNotification(method) {
  return postProxy({ jsonrpc: '2.0', method });
}

async function postRpcCall(id, args, inputResponses) {
  const params = {
    name: 'delete_environment',
    arguments: args,
    _meta: { ...ENVELOPE },
  };
  if (inputResponses !== undefined) {
    params.inputResponses = inputResponses;
  }
  return postProxy(req(id, 'tools/call', params));
}

main().catch((e) => { console.error('E2E driver crashed:', e); process.exit(1); });
