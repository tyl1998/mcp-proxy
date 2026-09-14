#!/usr/bin/env node
/**
 * MRTR E2E mock 后端（MCP 协议修订 2026-07-28，真实契约模拟）
 *
 * 模拟 apitest `/mcp` 的 stateless dual-era 行为（TS SDK 2.0.0 实测契约，
 * 见《MCP-删除二次确认-MRTR总方案》§2）：
 *  - initialize 永远答 legacy 版本（2025-11-25）
 *  - per-request envelope（params._meta 两个必填键）激活 MRTR 分类
 *  - envelope 请求强制标准头校验：Mcp-Method / Mcp-Name(tools/call) /
 *    MCP-Protocol-Version（与 body envelope 交叉一致），缺失/不一致 = 400 -32020
 *  - tools/call 三态：无 inputResponses = input_required（嵌套 params 表单）；
 *    action:"accept" 且 content.confirm===true = 删除成功；其余 = 拒绝终态(1003)
 *  - 每个请求的头快照写入 $MRTR_HEADER_LOG（供驱动断言 B3 三头注入）
 *
 * 用法：node mock-server.mjs [port]   （默认 3100）
 */
import http from 'node:http';
import fs from 'node:fs';

const PORT = Number(process.argv[2] || process.env.MRTR_MOCK_PORT || 3100);
const HEADER_LOG = process.env.MRTR_HEADER_LOG || '/tmp/mrtr-mock-headers.jsonl';
const ENVELOPE_VERSION_KEY = 'io.modelcontextprotocol/protocolVersion';
const ENVELOPE_CAPABILITIES_KEY = 'io.modelcontextprotocol/clientCapabilities';

const log = (...a) => console.log('[mock]', ...a);
const headerLog = (entry) => {
  try { fs.appendFileSync(HEADER_LOG, JSON.stringify(entry) + '\n'); } catch {}
};

const json = (res, status, body) => {
  const payload = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(payload);
};

const jsonrpcError = (res, id, code, message) =>
  json(res, 400, { jsonrpc: '2.0', id: id ?? null, error: { code, message } });

function hasEnvelopeClaim(params) {
  return !!params && typeof params === 'object' && !!params._meta &&
    typeof params._meta === 'object' && ENVELOPE_VERSION_KEY in params._meta;
}

function validateEnvelopeMeta(meta) {
  const issues = [];
  if (!(ENVELOPE_VERSION_KEY in meta)) issues.push({ key: ENVELOPE_VERSION_KEY, problem: 'missing' });
  else if (typeof meta[ENVELOPE_VERSION_KEY] !== 'string') issues.push({ key: ENVELOPE_VERSION_KEY, problem: 'not a string' });
  if (!(ENVELOPE_CAPABILITIES_KEY in meta)) issues.push({ key: ENVELOPE_CAPABILITIES_KEY, problem: 'missing' });
  return issues;
}

/** 模拟 TS SDK classifyRequestBody + validateStandardRequestHeaders 的 ladder 拒绝 */
function classifyRequest(req, body) {
  const params = body.params;
  const method = body.method;
  const headerVersion = req.headers['mcp-protocol-version'];
  if (!hasEnvelopeClaim(params)) {
    return { kind: 'legacy', method };
  }
  const meta = params._meta;
  const issues = validateEnvelopeMeta(meta);
  if (issues.length > 0) {
    return { reject: { code: -32602, message: `Invalid _meta envelope for protocol revision 2026-07-28: ${issues[0].key}: ${issues[0].problem}` } };
  }
  const claimed = meta[ENVELOPE_VERSION_KEY];
  if (headerVersion !== undefined && headerVersion !== claimed) {
    return { reject: { code: -32020, message: `the body envelope names protocol version ${claimed} but the MCP-Protocol-Version header names ${headerVersion}` } };
  }
  // modern route：标准头校验（SEP-2243）
  const mcpMethod = req.headers['mcp-method'];
  if (mcpMethod === undefined) {
    return { reject: { code: -32020, message: `the body names method ${method} but the required Mcp-Method header is absent` } };
  }
  if (mcpMethod !== method) {
    return { reject: { code: -32020, message: `the body names method ${method} but the Mcp-Method header names ${mcpMethod}` } };
  }
  if (method === 'tools/call') {
    const mcpName = req.headers['mcp-name'];
    const bodyName = typeof params.name === 'string' ? params.name : undefined;
    if (mcpName === undefined && bodyName !== undefined) {
      return { reject: { code: -32020, message: `the body carries params.name="${bodyName}" but the required Mcp-Name header is absent` } };
    }
    if (mcpName !== undefined && bodyName !== undefined && mcpName !== bodyName) {
      return { reject: { code: -32020, message: `the body carries params.name="${bodyName}" but the Mcp-Name header names "${mcpName}"` } };
    }
  }
  return { kind: 'modern', method, params };
}

/** mrtrGate 三态判定（apitest mcpToolResult.ts:105 模拟） */
function mrtrGate(params) {
  const responses = params.inputResponses;
  if (responses === undefined) return { state: 'first' };
  const entry = responses && typeof responses === 'object' ? responses.confirm : undefined;
  const accept = entry && typeof entry === 'object' &&
    entry.action === 'accept' &&
    entry.content && entry.content.confirm === true;
  if (accept) return { state: 'confirmed' };
  // 有回应但不是确认（decline/cancel/空对象/形状不对/kind 字段名）一律拒绝
  return { state: 'refused' };
}

const server = http.createServer((req, res) => {
  if (req.method === 'GET') {
    // streamable GET（服务端→客户端流）：不支持，405（rmcp 容忍并跳过）
    res.writeHead(405, { allow: 'POST, DELETE' });
    res.end();
    return;
  }
  if (req.method === 'DELETE') {
    res.writeHead(200);
    res.end();
    return;
  }
  if (req.method !== 'POST') {
    res.writeHead(405);
    res.end();
    return;
  }
  let raw = '';
  req.on('data', (c) => (raw += c));
  req.on('end', () => {
    let body;
    try {
      body = JSON.parse(raw);
    } catch {
      return jsonrpcError(res, null, -32600, 'Bad Request: the request body is not valid JSON');
    }
    // 头快照（供 E2E 断言三头）
    headerLog({
      t: Date.now(),
      method: body.method,
      mcpMethodHeader: req.headers['mcp-method'] ?? null,
      mcpNameHeader: req.headers['mcp-name'] ?? null,
      protocolVersionHeader: req.headers['mcp-protocol-version'] ?? null,
    });
    log('POST', body.method, 'id=' + body.id,
      'headers{Mcp-Method=' + (req.headers['mcp-method'] ?? '-') +
      ', Mcp-Name=' + (req.headers['mcp-name'] ?? '-') +
      ', MCP-Protocol-Version=' + (req.headers['mcp-protocol-version'] ?? '-') + '}');

    if (body.method === 'initialize') {
      // §2.1：initialize 永远答 legacy（stateless 代理无法走 2025 降级腿）
      return json(res, 200, {
        jsonrpc: '2.0', id: body.id,
        result: {
          protocolVersion: '2025-11-25',
          capabilities: { tools: { listChanged: false } },
          serverInfo: { name: 'mrtr-mock', version: '1.0.0' },
        },
      });
    }
    if (body.method === 'notifications/initialized' || body.method?.startsWith('notifications/')) {
      res.writeHead(202);
      res.end();
      return;
    }
    if (body.method === 'tools/list') {
      return json(res, 200, {
        jsonrpc: '2.0', id: body.id,
        result: {
          tools: [{
            name: 'delete_environment',
            description: 'Delete an environment (MRTR-gated)',
            inputSchema: {
              type: 'object',
              properties: { environmentId: { type: 'string' } },
              required: ['environmentId'],
            },
          }],
        },
      });
    }
    if (body.method === 'tools/call') {
      const route = classifyRequest(req, body);
      if (route.reject) {
        log('  -> ladder rejection:', route.reject.message);
        return jsonrpcError(res, body.id, route.reject.code, route.reject.message);
      }
      if (route.kind === 'legacy') {
        // 模拟 legacy 腿对 input_required 的桥接失败（无状态连接没有客户端能力声明）
        return json(res, 200, {
          jsonrpc: '2.0', id: body.id,
          result: {
            content: [{ type: 'text', text: "Cannot request input 'confirm' (elicitation/create): the client on this 2025-era connection did not declare the required capability" }],
            isError: true,
          },
        });
      }
      const gate = mrtrGate(route.params);
      if (gate.state === 'first') {
        // 真实 2026 wire codec 形状（与 apitest/TS SDK 2.0.0 实测一致）：
        // resultType/inputRequests 在 **result 顶层**，不在 structuredContent 里，
        // 无 content；_meta 带 serverInfo
        return json(res, 200, {
          jsonrpc: '2.0', id: body.id,
          result: {
            resultType: 'input_required',
            inputRequests: {
              confirm: {
                method: 'elicitation/create',
                params: {
                  message: 'Delete environment "staging"? 2 endpoint(s) reference variables only this environment supplies; 145 execution(s) ran with it; they keep their name snapshot.',
                  requestedSchema: {
                    type: 'object',
                    properties: { confirm: { type: 'boolean', description: 'set to true to delete; anything else cancels' } },
                    required: ['confirm'],
                  },
                  mode: 'form',
                },
              },
            },
            _meta: { 'io.modelcontextprotocol/serverInfo': { name: 'mrtr-mock', version: '1.0.0' } },
          },
        });
      }
      if (gate.state === 'confirmed') {
        const payload = { resultType: 'complete', deleted: true, affectedEndpoints: 0 };
        return json(res, 200, {
          jsonrpc: '2.0', id: body.id,
          result: {
            content: [{ type: 'text', text: JSON.stringify({ deleted: true, affectedEndpoints: 0 }) }],
            structuredContent: { deleted: true, affectedEndpoints: 0 },
            resultType: 'complete',
            _meta: { 'io.modelcontextprotocol/serverInfo': { name: 'mrtr-mock', version: '1.0.0' } },
          },
        });
      }
      // refused：in-band 工具错误（HTTP 200，isError: true），终态不重问
      return json(res, 200, {
        jsonrpc: '2.0', id: body.id,
        result: {
          content: [{ type: 'text', text: 'deletion was not confirmed by the human; environment kept intact (1003)' }],
          isError: true,
          resultType: 'complete',
          _meta: { 'io.modelcontextprotocol/serverInfo': { name: 'mrtr-mock', version: '1.0.0' } },
        },
      });
    }
    return jsonrpcError(res, body.id, -32601, `Method not found: ${body.method}`);
  });
});

server.listen(PORT, () => log(`MRTR mock backend listening on :${PORT}, header log -> ${HEADER_LOG}`));
