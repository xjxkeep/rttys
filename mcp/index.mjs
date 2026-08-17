#!/usr/bin/env node

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
import WebSocket from 'ws';
import { readFileSync, statSync, writeFileSync, createReadStream } from 'fs';
import { basename } from 'path';

// --- Constants (same as frontend RttyTerm.vue) ---
const MsgTypeFileData = 0x03;
const ReadFileBlkSize = 63 * 1024;
const AckBlkSize = 4 * 1024;
const FileReadyMarker = Buffer.from('\x1eRTTY_FILE_READY\x1f');
const FilePrepareCommand = "stty -echo; printf '\\036RTTY_FILE_READY\\037'\r";
const FileReceiveCommand = 'rtty -R\r';
const FileRestoreCommand = 'stty echo\r';
const FileHandshakeAttempts = 3;

// --- HTTP helpers ---

class RttysClient {
  constructor(baseURL) {
    this.baseURL = baseURL.replace(/\/+$/, '');
    this.cookies = '';
  }

  async signin(password) {
    const resp = await fetch(`${this.baseURL}/signin`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ password }),
      redirect: 'manual',
    });

    if (resp.status !== 200) {
      throw new Error(`authentication failed (status ${resp.status})`);
    }

    // Extract cookies from set-cookie headers
    const setCookies = resp.headers.getSetCookie?.() || [];
    this.cookies = setCookies.map(c => c.split(';')[0]).join('; ');
  }

  async get(path) {
    const resp = await fetch(`${this.baseURL}${path}`, {
      headers: { Cookie: this.cookies },
    });
    if (resp.status !== 200) {
      throw new Error(`request failed (status ${resp.status})`);
    }
    return resp.json();
  }

  async postJSON(path, payload) {
    const resp = await fetch(`${this.baseURL}${path}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Cookie: this.cookies,
      },
      body: JSON.stringify(payload),
    });
    return resp.json();
  }

  connectWS(devid, group = '') {
    const u = new URL(this.baseURL);
    const scheme = u.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsURL = `${scheme}//${u.host}/connect/${devid}?group=${group}`;

    return new Promise((resolve, reject) => {
      const ws = new WebSocket(wsURL, {
        headers: { Cookie: this.cookies },
      });

      const timeout = setTimeout(() => {
        ws.close();
        reject(new Error('websocket login timeout'));
      }, 10000);

      ws.on('error', (err) => {
        clearTimeout(timeout);
        reject(err);
      });

      ws.on('message', (data, isBinary) => {
        if (!isBinary) {
          try {
            const msg = JSON.parse(data.toString());
            if (msg.type === 'login') {
              clearTimeout(timeout);
              resolve(ws);
              return;
            }
          } catch {}
        }
        // If not login, check for close codes
        // Just ignore non-login messages during handshake
      });

      ws.on('close', (code) => {
        clearTimeout(timeout);
        if (code === 4000) reject(new Error('device offline'));
        else if (code === 4001) reject(new Error('device busy'));
        else if (code === 4002) reject(new Error('login timeout'));
        else reject(new Error(`websocket closed: ${code}`));
      });
    });
  }
}

// --- Wait for JSON message (mirrors frontend's message handling) ---

function waitForJSONMsg(ws, msgType, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    let unack = 0;

    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(`timeout waiting for ${msgType}`));
    }, timeoutMs);

    function onMessage(data, isBinary) {
      if (isBinary) {
        // Must send terminal data ACKs, same as frontend (RttyTerm.vue:445-453).
        // Without ACKs, device stops reading PTY → rtty -R blocks on printf →
        // FIFO fills up → daemon blocks on notify_progress() → event loop dead.
        const buf = Buffer.from(data);
        if (buf.length > 0 && buf[0] === 0) {
          unack += buf.length - 1;
          if (unack > AckBlkSize) {
            ws.send(JSON.stringify({ type: 'ack', ack: unack }));
            unack = 0;
          }
        }
        return;
      }
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type === msgType) {
          cleanup();
          resolve(msg);
        }
      } catch {}
    }

    function onClose(code) {
      cleanup();
      reject(new Error(`connection closed (${code}) while waiting for ${msgType}`));
    }

    function onError(err) {
      cleanup();
      reject(err);
    }

    function cleanup() {
      clearTimeout(timeout);
      ws.off('message', onMessage);
      ws.off('close', onClose);
      ws.off('error', onError);
    }

    ws.on('message', onMessage);
    ws.on('close', onClose);
    ws.on('error', onError);
  });
}

function sendTerminalCommand(ws, command) {
  ws.send(Buffer.concat([Buffer.from([0]), Buffer.from(command)]));
}

function waitForTerminalMarker(ws, marker, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    let unack = 0;
    let window = Buffer.alloc(0);

    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error('timeout waiting for terminal readiness'));
    }, timeoutMs);

    function onMessage(data, isBinary) {
      if (!isBinary) return;

      const buf = Buffer.from(data);
      if (buf.length < 2 || buf[0] !== 0) return;

      const payload = buf.subarray(1);
      unack += payload.length;
      if (unack > AckBlkSize) {
        ws.send(JSON.stringify({ type: 'ack', ack: unack }));
        unack = 0;
      }

      window = Buffer.concat([window, payload]);
      if (window.includes(marker)) {
        cleanup();
        resolve();
        return;
      }

      const keep = Math.max(0, marker.length - 1);
      if (window.length > keep) window = window.subarray(window.length - keep);
    }

    function onClose(code) {
      cleanup();
      reject(new Error(`connection closed (${code}) while preparing terminal`));
    }

    function onError(err) {
      cleanup();
      reject(err);
    }

    function cleanup() {
      clearTimeout(timeout);
      ws.off('message', onMessage);
      ws.off('close', onClose);
      ws.off('error', onError);
    }

    ws.on('message', onMessage);
    ws.on('close', onClose);
    ws.on('error', onError);
  });
}

function waitForShellPrompt(ws, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    let unack = 0;
    let window = Buffer.alloc(0);

    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(`timeout waiting for remote shell (last output: ${JSON.stringify(window.toString())})`));
    }, timeoutMs);

    function onMessage(data, isBinary) {
      if (!isBinary) return;
      const buf = Buffer.from(data);
      if (buf.length < 2 || buf[0] !== 0) return;

      const payload = buf.subarray(1);
      unack += payload.length;
      if (unack > AckBlkSize) {
        ws.send(JSON.stringify({ type: 'ack', ack: unack }));
        unack = 0;
      }

      window = Buffer.concat([window, payload]);
      const text = window.toString().replace(/[\r\n]+$/, '');
      if (text.endsWith('# ') || text.endsWith('$ ') || text.endsWith('> ')) {
        cleanup();
        resolve();
        return;
      }
      if (window.length > 512) window = window.subarray(window.length - 512);
    }

    function onClose(code) {
      cleanup();
      reject(new Error(`connection closed (${code}) while waiting for remote shell`));
    }

    function onError(err) {
      cleanup();
      reject(err);
    }

    function cleanup() {
      clearTimeout(timeout);
      ws.off('message', onMessage);
      ws.off('close', onClose);
      ws.off('error', onError);
    }

    ws.on('message', onMessage);
    ws.on('close', onClose);
    ws.on('error', onError);
  });
}

function shellQuote(value) {
  return `'${value.replaceAll("'", `'"'"'`)}'`;
}

async function prepareFileReceive(client, devid, group) {
  let lastError;

  for (let attempt = 1; attempt <= FileHandshakeAttempts; attempt++) {
    let ws;
    try {
      ws = await client.connectWS(devid, group);
      await waitForShellPrompt(ws, 10000);
      sendTerminalCommand(ws, FilePrepareCommand);
      await waitForTerminalMarker(ws, FileReadyMarker, 10000);
      sendTerminalCommand(ws, FileReceiveCommand);
      await waitForJSONMsg(ws, 'recvfile', 30000);
      return ws;
    } catch (err) {
      lastError = err;
      ws?.close();
      if (attempt < FileHandshakeAttempts) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
  }

  throw new Error(`prepare device file receiver after ${FileHandshakeAttempts} attempts: ${lastError?.message || lastError}`);
}

async function prepareFileSend(client, devid, group, remotePath) {
  let lastError;

  for (let attempt = 1; attempt <= FileHandshakeAttempts; attempt++) {
    let ws;
    try {
      ws = await client.connectWS(devid, group);
      await waitForShellPrompt(ws, 10000);
      sendTerminalCommand(ws, FilePrepareCommand);
      await waitForTerminalMarker(ws, FileReadyMarker, 10000);
      sendTerminalCommand(ws, `rtty -S ${shellQuote(remotePath)}\r`);
      const message = await waitForJSONMsg(ws, 'sendfile', 30000);
      return { ws, message };
    } catch (err) {
      lastError = err;
      ws?.close();
      if (attempt < FileHandshakeAttempts) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
  }

  throw new Error(`prepare device file sender after ${FileHandshakeAttempts} attempts: ${lastError?.message || lastError}`);
}

// --- Upload file (mirrors frontend RttyTerm.vue doUploadFile logic) ---

async function uploadFile(client, devid, group, filePath) {
  const stat = statSync(filePath);
  if (stat.size > 0xFFFFFFFF) {
    throw new Error('file too large (max 4GB)');
  }

  const ws = await prepareFileReceive(client, devid, group);

  try {
    const fileInfo = JSON.stringify({
      type: 'fileInfo',
      size: stat.size,
      name: basename(filePath),
    });
    ws.send(fileInfo);

    if (stat.size === 0) {
      ws.send(Buffer.from([1, MsgTypeFileData]));
      await new Promise(resolve => setTimeout(resolve, 500));
      sendTerminalCommand(ws, FileRestoreCommand);
      return 0;
    }

    const fileData = readFileSync(filePath);
    let offset = 0;
    let sent = 0;

    while (offset < fileData.length) {
      const end = Math.min(offset + ReadFileBlkSize, fileData.length);
      const chunk = fileData.subarray(offset, end);

      // Same as frontend: sendFileData -> new Uint8Array([1, MsgTypeFileData, ...data])
      const msg = Buffer.alloc(2 + chunk.length);
      msg[0] = 1;
      msg[1] = MsgTypeFileData;
      chunk.copy(msg, 2);
      ws.send(msg);

      sent += chunk.length;
      offset = end;

      // Device only sends fileAck for non-last chunks (file.c:406-409).
      // When remain_size==0, device calls file_context_reset() without ack.
      if (offset < fileData.length) {
        await waitForJSONMsg(ws, 'fileAck', 30000);
      }
    }

    // Give device time to finish writing before we close
    await new Promise(resolve => setTimeout(resolve, 500));
    sendTerminalCommand(ws, FileRestoreCommand);

    return sent;
  } finally {
    ws.close();
  }
}

// --- Download file (mirrors frontend file receive logic) ---

async function downloadFile(client, devid, group, remotePath, outputPath) {
  const { ws, message: sendFileMsg } = await prepareFileSend(client, devid, group, remotePath);

  try {
    const filename = sendFileMsg.name || basename(remotePath);

    // Send initial fileAck
    ws.send(JSON.stringify({ type: 'fileAck' }));

    const savePath = outputPath || filename;

    // Receive file chunks (mirrors frontend binary message handler)
    const chunks = [];
    let received = 0;
    let terminalUnack = 0;

    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('download timeout'));
      }, 60000);

      ws.on('message', (data, isBinary) => {
        if (!isBinary) return;

        const buf = Buffer.from(data);
        if (buf.length === 0) return;

        if (buf[0] === 0) {
          terminalUnack += buf.length - 1;
          if (terminalUnack > AckBlkSize) {
            ws.send(JSON.stringify({ type: 'ack', ack: terminalUnack }));
            terminalUnack = 0;
          }
          return;
        }

        // File transfer end signal: single byte [1] (same as frontend: data.length === 1)
        if (buf.length === 1) {
          clearTimeout(timeout);
          resolve();
          return;
        }

        // File data chunk: [1, ...chunk] (same as frontend: data.slice(1))
        const chunk = buf.subarray(1);
        chunks.push(chunk);
        received += chunk.length;

        // Send fileAck (same as frontend)
        ws.send(JSON.stringify({ type: 'fileAck' }));
      });

      ws.on('close', () => {
        clearTimeout(timeout);
        reject(new Error('connection closed during download'));
      });

      ws.on('error', (err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });

    writeFileSync(savePath, Buffer.concat(chunks));
    await new Promise(resolve => setTimeout(resolve, 500));
    sendTerminalCommand(ws, FileRestoreCommand);
    return { filename: savePath, received };
  } finally {
    ws.close();
  }
}

// --- MCP Server ---

const mcpServer = new McpServer({
  name: 'rttys',
  version: '2.0.0',
});

const serverParam = z.string().describe('rttys 服务器地址');
const passwordParam = z.string().describe('rttys 登录密码');
const groupParam = z.string().optional().describe('设备分组');

async function createClient(server, password) {
  const client = new RttysClient(server);
  await client.signin(password);
  return client;
}

// list_devices
mcpServer.tool(
  'list_devices',
  '列出在线设备',
  {
    server: serverParam,
    password: passwordParam,
    group: groupParam,
  },
  async ({ server, password, group }) => {
    try {
      const client = await createClient(server, password);
      const devs = await client.get(`/devs?group=${group || ''}`);
      return { content: [{ type: 'text', text: JSON.stringify(devs, null, 2) }] };
    } catch (err) {
      return { content: [{ type: 'text', text: `Error: ${err.message}` }], isError: true };
    }
  }
);

// list_groups
mcpServer.tool(
  'list_groups',
  '列出设备分组',
  {
    server: serverParam,
    password: passwordParam,
  },
  async ({ server, password }) => {
    try {
      const client = await createClient(server, password);
      const groups = await client.get('/groups');
      return { content: [{ type: 'text', text: JSON.stringify(groups, null, 2) }] };
    } catch (err) {
      return { content: [{ type: 'text', text: `Error: ${err.message}` }], isError: true };
    }
  }
);

// exec_command
mcpServer.tool(
  'exec_command',
  '在远程设备上执行命令',
  {
    server: serverParam,
    password: passwordParam,
    device_id: z.string().describe('设备 ID'),
    command: z.string().describe('要执行的命令'),
    user: z.string().optional().default('root').describe('登录用户名'),
    group: groupParam,
    wait: z.number().optional().default(30).describe('超时秒数'),
  },
  async ({ server, password, device_id, command, user, group, wait: waitSec }) => {
    try {
      const client = await createClient(server, password);
      const payload = { cmd: command, username: user || 'root', params: [] };
      const path = `/cmd/${device_id}?group=${group || ''}&wait=${waitSec || 30}`;
      const result = await client.postJSON(path, payload);

      const output = {
        exit_code: result.code || 0,
        stdout: result.stdout ? Buffer.from(result.stdout, 'base64').toString() : '',
        stderr: result.stderr ? Buffer.from(result.stderr, 'base64').toString() : '',
      };

      if (result.err) {
        output.error = `command error: ${result.msg}`;
      }

      return { content: [{ type: 'text', text: JSON.stringify(output, null, 2) }] };
    } catch (err) {
      return { content: [{ type: 'text', text: `Error: ${err.message}` }], isError: true };
    }
  }
);

// upload_file
mcpServer.tool(
  'upload_file',
  '上传本地文件到远程设备',
  {
    server: serverParam,
    password: passwordParam,
    device_id: z.string().describe('设备 ID'),
    file_path: z.string().describe('本地文件路径'),
    group: groupParam,
  },
  async ({ server, password, device_id, file_path, group }) => {
    try {
      const client = await createClient(server, password);
      const sent = await uploadFile(client, device_id, group || '', file_path);
      return {
        content: [{ type: 'text', text: `uploaded ${file_path} (${sent} bytes)` }],
      };
    } catch (err) {
      return { content: [{ type: 'text', text: `upload failed: ${err.message}` }], isError: true };
    }
  }
);

// download_file
mcpServer.tool(
  'download_file',
  '从远程设备下载文件到本地',
  {
    server: serverParam,
    password: passwordParam,
    device_id: z.string().describe('设备 ID'),
    remote_path: z.string().describe('设备上的文件路径'),
    output_path: z.string().optional().describe('本地保存路径'),
    group: groupParam,
  },
  async ({ server, password, device_id, remote_path, output_path, group }) => {
    try {
      const client = await createClient(server, password);
      const result = await downloadFile(client, device_id, group || '', remote_path, output_path || '');
      return {
        content: [{ type: 'text', text: `downloaded ${result.filename} (${result.received} bytes)` }],
      };
    } catch (err) {
      return { content: [{ type: 'text', text: `download failed: ${err.message}` }], isError: true };
    }
  }
);

// Start server
const transport = new StdioServerTransport();
await mcpServer.connect(transport);
