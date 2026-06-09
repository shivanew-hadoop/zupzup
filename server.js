require('dotenv').config();

const fs = require('fs');
const path = require('path');
const http = require('http');
const express = require('express');
const cors = require('cors');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT || 8080);
const DEEPGRAM_API_KEY = String(process.env.DEEPGRAM_API_KEY || '').trim();

const USERS_FILE = path.join(__dirname, 'users.json');

if (!DEEPGRAM_API_KEY) {
  console.warn('[BOOT] WARNING: DEEPGRAM_API_KEY missing');
} else {
  console.log('[BOOT] DEEPGRAM_API_KEY present: true');
  console.log('[BOOT] DEEPGRAM_API_KEY length:', DEEPGRAM_API_KEY.length);
}

const app = express();
app.use(cors());
app.use(express.json());

function loadUsers() {
  if (!fs.existsSync(USERS_FILE)) return {};
  return JSON.parse(fs.readFileSync(USERS_FILE, 'utf8'));
}

function isLicenseValid(email, licenseKey) {
  const users = loadUsers();
  const user = users[email];

  if (!user) return { ok: false, reason: 'User not found' };
  if (!user.active) return { ok: false, reason: 'License inactive' };
  if (user.licenseKey !== licenseKey) return { ok: false, reason: 'Invalid license key' };

  const today = new Date();
  const validTill = new Date(`${user.validTill}T23:59:59`);

  if (Number.isNaN(validTill.getTime()) || validTill < today) {
    return { ok: false, reason: 'License expired' };
  }

  return {
    ok: true,
    user: {
      email,
      name: user.name,
      plan: user.plan,
      validTill: user.validTill,
      active: user.active,
    },
  };
}

app.get('/', (req, res) => {
  res.json({
    ok: true,
    service: 'Zapper Backend',
    stt: '/stt',
  });
});

app.get('/health', (req, res) => {
  res.json({ ok: true });
});

app.post('/validate-license', (req, res) => {
  const email = String(req.body.email || '').trim().toLowerCase();
  const licenseKey = String(req.body.licenseKey || '').trim();

  if (!email || !licenseKey) {
    return res.status(400).json({ ok: false, reason: 'email and licenseKey required' });
  }

  const result = isLicenseValid(email, licenseKey);
  return res.status(result.ok ? 200 : 401).json(result);
});

const server = http.createServer(app);

const wss = new WebSocket.Server({
  server,
  path: '/stt',
});

function buildDeepgramUrl() {
  const params = new URLSearchParams({
    model: 'nova-2',
    language: 'en-US',
    encoding: 'linear16',
    sample_rate: '16000',
    channels: '1',
    interim_results: 'true',
    punctuate: 'true',
    smart_format: 'true',
    endpointing: '100',
  });

  return `wss://api.deepgram.com/v1/listen?${params.toString()}`;
}

wss.on('connection', (clientWs, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const email = String(url.searchParams.get('email') || 'unknown').trim().toLowerCase();

  console.log(`[STT] Client connected: ${email}`);

  if (!DEEPGRAM_API_KEY) {
    clientWs.send(JSON.stringify({
      type: 'error',
      message: 'DEEPGRAM_API_KEY missing on backend',
    }));
    clientWs.close();
    return;
  }

  const dgWs = new WebSocket(buildDeepgramUrl(), {
    headers: {
      Authorization: `Token ${DEEPGRAM_API_KEY}`,
    },
  });

  let dgOpen = false;

  dgWs.on('open', () => {
    dgOpen = true;
    console.log('[Deepgram] WebSocket connected');
  });

  dgWs.on('unexpected-response', (request, response) => {
    let body = '';

    response.on('data', chunk => {
      body += chunk.toString();
    });

    response.on('end', () => {
      console.error('[Deepgram] Unexpected response');
      console.error('[Deepgram] Status:', response.statusCode);
      console.error('[Deepgram] Headers:', response.headers);
      console.error('[Deepgram] Body:', body);

      if (clientWs.readyState === WebSocket.OPEN) {
        clientWs.send(JSON.stringify({
          type: 'error',
          message: 'Deepgram connection failed',
          status: response.statusCode,
          body,
          dgError: response.headers['dg-error'],
          dgRequestId: response.headers['dg-request-id'],
        }));
      }
    });
  });

  dgWs.on('message', data => {
    try {
      const msg = JSON.parse(data.toString());

      const transcript =
        msg?.channel?.alternatives?.[0]?.transcript || '';

      if (!transcript) return;

      const payload = {
        type: 'transcript',
        text: transcript,
        isFinal: Boolean(msg.is_final),
        speechFinal: Boolean(msg.speech_final),
      };

      if (clientWs.readyState === WebSocket.OPEN) {
        clientWs.send(JSON.stringify(payload));
      }
    } catch (err) {
      console.error('[Deepgram] Parse error:', err.message);
    }
  });

  dgWs.on('close', (code, reason) => {
    dgOpen = false;
    console.log('[Deepgram] Closed:', code, reason.toString());

    if (clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(JSON.stringify({
        type: 'deepgram_closed',
        code,
        reason: reason.toString(),
      }));
    }
  });

  dgWs.on('error', err => {
    console.error('[Deepgram] Error:', err.message);

    if (clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(JSON.stringify({
        type: 'error',
        message: err.message,
      }));
    }
  });

  clientWs.on('message', audioChunk => {
    if (!audioChunk || audioChunk.length === 0) return;

    if (dgOpen && dgWs.readyState === WebSocket.OPEN) {
      dgWs.send(audioChunk);
    }
  });

  clientWs.on('close', () => {
    console.log(`[STT] Client disconnected: ${email}`);

    if (dgWs.readyState === WebSocket.OPEN) {
      dgWs.send(JSON.stringify({ type: 'CloseStream' }));
      dgWs.close();
    }
  });

  clientWs.on('error', err => {
    console.error('[STT] Client error:', err.message);
  });
});

server.listen(PORT, () => {
  console.log(`[BOOT] Zapper backend running on port ${PORT}`);
});