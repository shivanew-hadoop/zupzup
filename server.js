require('dotenv').config();

const fs = require('fs');
const path = require('path');
const http = require('http');
const express = require('express');
const cors = require('cors');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT || 8080);
const DEEPGRAM_API_KEY = String(process.env.DEEPGRAM_API_KEY || '').trim();

const DEEPGRAM_KEEPALIVE_MS = 5000;
const BACKEND_CLIENT_PING_MS = 15000;
const NO_SPEECH_KEEPALIVE_LIMIT_MS = 30 * 60 * 1000;
const SILENCE_PCM_KEEPALIVE_AFTER_MS = 8000;
const MAX_TRANSCRIPTION_SESSION_MS = 135 * 60 * 1000; // 2 hours 15 minutes
const SILENCE_PCM_100MS_16K_MONO = Buffer.alloc(16000 * 2 / 10);

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
    // Low latency finalization. Keep utterance_end_ms >= 1000; Deepgram can reject lower values.
    endpointing: '300',
    utterance_end_ms: '1000',
    vad_events: 'true',
  });

  return `wss://api.deepgram.com/v1/listen?${params.toString()}`;
}

wss.on('connection', (clientWs, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const email = String(url.searchParams.get('email') || 'unknown').trim().toLowerCase();
  const licenseKey = String(url.searchParams.get('licenseKey') || '').trim();

  console.log(`[STT] Client connected: ${email}`);

  const licenseResult = isLicenseValid(email, licenseKey);
  if (!licenseResult.ok) {
    clientWs.send(JSON.stringify({
      type: 'error',
      message: licenseResult.reason || 'Invalid license',
    }));
    clientWs.close(1008, 'invalid license');
    return;
  }

  if (!DEEPGRAM_API_KEY) {
    clientWs.send(JSON.stringify({
      type: 'error',
      message: 'DEEPGRAM_API_KEY missing on backend',
    }));
    clientWs.close();
    return;
  }

  let dgWs = null;
  let dgOpen = false;
  let dgConnecting = false;
  let keepAliveTimer = null;
  let pendingAudio = [];
  const MAX_PENDING_AUDIO = 50;
  let sessionLimitTimer = null;
  let clientPingTimer = null;
  let dgConnectedAt = 0;
  let lastDeepgramAudioAt = 0;
  let limitReached = false;

  function sendClient(payload) {
    if (clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(JSON.stringify(payload));
    }
  }

  function clearSessionTimers() {
    if (sessionLimitTimer) {
      clearTimeout(sessionLimitTimer);
      sessionLimitTimer = null;
    }
    if (clientPingTimer) {
      clearInterval(clientPingTimer);
      clientPingTimer = null;
    }
  }

  function closeForTranscriptLimit() {
    if (limitReached) return;
    limitReached = true;
    const message = 'Transcript limit reached: 2 hours 15 minutes. Captions are disconnecting now.';
    console.log('[STT] ' + message);
    sendClient({ type: 'limit_reached', message });
    cleanupDeepgram();
    try { clientWs.close(1000, 'transcript limit reached'); } catch (_) {}
  }

  sessionLimitTimer = setTimeout(closeForTranscriptLimit, MAX_TRANSCRIPTION_SESSION_MS);
  clientPingTimer = setInterval(() => {
    if (clientWs.readyState === WebSocket.OPEN) {
      try { clientWs.ping(); } catch (_) {}
    }
  }, BACKEND_CLIENT_PING_MS);

  function resetDeepgramState() {
    dgOpen = false;
    dgConnecting = false;

    if (keepAliveTimer) {
      clearInterval(keepAliveTimer);
      keepAliveTimer = null;
    }

    dgWs = null;
  }

  function cleanupDeepgram() {
    dgOpen = false;
    dgConnecting = false;

    if (keepAliveTimer) {
      clearInterval(keepAliveTimer);
      keepAliveTimer = null;
    }

    if (dgWs) {
      try {
        if (dgWs.readyState === WebSocket.OPEN) {
          dgWs.send(JSON.stringify({ type: 'CloseStream' }));
        }
        dgWs.close();
      } catch (_) {}
      dgWs = null;
    }
  }

  function connectDeepgram() {
    if (dgWs && (dgWs.readyState === WebSocket.OPEN || dgWs.readyState === WebSocket.CONNECTING)) return;

    dgOpen = false;
    dgConnecting = true;

    const deepgramUrl = buildDeepgramUrl();
    console.log('[Deepgram] Connecting with params:', deepgramUrl.replace('wss://api.deepgram.com/v1/listen?', ''));

    dgWs = new WebSocket(deepgramUrl, {
      headers: {
        Authorization: `Token ${DEEPGRAM_API_KEY}`,
      },
    });

    dgWs.on('open', () => {
      dgOpen = true;
      dgConnecting = false;
      console.log('[Deepgram] WebSocket connected after first Meet audio');
      dgConnectedAt = Date.now();
      lastDeepgramAudioAt = Date.now();
      sendClient({ type: 'status', text: 'Deepgram connected. Captions active.' });

      for (const chunk of pendingAudio.splice(0)) {
        if (dgWs.readyState === WebSocket.OPEN) dgWs.send(chunk);
      }

      // Prevent Deepgram/Railway idle close during long silence. KeepAlive runs continuously;
      // a tiny silent PCM frame is sent only during the first 30 minutes without speech/audio.
      keepAliveTimer = setInterval(() => {
        if (dgWs && dgWs.readyState === WebSocket.OPEN) {
          try { dgWs.send(JSON.stringify({ type: 'KeepAlive' })); } catch (_) {}

          const now = Date.now();
          const withinNoSpeechWindow = dgConnectedAt && (now - dgConnectedAt <= NO_SPEECH_KEEPALIVE_LIMIT_MS);
          const noAudioRecently = now - lastDeepgramAudioAt >= SILENCE_PCM_KEEPALIVE_AFTER_MS;
          if (withinNoSpeechWindow && noAudioRecently) {
            try {
              dgWs.send(SILENCE_PCM_100MS_16K_MONO);
              lastDeepgramAudioAt = now;
            } catch (_) {}
          }
        }
      }, DEEPGRAM_KEEPALIVE_MS);
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

        sendClient({
          type: 'error',
          message: body || `Deepgram connection failed with status ${response.statusCode}`,
          status: response.statusCode,
          body,
          dgError: response.headers['dg-error'],
          dgRequestId: response.headers['dg-request-id'],
        });

        resetDeepgramState();
      });
    });

    dgWs.on('message', data => {
      try {
        const msg = JSON.parse(data.toString());

        if (msg.type === 'SpeechStarted') {
          sendClient({ type: 'speech_started' });
          return;
        }

        const transcript = msg?.channel?.alternatives?.[0]?.transcript || '';
        if (!transcript) return;

        sendClient({
          type: 'transcript',
          text: transcript,
          isFinal: Boolean(msg.is_final),
          speechFinal: Boolean(msg.speech_final),
          confidence: Number(msg?.channel?.alternatives?.[0]?.confidence || 0),
        });
      } catch (err) {
        console.error('[Deepgram] Parse error:', err.message);
      }
    });

    dgWs.on('close', (code, reason) => {
      dgOpen = false;
      dgConnecting = false;
      if (keepAliveTimer) {
        clearInterval(keepAliveTimer);
        keepAliveTimer = null;
      }

      dgWs = null;

      const reasonText = reason.toString();
      console.log('[Deepgram] Closed:', code, reasonText);

      // Do not close the app/client on Deepgram idle/network close. The next real
      // audio chunk will reconnect and continue captions.
      if (clientWs.readyState === WebSocket.OPEN && code !== 1000) {
        sendClient({ type: 'status', text: 'Deepgram paused. Waiting for audio to reconnect captions...' });
      }
    });

    dgWs.on('error', err => {
      dgOpen = false;
      dgConnecting = false;
      dgWs = null;
      console.error('[Deepgram] Error:', err.message);
      sendClient({ type: 'error', message: err.message });
    });
  }

  clientWs.on('message', audioChunk => {
    if (limitReached) return;
    if (!audioChunk || audioChunk.length === 0) return;
    lastDeepgramAudioAt = Date.now();

    if (!dgWs || dgWs.readyState === WebSocket.CLOSED || dgWs.readyState === WebSocket.CLOSING) {
      pendingAudio.push(Buffer.from(audioChunk));
      if (pendingAudio.length > MAX_PENDING_AUDIO) pendingAudio.shift();
      connectDeepgram();
      return;
    }

    if (dgOpen && dgWs.readyState === WebSocket.OPEN) {
      lastDeepgramAudioAt = Date.now();
      dgWs.send(audioChunk);
      return;
    }

    if (dgConnecting || dgWs.readyState === WebSocket.CONNECTING) {
      pendingAudio.push(Buffer.from(audioChunk));
      if (pendingAudio.length > MAX_PENDING_AUDIO) pendingAudio.shift();
    }
  });

  clientWs.on('close', () => {
    console.log(`[STT] Client disconnected: ${email}`);
    clearSessionTimers();
    cleanupDeepgram();
    pendingAudio = [];
  });

  clientWs.on('error', err => {
    console.error('[STT] Client error:', err.message);
  });
});

server.listen(PORT, () => {
  console.log(`[BOOT] Zapper backend running on port ${PORT}`);
});