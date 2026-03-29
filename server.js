// server.js — Proxy Manager & Session Sync — Full Session Control Protocol
'use strict';
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

// sessions: Map<code, { sender, receiver, startTime, status }>
const sessions = new Map();
// wsToAdminCodes: Map<ws, Set<code>> — admin owns these codes
const wsToAdminCodes = new Map();
// wsToJoinedCode: Map<ws, code> — client joined this code
const wsToJoinedCode = new Map();

const OPEN = 1;

function safeSend(ws, payload) {
  try { if (ws && ws.readyState === OPEN) ws.send(JSON.stringify(payload)); } catch (e) {}
}

function pushSessionsUpdate(adminWs) {
  const codes = wsToAdminCodes.get(adminWs);
  if (!codes) return;
  const list = [];
  for (const code of codes) {
    const s = sessions.get(code);
    if (!s) continue;
    list.push({ code, status: s.status, startTime: s.startTime, clientConnected: !!s.receiver });
  }
  safeSend(adminWs, { type: 'sessions-update', sessions: list });
}

// Clean a session fully
function cleanSession(code) {
  const s = sessions.get(code);
  if (!s) return;
  if (s.sender) {
    const codes = wsToAdminCodes.get(s.sender);
    if (codes) { codes.delete(code); if (codes.size === 0) wsToAdminCodes.delete(s.sender); }
  }
  if (s.receiver) wsToJoinedCode.delete(s.receiver);
  sessions.delete(code);
}

console.log(`✅ Signaling Server running on port ${PORT}`);

wss.on('connection', ws => {
  console.log('🔌 New WS connection');

  ws.on('message', raw => {
    let data;
    try { data = JSON.parse(raw); } catch (e) { return; }
    console.log(`📩 [${data.type}]`);

    switch (data.type) {

      // ── Admin: generate pairing code ──
      case 'generate-code': {
        let code;
        do { code = Math.random().toString(36).substring(2, 8).toUpperCase(); }
        while (sessions.has(code));

        sessions.set(code, { sender: ws, receiver: null, startTime: Date.now(), status: 'waiting' });

        if (!wsToAdminCodes.has(ws)) wsToAdminCodes.set(ws, new Set());
        wsToAdminCodes.get(ws).add(code);

        safeSend(ws, { type: 'code-generated', code });
        pushSessionsUpdate(ws);
        console.log(`🔑 Code ${code} generated`);
        break;
      }

      // ── Client: join a session with code ──
      case 'join-session': {
        const code = (data.code || '').toUpperCase().trim();
        const session = sessions.get(code);

        if (!session) {
          safeSend(ws, { type: 'session-error', message: 'الرمز غير صحيح أو منتهي الصلاحية.' });
          break;
        }
        if (session.receiver) {
          safeSend(ws, { type: 'session-error', message: 'هذه الجلسة ممتلئة بالفعل.' });
          break;
        }

        session.receiver = ws;
        session.status = 'paired';
        wsToJoinedCode.set(ws, code);

        // Notify both sides (include code so popup can track)
        safeSend(session.sender, { type: 'session-paired', code });
        safeSend(ws, { type: 'session-paired', code });
        pushSessionsUpdate(session.sender);
        console.log(`🔗 Session ${code} paired`);
        break;
      }

      // ── Admin → Client: send sync data ──
      case 'sync-data': {
        // Find which of admin's sessions has a receiver
        const adminCodes = wsToAdminCodes.get(ws);
        if (!adminCodes) break;

        // Try specific code first, else find any paired session
        let targetCode = data.code;
        let session = targetCode ? sessions.get(targetCode) : null;
        if (!session) {
          for (const c of adminCodes) {
            const s = sessions.get(c);
            if (s && s.receiver) { session = s; targetCode = c; break; }
          }
        }
        if (session && session.receiver) {
          session.status = 'synced';
          safeSend(session.receiver, { type: 'sync-data', data: data.data });
          pushSessionsUpdate(ws);
        }
        break;
      }

      // ── Admin: request current sessions list ──
      case 'get-sessions': {
        pushSessionsUpdate(ws);
        break;
      }

      // ── Admin: terminate a specific session ──
      case 'terminate-session': {
        const code = (data.code || '').toUpperCase().trim();
        const adminCodes = wsToAdminCodes.get(ws);
        if (!adminCodes || !adminCodes.has(code)) {
          safeSend(ws, { type: 'session-error', message: 'لا تملك صلاحية إنهاء هذه الجلسة.' });
          break;
        }
        const session = sessions.get(code);
        if (session) {
          // Notify client of forced termination
          if (session.receiver) {
            safeSend(session.receiver, { type: 'session-terminated', message: 'تم إنهاء الجلسة من قِبل الإداري.' });
          }
          cleanSession(code);
        }
        // ACK to admin
        safeSend(ws, { type: 'session-terminated-ack', code });
        pushSessionsUpdate(ws);
        console.log(`🔴 Session ${code} terminated by admin`);
        break;
      }

      // ── Client → Admin: return session (callback) ──
      case 'return-session': {
        const joinedCode = wsToJoinedCode.get(ws);
        const session = joinedCode ? sessions.get(joinedCode) : null;
        if (session && session.sender) {
          safeSend(session.sender, { type: 'return-session-data', data: data.data });
        }
        break;
      }
    }
  });

  ws.on('close', () => {
    console.log('🔴 WS disconnected');

    // If admin disconnects → terminate all their sessions and notify clients
    const adminCodes = wsToAdminCodes.get(ws);
    if (adminCodes) {
      for (const code of [...adminCodes]) {
        const session = sessions.get(code);
        if (session && session.receiver) {
          safeSend(session.receiver, { type: 'session-terminated', message: 'انقطع اتصال الإداري.' });
          wsToJoinedCode.delete(session.receiver);
        }
        sessions.delete(code);
      }
      wsToAdminCodes.delete(ws);
    }

    // If client disconnects → update admin
    const joinedCode = wsToJoinedCode.get(ws);
    if (joinedCode) {
      const session = sessions.get(joinedCode);
      if (session) {
        session.receiver = null;
        session.status = 'waiting';
        pushSessionsUpdate(session.sender);
      }
      wsToJoinedCode.delete(ws);
    }
  });

  ws.on('error', err => console.error('WS error:', err.message));
});
