// server.js
const WebSocket = require('ws');

// إنشاء خادم WebSocket على المنفذ 8080
const wss = new WebSocket.Server({ port: 8080 });

// لتخزين الجلسات والاتصالات
const sessions = new Map();
const clients = new Map();

console.log('Signaling Server is running on port 8080...');

wss.on('connection', ws => {
    console.log('Client connected');

    ws.on('message', message => {
        const data = JSON.parse(message);
        console.log('Received message:', data.type);

        switch (data.type) {
            // الخطوة 1: المرسل يطلب إنشاء رمز جديد
            case 'generate-code': {
                let code;
                // تأكد من أن الرمز فريد
                do {
                    code = Math.random().toString(36).substring(2, 8).toUpperCase();
                } while (sessions.has(code));

                const session = { sender: ws, receiver: null };
                sessions.set(code, session);
                clients.set(ws, code);

                ws.send(JSON.stringify({ type: 'code-generated', code: code }));
                console.log(`Generated code ${code}`);
                break;
            }

            // الخطوة 2: المستقبل يحاول الانضمام باستخدام الرمز
            case 'join-session': {
                const code = data.code;
                const session = sessions.get(code);

                if (session && !session.receiver) {
                    session.receiver = ws;
                    clients.set(ws, code);

                    // إعلام الطرفين بأنه تم الاقتران بنجاح
                    session.sender.send(JSON.stringify({ type: 'session-paired' }));
                    session.receiver.send(JSON.stringify({ type: 'session-paired' }));
                    console.log(`Session ${code} paired.`);
                } else {
                    ws.send(JSON.stringify({ type: 'session-error', message: 'Invalid or full code' }));
                }
                break;
            }

            // الخطوة 3: تمرير بيانات المزامنة من المرسل إلى المستقبل
            case 'sync-data': {
                const code = clients.get(ws);
                const session = sessions.get(code);
                if (session && session.receiver) {
                    // إعادة توجيه البيانات إلى المستقبل
                    session.receiver.send(JSON.stringify({ type: 'sync-data', data: data.data }));
                }
                break;
            }
            
            // ----------------------------------------------------
            // الميزة الجديدة 1: طلب قائمة الأكواد النشطة من المشرف
            // ----------------------------------------------------
            case 'request-active-codes': {
                const activeCodes = [];
                // نبحث عن الجلسات التي تم فيها إقران (receiver !== null)
                sessions.forEach((session, code) => {
                    if (session.receiver) {
                        activeCodes.push(code);
                    }
                });
                // نرسل القائمة إلى المشرف الطالب
                ws.send(JSON.stringify({ type: 'active-codes-list', codes: activeCodes }));
                console.log(`Sent active codes list (${activeCodes.length})`);
                break;
            }

            // ----------------------------------------------------
            // الميزة الجديدة 2: توجيه أمر الفصل القسري إلى العميل
            // ----------------------------------------------------
            case 'admin-force-disconnect': {
                const codeToClose = data.code;
                const sessionToClose = sessions.get(codeToClose);
                
                if (sessionToClose && sessionToClose.receiver) {
                    // نرسل أمر 'admin-disconnect' إلى العميل (المستقبل) لقطع الاتصال
                    sessionToClose.receiver.send(JSON.stringify({ type: 'admin-disconnect' }));
                    
                    // إشعار المشرف بأن الأمر تم إرساله
                    ws.send(JSON.stringify({ type: 'admin-action-success', message: `Disconnect command sent to ${codeToClose}` }));
                    console.log(`Sent force disconnect command for ${codeToClose}`);
                } else {
                    ws.send(JSON.stringify({ type: 'session-error', message: `Code ${codeToClose} not active or invalid.` }));
                }
                break;
            }
            
        }
    });

    ws.on('close', () => {
        console.log('Client disconnected');
        const code = clients.get(ws);
        if (code) {
            sessions.delete(code);
            clients.delete(ws);
            console.log(`Session ${code} closed.`);
        }
    });
});
