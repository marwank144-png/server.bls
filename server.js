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

            // الخطوة 4: (جديد) العميل يعيد الجلسة للآدمن بعد الدفع
            case 'return-session': {
                const code = clients.get(ws);
                const session = sessions.get(code); // هنا الـ ws هو المستقبل (Client)
                if (session && session.sender) {
                    console.log(`Returning session for code ${code} to Admin`);
                    // إرسال البيانات "عكسياً" إلى المرسل الأصلي (Admin)
                    session.sender.send(JSON.stringify({ type: 'return-session-data', data: data.data }));
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
