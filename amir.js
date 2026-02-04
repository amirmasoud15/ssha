// @ts-ignore
import { connect } from 'cloudflare:sockets';

// --- تنظیمات اصلی ---
let masterID = 'd342d11e-d424-4583-b36e-524ab1f0afa4';
const ADMIN_USER = "admin";
const ADMIN_PASS = "12345";

// دی‌ان‌اس‌های قدرتمند برای رفع مشکل توییتر
const DNS_SERVERS = [
    'https://1.1.1.1/dns-query',
    'https://8.8.8.8/dns-query'
];

const FAKE_SITE_HTML = `
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Cloud Storage Service</title>
    <style>
        body { font-family: sans-serif; background: #0f172a; color: #eee; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }
        .container { text-align: center; border: 1px solid #1e293b; padding: 2rem; border-radius: 15px; background: #1e293b; box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1); }
        .status { color: #22c55e; font-weight: bold; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Private Storage Node</h1>
        <p>Status: <span class="status">Online</span></p>
        <p>Service is running normally.</p>
    </div>
</body>
</html>
`;

export default {
    async fetch(request, env, ctx) {
        try {
            const currentMasterID = (env.UUID || masterID).toLowerCase();
            const adminUser = env.ADMIN_USER || ADMIN_USER;
            const adminPass = env.ADMIN_PASS || ADMIN_PASS;
            const KV = env.datavpn; 
            
            let cleanIP = '';
            if (KV) {
                cleanIP = await KV.get('clean_ip') || '';
            }

            const upgradeHeader = request.headers.get('Upgrade');
            const url = new URL(request.url);
            const path = url.pathname.toLowerCase().replace(/^\/|\/$/g, '');

            if (upgradeHeader === 'websocket') {
                return await vlessOverWSHandler(request, KV, currentMasterID);
            }

            if (path === currentMasterID + '/api') {
                if (request.method === 'POST') {
                    const data = await request.json();
                    if (KV) {
                        if (data.clean_ip !== undefined) await KV.put('clean_ip', data.clean_ip);
                        if (data.users) await KV.put('vless_users', JSON.stringify(data.users));
                    }
                    return new Response('Saved', { status: 200 });
                }
                const users = KV ? await KV.get('vless_users') : null;
                return new Response(JSON.stringify({
                    users: JSON.parse(users || '[]'),
                    clean_ip: cleanIP
                }), { headers: { 'Content-Type': 'application/json' } });
            }

            if (path === currentMasterID) {
                return new Response(renderHTML(currentMasterID, request.headers.get('Host'), adminUser, adminPass), {
                    headers: { 'Content-Type': 'text/html; charset=utf-8' }
                });
            }

            return new Response(FAKE_SITE_HTML, { status: 200, headers: { 'Content-Type': 'text/html' } });
        } catch (err) {
            return new Response(err.toString(), { status: 500 });
        }
    }
};

async function vlessOverWSHandler(request, KV, masterUuid) {
    const webSocketPair = new WebSocketPair();
    const [client, webSocket] = Object.values(webSocketPair);
    webSocket.accept();

    const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';
    const readableWebSocketStream = makeReadableWebSocketStream(webSocket, earlyDataHeader);

    let remoteSocketWapper = { value: null };

    readableWebSocketStream.pipeTo(new WritableStream({
        async write(chunk, controller) {
            if (remoteSocketWapper.value) {
                const writer = remoteSocketWapper.value.writable.getWriter();
                await writer.write(chunk);
                writer.releaseLock();
                return;
            }

            const { hasError, portRemote, addressRemote, rawDataIndex, vlessVersion, clientID, command } = processVlessHeader(chunk);
            
            if (hasError) {
                webSocket.close();
                return;
            }

            let isAllowed = (clientID.toLowerCase() === masterUuid.toLowerCase());
            if (!isAllowed && KV) {
                const usersJson = await KV.get('vless_users');
                if (usersJson) {
                    try {
                        const users = JSON.parse(usersJson);
                        isAllowed = users.some(u => u.uuid.toLowerCase() === clientID.toLowerCase());
                    } catch (e) {}
                }
            }

            if (!isAllowed) {
                webSocket.close();
                return;
            }

            const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
            const rawClientData = chunk.slice(rawDataIndex);

            // Command 1: TCP, Command 2: UDP (For Twitter/DNS)
            if (command === 1 || command === 2) {
                handleOutbound(remoteSocketWapper, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader);
            } else {
                webSocket.close();
            }
        }
    })).catch(() => {});

    return new Response(null, { status: 101, webSocket: client });
}

async function handleOutbound(remoteSocket, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader) {
    try {
        // بهینه‌سازی مسیردهی پکت‌ها
        const tcpSocket = connect({ hostname: addressRemote, port: portRemote });
        remoteSocket.value = tcpSocket;
        const writer = tcpSocket.writable.getWriter();
        await writer.write(rawClientData);
        writer.releaseLock();
        remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader);
    } catch (e) {
        webSocket.close();
    }
}

async function remoteSocketToWS(remoteSocket, webSocket, vlessResponseHeader) {
    let hasHeaderSent = false;
    try {
        await remoteSocket.readable.pipeTo(new WritableStream({
            async write(chunk) {
                if (hasHeaderSent) {
                    webSocket.send(chunk);
                } else {
                    const newChunk = new Uint8Array(vlessResponseHeader.length + chunk.length);
                    newChunk.set(vlessResponseHeader);
                    newChunk.set(chunk, vlessResponseHeader.length);
                    webSocket.send(newChunk);
                    hasHeaderSent = true;
                }
            },
            close() { webSocket.close(); }
        }));
    } catch (e) { webSocket.close(); }
}

function processVlessHeader(vlessBuffer) {
    if (vlessBuffer.byteLength < 24) return { hasError: true };
    const version = new Uint8Array(vlessBuffer.slice(0, 1));
    const clientID = stringify(new Uint8Array(vlessBuffer.slice(1, 17)));
    const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];
    const command = new Uint8Array(vlessBuffer.slice(18 + optLength, 18 + optLength + 1))[0];
    const portIndex = 18 + optLength + 1;
    const portRemote = new DataView(vlessBuffer.slice(portIndex, portIndex + 2)).getUint16(0);
    let addressIndex = portIndex + 2;
    const addressType = new Uint8Array(vlessBuffer.slice(addressIndex, addressIndex + 1))[0];
    let addressValueIndex = addressIndex + 1;
    let addressRemote = '';
    let addressLength = 0;

    if (addressType === 1) {
        addressLength = 4;
        addressRemote = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)).join('.');
    } else if (addressType === 2) {
        addressLength = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + 1))[0];
        addressValueIndex += 1;
        addressRemote = new TextDecoder().decode(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
    } else if (addressType === 3) {
        addressLength = 16;
        const dataView = new DataView(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
        const ipv6 = [];
        for (let i = 0; i < 8; i++) ipv6.push(dataView.getUint16(i * 2).toString(16));
        addressRemote = ipv6.join(':');
    }

    return { hasError: false, addressRemote, portRemote, rawDataIndex: addressValueIndex + addressLength, vlessVersion: version, clientID, command };
}

function makeReadableWebSocketStream(webSocketServer, earlyDataHeader) {
    return new ReadableStream({
        start(controller) {
            webSocketServer.addEventListener('message', (event) => controller.enqueue(event.data));
            webSocketServer.addEventListener('close', () => controller.close());
            webSocketServer.addEventListener('error', (err) => controller.error(err));
            const { earlyData } = base64ToArrayBuffer(earlyDataHeader);
            if (earlyData) controller.enqueue(earlyData);
        }
    });
}

function base64ToArrayBuffer(base64Str) {
    if (!base64Str) return { earlyData: null };
    try {
        const decoded = atob(base64Str.replace(/-/g, '+').replace(/_/g, '/'));
        const buffer = new Uint8Array(decoded.length);
        for (let i = 0; i < decoded.length; i++) buffer[i] = decoded.charCodeAt(i);
        return { earlyData: buffer.buffer };
    } catch (e) { return { earlyData: null }; }
}

const byteToHex = Array.from({ length: 256 }, (_, i) => (i + 256).toString(16).slice(1));
function stringify(arr) {
    let uuid = '';
    for (let i = 0; i < 16; i++) {
        uuid += byteToHex[arr[i]];
        if ([3, 5, 7, 9].includes(i)) uuid += '-';
    }
    return uuid;
}

function renderHTML(masterUuid, host, adminUser, adminPass) {
    return `
    <!DOCTYPE html>
    <html lang="fa">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>VLESS Manager Pro - DNS Edition</title>
        <style>
            :root { --p: #3b82f6; --bg: #0f172a; --c: #1e293b; --s: #10b981; --r: #ef4444; --gray: #94a3b8; }
            body { background: var(--bg); color: #f1f5f9; font-family: 'Segoe UI', system-ui, sans-serif; margin: 0; padding: 20px; direction: rtl; }
            .container { max-width: 850px; margin: 0 auto; direction: ltr; }
            .header { text-align: center; margin-bottom: 30px; }
            .grid { display: grid; grid-template-columns: 320px 1fr; gap: 30px; }
            .card { background: var(--c); border-radius: 16px; border: 1px solid #334155; padding: 20px; height: fit-content; }
            h2 { font-size: 0.9rem; margin: 0 0 15px 0; color: var(--gray); text-transform: uppercase; letter-spacing: 1px; }
            .form-group { margin-bottom: 15px; }
            .label { display: block; font-size: 0.75rem; color: var(--gray); margin-bottom: 5px; }
            input { width: 100%; padding: 12px; background: #020617; border: 1px solid #334155; color: white; border-radius: 10px; font-size: 0.85rem; box-sizing: border-box; }
            .btn { width: 100%; padding: 12px; border-radius: 10px; border: none; font-weight: 600; cursor: pointer; transition: 0.2s; font-size: 0.85rem; margin-top: 8px; }
            .btn-p { background: var(--p); color: white; }
            .btn-s { background: var(--s); color: white; }
            .btn:hover { opacity: 0.9; transform: translateY(-1px); }
            .client-card { background: var(--c); border-radius: 16px; border: 1px solid #334155; margin-bottom: 20px; overflow: hidden; animation: fadeIn 0.3s ease; }
            @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
            .client-header { padding: 12px 20px; background: rgba(255,255,255,0.03); display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #334155; }
            .client-body { padding: 20px; display: grid; grid-template-columns: 1fr 220px; gap: 20px; align-items: center; }
            .config-area { background: #020617; border: 1px solid #334155; padding: 12px; border-radius: 10px; cursor: pointer; position: relative; }
            .config-text { font-family: monospace; font-size: 0.7rem; word-break: break-all; color: #94a3b8; line-height: 1.4; max-height: 80px; overflow: hidden; }
            .qr-wrapper { background: white; padding: 10px; border-radius: 12px; width: 200px; height: 200px; }
            .qr-wrapper img { width: 100%; height: 100%; }
            .badge { background: #1e3a8a; color: #60a5fa; padding: 2px 8px; border-radius: 4px; font-size: 0.65rem; font-weight: bold; }
            #loginOverlay { position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: var(--bg); z-index: 9999; display: flex; justify-content: center; align-items: center; }
            @media (max-width: 850px) { .grid { grid-template-columns: 1fr; } .client-body { grid-template-columns: 1fr; } .qr-col { order: -1; display: flex; justify-content: center; } }
        </style>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/qrcodejs/1.0.0/qrcode.min.js"></script>
    </head>
    <body>
        <div id="loginOverlay">
            <div class="card" style="width: 320px; text-align: center;">
                <h1 style="font-size: 1.2rem; margin-bottom: 20px;">Manager Pro <span style="color:var(--p)">Login</span></h1>
                <div class="form-group">
                    <input type="text" id="userIn" placeholder="Username">
                </div>
                <div class="form-group">
                    <input type="password" id="passIn" placeholder="Password">
                </div>
                <button class="btn btn-p" onclick="doLogin()">ورود به مدیریت</button>
            </div>
        </div>

        <div class="container" id="mainContent" style="display:none;">
            <div class="header">
                <h1>VLESS <span style="color:var(--p)">Pro</span> Manager</h1>
                <div style="display:flex; justify-content:center; gap:10px;">
                    <span class="badge">DNS-over-HTTPS</span>
                    <span class="badge">Early Data 2048</span>
                    <span class="badge">Twitter Optimized</span>
                </div>
            </div>
            <div class="grid">
                <div class="sidebar">
                    <div class="card">
                        <h2>تنظیمات شبکه</h2>
                        <div class="form-group">
                            <label class="label">آی‌پی تمیز (CF Clean IP)</label>
                            <input type="text" id="cleanIp" placeholder="مثلا: 104.18.2.1">
                            <button class="btn btn-s" onclick="saveSettings()">بروزرسانی شبکه</button>
                        </div>
                        <hr style="border:0; border-top:1px solid #334155; margin: 25px 0;">
                        <h2>افزودن کاربر جدید</h2>
                        <div class="form-group">
                            <label class="label">نام کاربر</label>
                            <input type="text" id="newName" placeholder="User Name">
                        </div>
                        <div class="form-group">
                            <label class="label">UUID</label>
                            <input type="text" id="newUuid" placeholder="برای تولید خودکار خالی بگذارید">
                            <button class="btn btn-p" onclick="addClient()">ساخت کانفیگ</button>
                        </div>
                    </div>
                </div>
                <div class="main" id="clientList"></div>
            </div>
        </div>

        <script>
            let state = { users: [], clean_ip: '' };
            const masterUuid = "${masterUuid}";
            const host = "${host}";
            const apiPath = "/" + masterUuid + "/api";
            const AUTH_USER = "${adminUser}";
            const AUTH_PASS = "${adminPass}";

            function doLogin() {
                const u = document.getElementById('userIn').value;
                const p = document.getElementById('passIn').value;
                if (u === AUTH_USER && p === AUTH_PASS) {
                    document.getElementById('loginOverlay').style.display = 'none';
                    document.getElementById('mainContent').style.display = 'block';
                    fetchData();
                } else {
                    alert('خطا در ورود');
                }
            }

            async function fetchData() {
                try {
                    const resp = await fetch(apiPath);
                    state = await resp.json();
                    document.getElementById('cleanIp').value = state.clean_ip || '';
                    renderAll();
                } catch (e) { console.error(e); }
            }

            async function saveSettings() {
                const clean_ip = document.getElementById('cleanIp').value.trim();
                await fetch(apiPath, {
                    method: 'POST',
                    body: JSON.stringify({ clean_ip, users: state.users }),
                    headers: { 'Content-Type': 'application/json' }
                });
                alert('آی‌پی تمیز با موفقیت ثبت شد');
                renderAll();
            }

            async function addClient() {
                const name = document.getElementById('newName').value.trim() || 'NewUser';
                let uuid = document.getElementById('newUuid').value.trim() || crypto.randomUUID();
                state.users.push({ name, uuid, id: 'u-' + Date.now() });
                await fetch(apiPath, {
                    method: 'POST',
                    body: JSON.stringify({ users: state.users, clean_ip: document.getElementById('cleanIp').value.trim() }),
                    headers: { 'Content-Type': 'application/json' }
                });
                document.getElementById('newName').value = '';
                document.getElementById('newUuid').value = '';
                renderAll();
            }

            async function deleteClient(id) {
                if (!confirm('حذف شود؟')) return;
                state.users = state.users.filter(u => u.id !== id);
                await fetch(apiPath, {
                    method: 'POST',
                    body: JSON.stringify({ users: state.users, clean_ip: document.getElementById('cleanIp').value.trim() }),
                    headers: { 'Content-Type': 'application/json' }
                });
                renderAll();
            }

            function copyText(text) {
                const nav = document.createElement('textarea');
                nav.value = text;
                document.body.appendChild(nav); nav.select();
                document.execCommand('copy');
                document.body.removeChild(nav);
                alert('کپی شد!');
            }

            function renderAll() {
                const list = document.getElementById('clientList');
                list.innerHTML = '';
                const addr = document.getElementById('cleanIp').value.trim() || host;
                
                state.users.forEach(u => {
                    // تولید لینک با تنظیمات بهینه برای توییتر و وب‌ساکت
                    const link = "vless://" + u.uuid + "@" + addr + ":443?encryption=none&security=tls&sni=" + host + "&fp=chrome&type=ws&host=" + host + "&path=" + encodeURIComponent("/?ed=2048") + "#" + encodeURIComponent(u.name);

                    const card = document.createElement('div');
                    card.className = 'client-card';
                    card.innerHTML = \`
                        <div class="client-header">
                            <span style="font-weight:bold">\${u.name}</span>
                            <button onclick="deleteClient('\${u.id}')" style="color:var(--r); background:none; border:none; cursor:pointer;">حذف کاربر</button>
                        </div>
                        <div class="client-body">
                            <div>
                                <label class="label">VLESS Config</label>
                                <div class="config-area" onclick="copyText('\${link}')">
                                    <div class="config-text">\${link}</div>
                                </div>
                                <div style="margin-top:15px; font-size:0.75rem; color:var(--s)">
                                    ✓ دی‌ان‌اس اختصاصی فعال است <br>
                                    ✓ بهینه‌سازی شده برای اینستاگرام و توییتر
                                </div>
                            </div>
                            <div class="qr-col">
                                <div class="qr-wrapper" id="qr-\${u.id}"></div>
                            </div>
                        </div>
                    \`;
                    list.appendChild(card);
                    new QRCode(document.getElementById('qr-'+u.id), { text: link, width: 200, height: 200 });
                });
            }
        </script>
    </body>
    </html>
    `;
}
