#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import websockets
import json
import sqlite3
import aiosqlite
from aiohttp import web
from datetime import datetime, timedelta

HOST = "0.0.0.0"
WS_PORT = 5000
HTTP_PORT = 8080
DB_PATH = "tracker.db"
AUTO_ACK = True

# Цвета категорий (можно менять на сервере без правки JS)
CATEGORY_COLORS = {
    "Полироль": "#00ff00",
    "Стандарт": "#0066ff",
    "Туризм": "#ff8800",
    "Спорт": "#ff00ff",
    "Организатор": "#000000",
    # категория по умолчанию: "#888888"
}

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS nodes (
            node_id INTEGER PRIMARY KEY,
            alias TEXT,
            start_number TEXT,
            pilot1 TEXT,
            pilot2 TEXT,
            category TEXT,
            last_seen INTEGER,
            lat REAL,
            lon REAL,
            speed REAL,
            battery_percent INTEGER,
            satellites INTEGER,
            rssi INTEGER,
            hops INTEGER,
            flags INTEGER,
            sos_seq INTEGER,
            role INTEGER
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id INTEGER,
            timestamp INTEGER,
            lat REAL,
            lon REAL,
            speed REAL,
            battery_percent INTEGER,
            satellites INTEGER,
            rssi INTEGER,
            hops INTEGER,
            flags INTEGER,
            sos_seq INTEGER
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_tracks_node_time ON tracks(node_id, timestamp)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_tracks_timestamp ON tracks(timestamp)')
    conn.commit()
    conn.close()
    print("[DB] Инициализирована")

# ----------------------------------------------------------------------
# Работа с БД (телеметрия)
# ----------------------------------------------------------------------
async def update_node_telemetry(data):
    """Обновляет только динамические поля (координаты, батарею и т.д.),
    не трогая статические данные экипажа (start_number, pilot1, ...)."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Сначала убедимся, что запись существует (создаём с пустыми полями, если нет)
        await db.execute('''
            INSERT INTO nodes (node_id, last_seen, lat, lon, speed, battery_percent,
                               satellites, rssi, hops, flags, sos_seq, role)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(node_id) DO NOTHING
        ''', (
            data["nodeId"], int(datetime.now().timestamp()), data.get("lat"), data.get("lon"),
            data.get("speed"), data.get("batteryPercent"), data.get("satellites"),
            data.get("rssi"), data.get("hops"), data.get("flags", 0),
            data.get("sosSequence", 0), data.get("role", 0)
        ))
        # Теперь обновляем динамические поля
        await db.execute('''
            UPDATE nodes
            SET last_seen = ?,
                lat = ?,
                lon = ?,
                speed = ?,
                battery_percent = ?,
                satellites = ?,
                rssi = ?,
                hops = ?,
                flags = ?,
                sos_seq = ?,
                role = ?
            WHERE node_id = ?
        ''', (
            int(datetime.now().timestamp()), data.get("lat"), data.get("lon"),
            data.get("speed"), data.get("batteryPercent"), data.get("satellites"),
            data.get("rssi"), data.get("hops"), data.get("flags", 0),
            data.get("sosSequence", 0), data.get("role", 0),
            data["nodeId"]
        ))
        await db.commit()

async def insert_track(data):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT INTO tracks 
            (node_id, timestamp, lat, lon, speed, battery_percent, satellites, rssi, hops, flags, sos_seq)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data["nodeId"], int(datetime.now().timestamp()), data.get("lat"), data.get("lon"),
            data.get("speed"), data.get("batteryPercent"), data.get("satellites"),
            data.get("rssi"), data.get("hops"), data.get("flags", 0),
            data.get("sosSequence", 0)
        ))
        await db.commit()

# ----------------------------------------------------------------------
# API для получения данных
# ----------------------------------------------------------------------
async def get_all_nodes():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('''
            SELECT node_id, alias, start_number, pilot1, pilot2, category,
                   lat, lon, battery_percent, flags, last_seen, rssi
            FROM nodes
        ''') as cursor:
            rows = await cursor.fetchall()
    nodes = []
    for row in rows:
        nodes.append({
            "id": row[0],
            "alias": row[1],
            "start_number": row[2],
            "pilot1": row[3],
            "pilot2": row[4],
            "category": row[5],
            "lat": row[6],
            "lon": row[7],
            "battery": row[8],
            "sos": bool(row[9] & 2),          # флаг SOS (бит 1)
            "last_seen": row[10],
            "rssi": row[11]
        })
    return nodes

async def get_tracks(node_id, hours=24):
    since = int((datetime.now() - timedelta(hours=hours)).timestamp())
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('''
            SELECT timestamp, lat, lon, speed, battery_percent 
            FROM tracks 
            WHERE node_id = ? AND timestamp >= ?
            ORDER BY timestamp ASC
        ''', (node_id, since)) as cursor:
            rows = await cursor.fetchall()
    return [{"timestamp": r[0], "lat": r[1], "lon": r[2], "speed": r[3], "battery": r[4]} for r in rows]

async def get_node_info(node_id):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('''
            SELECT node_id, alias, start_number, pilot1, pilot2, category
            FROM nodes WHERE node_id = ?
        ''', (node_id,)) as cursor:
            row = await cursor.fetchone()
    if row:
        return {
            "node_id": row[0],
            "alias": row[1],
            "start_number": row[2],
            "pilot1": row[3],
            "pilot2": row[4],
            "category": row[5]
        }
    return None

# ----------------------------------------------------------------------
# Обновление данных экипажа (не затирается телеметрией)
# ----------------------------------------------------------------------
async def update_node_info(node_id, data):
    """Обновляет только статические поля (start_number, pilot1, pilot2, category, alias)."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Сначала убедимся, что запись существует (на случай, если телеметрия ещё не приходила)
        await db.execute('''
            INSERT INTO nodes (node_id) VALUES (?)
            ON CONFLICT(node_id) DO NOTHING
        ''', (node_id,))
        # Обновляем переданные поля
        set_clauses = []
        params = []
        for field in ['start_number', 'pilot1', 'pilot2', 'category', 'alias']:
            if field in data and data[field] is not None:
                set_clauses.append(f"{field} = ?")
                params.append(data[field])
        if set_clauses:
            query = f"UPDATE nodes SET {', '.join(set_clauses)} WHERE node_id = ?"
            params.append(node_id)
            await db.execute(query, params)
            await db.commit()

# ----------------------------------------------------------------------
# WebSocket и обработка телеметрии
# ----------------------------------------------------------------------
active_websocket = None

async def send_to_esp32(command_dict):
    global active_websocket
    if active_websocket is None:
        return False
    try:
        await active_websocket.send(json.dumps(command_dict))
        return True
    except:
        return False

async def send_ack(target_id, sos_seq):
    cmd = {"command": "send_ack", "targetId": target_id, "sosSequence": sos_seq}
    if await send_to_esp32(cmd):
        print(f"[ACK] Отправлен узлу {target_id} (seq={sos_seq})")

async def handle_telemetry(data):
    node_id = data.get("nodeId")
    if node_id is None:
        return
    await update_node_telemetry(data)
    await insert_track(data)
    print(f"[TELEMETRY] Узел {node_id}: {data.get('lat')}, {data.get('lon')} | бат={data.get('batteryPercent')}%")
    if data.get("flags", 0) & 2:
        print(f"[SOS] АКТИВЕН на узле {node_id} (seq={data.get('sosSequence')})")
        if AUTO_ACK:
            await send_ack(node_id, data.get("sosSequence"))

async def handle_status(data):
    print(f"[STATUS] uptime={data.get('uptime')}c, heap={data.get('freeHeap')}, видимых узлов={len(data.get('nodes', []))}")

async def ws_handler(websocket):
    global active_websocket
    active_websocket = websocket
    print(f"[WS] ESP32 подключился ({websocket.remote_address[0]})")
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                if data.get("type") == "telemetry":
                    await handle_telemetry(data)
                elif data.get("type") == "status":
                    await handle_status(data)
            except json.JSONDecodeError:
                print("[WS] Некорректный JSON")
    except websockets.exceptions.ConnectionClosed:
        print("[WS] ESP32 отключился")
    finally:
        if active_websocket == websocket:
            active_websocket = None

# ----------------------------------------------------------------------
# HTTP API endpoints
# ----------------------------------------------------------------------
async def api_nodes(request):
    return web.json_response(await get_all_nodes())

async def api_tracks(request):
    node_id = int(request.query.get("node_id", 0))
    hours = int(request.query.get("hours", 24))
    if node_id == 0:
        return web.json_response({"error": "node_id required"}, status=400)
    return web.json_response(await get_tracks(node_id, hours))

async def api_categories(request):
    """Возвращает словарь цветов категорий."""
    return web.json_response(CATEGORY_COLORS)

async def api_node_info(request):
    """GET /api/node?node_id=... – получить информацию об экипаже.
       POST /api/node – обновить информацию об экипаже."""
    if request.method == 'GET':
        node_id = int(request.query.get("node_id", 0))
        if node_id == 0:
            return web.json_response({"error": "node_id required"}, status=400)
        info = await get_node_info(node_id)
        if info:
            return web.json_response(info)
        else:
            return web.json_response({"error": "node not found"}, status=404)
    elif request.method == 'POST':
        try:
            body = await request.json()
            node_id = body.get("node_id")
            if node_id is None:
                return web.json_response({"error": "node_id required"}, status=400)
            # Обновляем только переданные поля
            await update_node_info(node_id, body)
            return web.json_response({"status": "ok"})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

# ----------------------------------------------------------------------
# Веб-интерфейс (HTML + JS)
# ----------------------------------------------------------------------
async def index_page(request):
    html = '''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Tracker Map | Внедорожные соревнования</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <style>
        body { margin:0; padding:0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
        #map { height: 100vh; width: 100%; }
        #controls {
            position: absolute;
            top: 10px;
            right: 10px;
            background: white;
            z-index: 1000;
            padding: 12px;
            border-radius: 8px;
            box-shadow: 0 0 10px rgba(0,0,0,0.3);
            font-size: 13px;
            min-width: 160px;
            pointer-events: auto;
        }
        button {
            margin: 4px 0;
            padding: 5px 10px;
            width: 100%;
            cursor: pointer;
            border: none;
            border-radius: 4px;
            background-color: #f0f0f0;
            transition: 0.2s;
        }
        button:hover {
            background-color: #ddd;
        }
        .legend {
            margin-top: 10px;
            border-top: 1px solid #ccc;
            padding-top: 8px;
        }
        .legend span {
            display: inline-block;
            width: 14px;
            height: 14px;
            border-radius: 50%;
            margin-right: 6px;
            vertical-align: middle;
        }
        /* Модальное окно */
        .modal {
            display: none;
            position: fixed;
            z-index: 2000;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0,0,0,0.5);
            justify-content: center;
            align-items: center;
        }
        .modal-content {
            background: white;
            padding: 20px;
            border-radius: 10px;
            width: 300px;
            max-width: 80%;
            box-shadow: 0 5px 20px rgba(0,0,0,0.3);
        }
        .modal-content h3 {
            margin-top: 0;
        }
        .modal-content input, .modal-content select {
            width: 100%;
            margin: 6px 0;
            padding: 6px;
            box-sizing: border-box;
        }
        .modal-buttons {
            display: flex;
            justify-content: space-between;
            margin-top: 12px;
        }
        .modal-buttons button {
            width: 48%;
        }
    </style>
</head>
<body>
<div id="map"></div>
<div id="controls">
    <b>🏁 Трекер соревнований</b><br>
    <span id="nodeCount">0</span> экипажей<br>
    <button id="zoomToAllBtn">🔍 Показать всех</button>
    <div class="legend">
        <span style="background:#33cc33;"></span> >50%<br>
        <span style="background:#ffaa00;"></span> 15-50%<br>
        <span style="background:#ff4444;"></span> <15%<br>
        <span style="background:#ff0000;"></span> SOS
    </div>
</div>

<!-- Модальное окно редактирования экипажа -->
<div id="editModal" class="modal">
    <div class="modal-content">
        <h3>✏️ Редактировать экипаж</h3>
        <input type="hidden" id="editNodeId">
        <label>Стартовый номер:</label>
        <input type="text" id="editStartNumber" placeholder="например 215">
        <label>Пилот:</label>
        <input type="text" id="editPilot1" placeholder="Иванов Иван">
        <label>Штурман:</label>
        <input type="text" id="editPilot2" placeholder="Петров Петр">
        <label>Категория:</label>
        <select id="editCategory">
            <option value="Полироль">Полироль</option>
            <option value="Стандарт">Стандарт</option>
            <option value="Туризм">Туризм</option>
            <option value="Спорт">Спорт</option>
            <option value="Организатор">Организатор</option>
        </select>
        <label>Отображаемое имя (alias):</label>
        <input type="text" id="editAlias" placeholder="необязательно">
        <div class="modal-buttons">
            <button id="saveEditBtn">💾 Сохранить</button>
            <button id="cancelEditBtn">❌ Отмена</button>
        </div>
    </div>
</div>

<script>
    // ---------- Глобальные переменные ----------
    var map = L.map('map').setView([55.751244, 37.618423], 12);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> & CartoDB'
    }).addTo(map);

    var markers = new Map();     // id -> L.marker
    var polylines = new Map();   // id -> L.polyline
    var selectedNodeId = null;

    var categoryColors = {};     // загрузим с сервера

    // ---------- Вспомогательные функции ----------
    async function loadCategoryColors() {
        try {
            var resp = await fetch('/api/categories');
            var colors = await resp.json();
            categoryColors = colors;
        } catch(e) { console.error("Ошибка загрузки цветов категорий", e); }
    }

    function getColor(category, sos) {
        if (sos) return "#ff0000";
        if (categoryColors[category]) return categoryColors[category];
        return "#888888"; // цвет по умолчанию
    }

    function createIcon(startNumber, category, sos) {
        var color = getColor(category, sos);
        var displayNumber = startNumber && startNumber !== "" ? startNumber : "?";
        return L.divIcon({
            className: '',
            html: '<div style="background:'+color+'; width:36px; height:36px; border-radius:50%; border:2px solid white; text-align:center; line-height:32px; font-weight:bold; color:white; font-size:14px; box-shadow:0 1px 3px rgba(0,0,0,0.3);">' + displayNumber + '</div>',
            iconSize: [36, 36],
            popupAnchor: [0, -18]
        });
    }

    function formatTimeAgo(timestamp) {
        if (!timestamp) return "никогда";
        var seconds = Math.floor(Date.now() / 1000) - timestamp;
        if (seconds < 60) return seconds + " сек назад";
        var minutes = Math.floor(seconds / 60);
        if (minutes < 60) return minutes + " мин назад";
        var hours = Math.floor(minutes / 60);
        if (hours < 24) return hours + " ч назад";
        return Math.floor(hours / 24) + " дн назад";
    }

    function escapeHtml(str) {
        if (!str) return "";
        return str.replace(/[&<>]/g, function(m) {
            if (m === '&') return '&amp;';
            if (m === '<') return '&lt;';
            if (m === '>') return '&gt;';
            return m;
        });
    }

    // ---------- Редактирование (модальное окно) ----------
    function openEditModal(nodeId, currentData) {
        document.getElementById('editNodeId').value = nodeId;
        document.getElementById('editStartNumber').value = currentData.start_number || '';
        document.getElementById('editPilot1').value = currentData.pilot1 || '';
        document.getElementById('editPilot2').value = currentData.pilot2 || '';
        document.getElementById('editCategory').value = currentData.category || 'Туризм';
        document.getElementById('editAlias').value = currentData.alias || '';
        document.getElementById('editModal').style.display = 'flex';
    }

    async function saveEdit() {
        var nodeId = parseInt(document.getElementById('editNodeId').value);
        var data = {
            node_id: nodeId,
            start_number: document.getElementById('editStartNumber').value.trim(),
            pilot1: document.getElementById('editPilot1').value.trim(),
            pilot2: document.getElementById('editPilot2').value.trim(),
            category: document.getElementById('editCategory').value,
            alias: document.getElementById('editAlias').value.trim()
        };
        try {
            var resp = await fetch('/api/node', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(data)
            });
            if (resp.ok) {
                closeModal();
                updateNodes(); // обновить карту и попапы
            } else {
                alert("Ошибка сохранения");
            }
        } catch(e) {
            console.error(e);
            alert("Ошибка сети");
        }
    }

    function closeModal() {
        document.getElementById('editModal').style.display = 'none';
    }

    // ---------- Показать/скрыть трек ----------
    async function toggleTrack(nodeId) {
        if (selectedNodeId === nodeId && polylines.has(nodeId)) {
            // скрываем трек
            map.removeLayer(polylines.get(nodeId));
            polylines.delete(nodeId);
            selectedNodeId = null;
            return;
        }
        // убираем предыдущий трек
        if (selectedNodeId !== null && polylines.has(selectedNodeId)) {
            map.removeLayer(polylines.get(selectedNodeId));
            polylines.delete(selectedNodeId);
        }
        selectedNodeId = nodeId;
        try {
            var resp = await fetch('/api/tracks?node_id=' + nodeId + '&hours=24');
            var tracks = await resp.json();
            if (tracks.length > 0) {
                var points = tracks.map(t => [t.lat, t.lon]);
                var polyline = L.polyline(points, { color: '#3388ff', weight: 4, opacity: 0.7 }).addTo(map);
                polylines.set(nodeId, polyline);
                map.fitBounds(polyline.getBounds());
            } else {
                alert('Нет данных трека за последние 24 часа');
                selectedNodeId = null;
            }
        } catch(e) { console.error(e); }
    }

    // ---------- Обновление маркеров с картой ----------
    async function updateNodes() {
        try {
            var resp = await fetch('/api/nodes');
            var nodes = await resp.json();
            document.getElementById('nodeCount').innerText = nodes.length;
            var nowSec = Date.now() / 1000;
            for (var n of nodes) {
                var id = n.id;
                var lat = n.lat;
                var lon = n.lon;
                if (!lat || !lon) continue;
                var battery = (n.battery !== undefined && n.battery !== null) ? n.battery : 100;
                var sos = n.sos === true;
                var category = n.category || "";
                var startNumber = (n.start_number && n.start_number !== "") ? n.start_number : id.toString();
                var alias = n.alias || "";
                var pilot1 = n.pilot1 || "";
                var pilot2 = n.pilot2 || "";
                var lastSeen = n.last_seen;
                
                var popupContent = 
                    '<div style="min-width:200px;">' +
                    '<b style="font-size:1.2em;">🏁 ' + escapeHtml(startNumber) + '</b><br>' +
                    (pilot1 ? '👨‍✈️ Пилот: ' + escapeHtml(pilot1) + '<br>' : '') +
                    (pilot2 ? '🧭 Штурман: ' + escapeHtml(pilot2) + '<br>' : '') +
                    (category ? '🏆 Категория: ' + escapeHtml(category) + '<br>' : '') +
                    '🔋 Батарея: ' + battery + '%<br>' +
                    '🕒 Последний пакет: ' + formatTimeAgo(lastSeen) + '<br>' +
                    '<div style="margin-top:8px;">' +
                    '<button onclick="toggleTrack(' + id + ')" style="width:auto; margin-right:5px;">📍 Трек</button>' +
                    '<button onclick="editNodeData(' + id + ')" style="width:auto;">✏️ Редактировать</button>' +
                    '</div></div>';
                
                var icon = createIcon(startNumber, category, sos);
                if (markers.has(id)) {
                    var marker = markers.get(id);
                    marker.setLatLng([lat, lon]);
                    marker.setIcon(icon);
                    marker.bindPopup(popupContent);
                } else {
                    var marker = L.marker([lat, lon], { icon: icon }).addTo(map);
                    marker.bindPopup(popupContent);
                    markers.set(id, marker);
                }
            }
            // удаляем маркеры узлов, которых больше нет
            for (var [id, marker] of markers.entries()) {
                if (!nodes.find(n => n.id == id)) {
                    map.removeLayer(marker);
                    markers.delete(id);
                    if (polylines.has(id)) {
                        map.removeLayer(polylines.get(id));
                        polylines.delete(id);
                    }
                }
            }
        } catch(e) { console.error(e); }
    }

    // Глобальная функция для вызова из onclick (редактирование)
    window.editNodeData = async function(nodeId) {
        // сначала загрузим актуальные данные об экипаже
        try {
            var resp = await fetch('/api/node?node_id=' + nodeId);
            if (resp.ok) {
                var data = await resp.json();
                openEditModal(nodeId, data);
            } else {
                // если узла ещё нет в БД, создадим пустую карточку
                openEditModal(nodeId, {});
            }
        } catch(e) { console.error(e); }
    };
    window.toggleTrack = toggleTrack;

    function zoomToAll() {
        var bounds = [];
        for (var [id, marker] of markers.entries()) {
            var latlng = marker.getLatLng();
            if (latlng) bounds.push(latlng);
        }
        if (bounds.length > 0) {
            var group = L.featureGroup(bounds.map(p => L.marker(p)));
            map.fitBounds(group.getBounds().pad(0.1));
        } else {
            alert("Нет активных маркеров");
        }
    }

    // ---------- Инициализация и таймер ----------
    document.getElementById('zoomToAllBtn').addEventListener('click', zoomToAll);
    document.getElementById('saveEditBtn').addEventListener('click', saveEdit);
    document.getElementById('cancelEditBtn').addEventListener('click', closeModal);
    // закрыть модалку по клику вне окна
    window.onclick = function(event) {
        var modal = document.getElementById('editModal');
        if (event.target == modal) closeModal();
    };

    loadCategoryColors().then(() => {
        updateNodes();
        setInterval(updateNodes, 5000);
    });
</script>
</body>
</html>'''
    return web.Response(text=html, content_type='text/html')

# ----------------------------------------------------------------------
# Запуск HTTP и WebSocket серверов
# ----------------------------------------------------------------------
async def start_http_server():
    app = web.Application()
    app.router.add_get('/', index_page)
    app.router.add_get('/api/nodes', api_nodes)
    app.router.add_get('/api/tracks', api_tracks)
    app.router.add_get('/api/categories', api_categories)
    app.router.add_get('/api/node', api_node_info)
    app.router.add_post('/api/node', api_node_info)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, HTTP_PORT)
    await site.start()
    print(f"[HTTP] Веб-интерфейс: http://{HOST}:{HTTP_PORT}")

async def main():
    init_db()
    asyncio.create_task(start_http_server())
    async with websockets.serve(ws_handler, HOST, WS_PORT):
        print(f"[WS] WebSocket сервер: ws://{HOST}:{WS_PORT}")
        await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nОстановка сервера")