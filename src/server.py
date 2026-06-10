#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import websockets
import json
import sys
import sqlite3
import aiosqlite
from aiohttp import web
from datetime import datetime, timedelta

HOST = "0.0.0.0"
WS_PORT = 5000
HTTP_PORT = 8080
DB_PATH = "tracker.db"
AUTO_ACK = True

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS nodes (
            node_id INTEGER PRIMARY KEY,
            alias TEXT,
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

async def update_node(data):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('SELECT alias FROM nodes WHERE node_id = ?', (data["nodeId"],)) as cur:
            row = await cur.fetchone()
            alias = row[0] if row else None
        await db.execute('''
            INSERT OR REPLACE INTO nodes 
            (node_id, alias, last_seen, lat, lon, speed, battery_percent, satellites, rssi, hops, flags, sos_seq, role)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data["nodeId"], alias, int(datetime.now().timestamp()), data.get("lat"), data.get("lon"),
            data.get("speed"), data.get("batteryPercent"), data.get("satellites"),
            data.get("rssi"), data.get("hops"), data.get("flags", 0),
            data.get("sosSequence", 0), data.get("role", 0)
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

async def get_all_nodes():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('SELECT node_id, alias, lat, lon, battery_percent, flags, last_seen, rssi FROM nodes') as cursor:
            rows = await cursor.fetchall()
    nodes = []
    for row in rows:
        nodes.append({
            "id": row[0],
            "alias": row[1] or str(row[0]),
            "lat": row[2],
            "lon": row[3],
            "battery": row[4],
            "sos": bool(row[5] & 2),
            "last_seen": row[6],
            "rssi": row[7]
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

async def set_alias(node_id, alias):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE nodes SET alias = ? WHERE node_id = ?', (alias, node_id))
        await db.commit()

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
    await update_node(data)
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

async def api_nodes(request):
    return web.json_response(await get_all_nodes())

async def api_tracks(request):
    node_id = int(request.query.get("node_id", 0))
    hours = int(request.query.get("hours", 24))
    if node_id == 0:
        return web.json_response({"error": "node_id required"}, status=400)
    return web.json_response(await get_tracks(node_id, hours))

async def api_set_alias(request):
    try:
        data = await request.json()
        node_id = data.get("node_id")
        alias = data.get("alias")
        if node_id is None or alias is None:
            return web.json_response({"error": "node_id and alias required"}, status=400)
        await set_alias(node_id, alias)
        return web.json_response({"status": "ok"})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

async def index_page(request):
    html = '''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Tracker Map</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <style>
        body { margin:0; padding:0; font-family: sans-serif; }
        #map { height: 100vh; width: 100%; }
        #controls {
            position: absolute;
            top: 10px;
            right: 10px;
            background: white;
            z-index: 1000;
            padding: 10px;
            border-radius: 5px;
            box-shadow: 0 0 5px rgba(0,0,0,0.3);
            font-size: 12px;
            min-width: 150px;
        }
        button {
            margin: 3px 0;
            padding: 4px 8px;
            width: 100%;
            cursor: pointer;
        }
        .legend {
            margin-top: 8px;
            border-top: 1px solid #ccc;
            padding-top: 5px;
        }
        .legend span {
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 4px;
        }
        .edit-alias {
            font-size: 11px;
            margin-left: 5px;
            cursor: pointer;
            color: #069;
        }
    </style>
</head>
<body>
<div id="map"></div>
<div id="controls">
    <b>Tracker Map</b><br>
    <span id="nodeCount">0</span> узлов<br>
    <button id="zoomToAllBtn">🔍 Показать всех</button>
    <div class="legend">
        <span style="background:#33cc33;"></span> >50%<br>
        <span style="background:#ffaa00;"></span> 15-50%<br>
        <span style="background:#ff4444;"></span> <15%<br>
        <span style="background:#ff0000;"></span> SOS
    </div>
</div>

<script>
    var map = L.map('map').setView([55.751244, 37.618423], 12);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> & CartoDB'
    }).addTo(map);

    var markers = new Map();
    var polylines = new Map();
    var selectedNodeId = null;

    function getColor(battery, sos) {
        if (sos) return '#ff0000';
        if (battery < 15) return '#ff4444';
        if (battery < 50) return '#ffaa00';
        return '#33cc33';
    }

    function createIcon(battery, sos) {
        var color = getColor(battery, sos);
        var html = '<div style="background-color: ' + color + '; width: 22px; height: 22px; border-radius: 50%; border: 2px solid white; box-shadow: 0 2px 4px rgba(0,0,0,0.3);"></div>';
        if (sos) {
            html = '<div style="position: relative;">' + html + '<div style="position: absolute; top: -6px; right: -6px; background: red; color: white; font-size: 11px; border-radius: 10px; width: 16px; height: 16px; text-align: center; line-height: 16px; font-weight: bold;">SOS</div></div>';
        }
        return L.divIcon({ html: html, iconSize: [22, 22], className: 'custom-div-icon' });
    }

    function escapeHtml(str) {
        return str.replace(/[&<>]/g, function(m) {
            if (m === '&') return '&amp;';
            if (m === '<') return '&lt;';
            if (m === '>') return '&gt;';
            return m;
        });
    }

    async function setAlias(nodeId, currentAlias) {
        var newAlias = prompt("Введите отображаемое имя для узла " + nodeId, currentAlias);
        if (newAlias !== null && newAlias !== "") {
            try {
                var resp = await fetch('/api/alias', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ node_id: nodeId, alias: newAlias })
                });
                if (resp.ok) {
                    var marker = markers.get(nodeId);
                    if (marker) {
                        var popup = marker.getPopup();
                        if (popup) {
                            var content = popup.getContent();
                            var start = content.indexOf('<b>');
                            var end = content.indexOf('</b>');
                            if (start !== -1 && end !== -1) {
                                var newContent = content.substring(0, start + 3) + escapeHtml(newAlias) + ' (ID ' + nodeId + ')' + content.substring(end);
                                marker.bindPopup(newContent);
                                if (marker.isPopupOpen()) {
                                    marker.openPopup();
                                }
                            }
                        }
                    }
                } else {
                    alert("Ошибка сохранения имени");
                }
            } catch(e) { console.error(e); }
        }
    }

    async function updateNodes() {
        try {
            var resp = await fetch('/api/nodes');
            var nodes = await resp.json();
            document.getElementById('nodeCount').innerText = nodes.length;
            var now = Date.now() / 1000;
            for (var n of nodes) {
                var id = n.id;
                var lat = n.lat;
                var lon = n.lon;
                var battery = n.battery !== undefined ? n.battery : 100;
                var sos = n.sos === true;
                var alias = n.alias || 'Узел ' + id;
                if (!lat || !lon) continue;
                // Безопасное экранирование для передачи в onclick
                var safeAlias = JSON.stringify(alias);
                var popupContent = '<b>' + escapeHtml(alias) + ' (ID ' + id + ')</b><br>'
                    + 'Батарея: ' + battery + '%<br>'
                    + 'SOS: ' + (sos ? 'АКТИВЕН' : 'нет') + '<br>'
                    + 'Последний пакет: ' + Math.floor((now - n.last_seen) / 60) + ' мин назад<br>'
                    + '<button onclick="showTrack(' + id + ')">Показать трек</button> '
                    + '<button class="edit-alias" onclick="setAlias(' + id + ', ' + safeAlias + ')">✏️</button>';
                if (markers.has(id)) {
                    var marker = markers.get(id);
                    marker.setLatLng([lat, lon]);
                    marker.setIcon(createIcon(battery, sos));
                    marker.bindPopup(popupContent);
                } else {
                    var marker = L.marker([lat, lon], { icon: createIcon(battery, sos) }).addTo(map);
                    marker.bindPopup(popupContent);
                    markers.set(id, marker);
                }
            }
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

    async function showTrack(nodeId) {
        if (selectedNodeId === nodeId && polylines.has(nodeId)) {
            if (polylines.has(nodeId)) {
                map.removeLayer(polylines.get(nodeId));
                polylines.delete(nodeId);
            }
            selectedNodeId = null;
            return;
        }
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
                var polyline = L.polyline(points, { color: 'blue', weight: 3 }).addTo(map);
                polylines.set(nodeId, polyline);
                map.fitBounds(polyline.getBounds());
            } else {
                alert('Нет данных трека за 24 часа');
            }
        } catch(e) { console.error(e); }
    }

    document.getElementById('zoomToAllBtn').addEventListener('click', zoomToAll);
    updateNodes();
    setInterval(updateNodes, 5000);
</script>
</body>
</html>'''
    return web.Response(text=html, content_type='text/html')

async def start_http_server():
    app = web.Application()
    app.router.add_get('/', index_page)
    app.router.add_get('/api/nodes', api_nodes)
    app.router.add_get('/api/tracks', api_tracks)
    app.router.add_post('/api/alias', api_set_alias)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, HTTP_PORT)
    await site.start()
    print(f"[HTTP] Веб-интерфейс доступен на http://{HOST}:{HTTP_PORT}")

async def main():
    init_db()
    asyncio.create_task(start_http_server())
    async with websockets.serve(ws_handler, HOST, WS_PORT):
        print(f"[WS] WebSocket сервер запущен на ws://{HOST}:{WS_PORT}")
        await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nОстановка сервера")