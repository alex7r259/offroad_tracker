#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import websockets
import json
import sqlite3
import aiosqlite
from aiohttp import web
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import os
import stat

HOST = "0.0.0.0"
WS_PORT = 5000
HTTP_PORT = 8080
DB_PATH = "tracker.db"
AUTO_ACK = False
CATEGORY_COLORS_FILE = "category_colors.json"

# Цвета категорий по умолчанию
DEFAULT_CATEGORY_COLORS = {
    "Полироль": "#00ff00",
    "Стандарт": "#0066ff",
    "Туризм": "#ff8800",
    "Спорт": "#ff00ff",
    "Организатор": "#000000",
    "Базовый лагерь": "#8B4513",
}

def load_category_colors():
    global CATEGORY_COLORS
    try:
        with open(CATEGORY_COLORS_FILE, "r", encoding="utf-8") as f:
            saved = json.load(f)
            DEFAULT_CATEGORY_COLORS.update(saved)
    except FileNotFoundError:
        pass
    CATEGORY_COLORS = DEFAULT_CATEGORY_COLORS.copy()

def save_category_colors(colors_dict):
    with open(CATEGORY_COLORS_FILE, "w", encoding="utf-8") as f:
        json.dump(colors_dict, f, ensure_ascii=False, indent=2)
    global CATEGORY_COLORS
    CATEGORY_COLORS.update(colors_dict)

load_category_colors()

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
    c.execute('''
        CREATE TABLE IF NOT EXISTS waypoints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            categories TEXT,
            type TEXT DEFAULT 'checkpoint',
            description TEXT
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_tracks_node_time ON tracks(node_id, timestamp)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_tracks_timestamp ON tracks(timestamp)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_waypoints_latlon ON waypoints(lat, lon)')
    conn.commit()
    conn.close()
    # Снимаем атрибут "только чтение" для Windows
    if os.name == 'nt':
        os.chmod(DB_PATH, stat.S_IWRITE)
    else:
        os.chmod(DB_PATH, 0o666)
    print("[DB] Инициализирована (nodes, tracks, waypoints)")

# ----------------------------------------------------------------------
# Работа с БД (телеметрия) – без изменений
# ----------------------------------------------------------------------
async def update_node_telemetry(data):
    async with aiosqlite.connect(DB_PATH) as db:
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
# API для получения данных об экипажах – без изменений
# ----------------------------------------------------------------------
async def get_all_nodes():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('''
            SELECT node_id, alias, start_number, pilot1, pilot2, category,
                   lat, lon, battery_percent, flags, last_seen, rssi, sos_seq
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
            "sos": bool(row[9] & 2),
            "last_seen": row[10],
            "rssi": row[11],
            "sos_seq": row[12]
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

async def update_node_info(node_id, data):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT INTO nodes (node_id) VALUES (?)
            ON CONFLICT(node_id) DO NOTHING
        ''', (node_id,))
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
# Работа с точками (waypoints) – без изменений
# ----------------------------------------------------------------------
async def get_all_waypoints():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('SELECT id, name, lat, lon, categories, type, description FROM waypoints ORDER BY id') as cursor:
            rows = await cursor.fetchall()
    return [{
        "id": r[0],
        "name": r[1],
        "lat": r[2],
        "lon": r[3],
        "categories": r[4].split(',') if r[4] else [],
        "type": r[5],
        "description": r[6]
    } for r in rows]

async def add_waypoint(name, lat, lon, categories_list, type_, description):
    categories_str = ','.join(categories_list) if categories_list else ''
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT INTO waypoints (name, lat, lon, categories, type, description)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (name, lat, lon, categories_str, type_, description))
        await db.commit()
        cursor = await db.execute('SELECT last_insert_rowid()')
        row = await cursor.fetchone()
        return row[0] if row else None

async def update_waypoint(wp_id, name, lat, lon, categories_list, type_, description):
    categories_str = ','.join(categories_list) if categories_list else ''
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            UPDATE waypoints
            SET name=?, lat=?, lon=?, categories=?, type=?, description=?
            WHERE id=?
        ''', (name, lat, lon, categories_str, type_, description, wp_id))
        await db.commit()

async def delete_waypoint(wp_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('DELETE FROM waypoints WHERE id=?', (wp_id,))
        await db.commit()

async def async_export_gpx():
    waypoints = await get_all_waypoints()
    gpx_template = '''<?xml version="1.0" encoding="UTF-8"?>
<gpx version="1.1" creator="Tracker Server" xmlns="http://www.topografix.com/GPX/1/1">
    <metadata>
        <name>Соревновательные точки</name>
        <desc>Контрольные пункты и базовый лагерь</desc>
        <time>{time}</time>
    </metadata>
{waypoints}
</gpx>'''
    wpt_template = '''    <wpt lat="{lat}" lon="{lon}">
        <name>{name}</name>
        <desc>{desc}</desc>
        <type>{type}</type>
        <extensions>
            <categories>{categories}</categories>
        </extensions>
    </wpt>'''
    wpt_strings = []
    for wp in waypoints:
        desc = wp.get('description') or ''
        if wp['type'] == 'basecamp':
            type_str = 'Базовый лагерь'
        else:
            type_str = f"КП {wp['name']}"
        categories_str = ','.join(wp.get('categories', []))
        wpt_strings.append(wpt_template.format(
            lat=wp['lat'], lon=wp['lon'],
            name=wp['name'],
            desc=desc,
            type=type_str,
            categories=categories_str
        ))
    gpx = gpx_template.format(time=datetime.now().isoformat(), waypoints='\n'.join(wpt_strings))
    return gpx

async def async_import_gpx(content):
    try:
        root = ET.fromstring(content)
        ns = {'gpx': 'http://www.topografix.com/GPX/1/1'}
        added = 0
        for wpt in root.findall('.//gpx:wpt', ns):
            lat = float(wpt.get('lat'))
            lon = float(wpt.get('lon'))
            name_elem = wpt.find('gpx:name', ns)
            name = name_elem.text if name_elem is not None else ''
            desc_elem = wpt.find('gpx:desc', ns)
            description = desc_elem.text if desc_elem is not None else ''
            type_elem = wpt.find('gpx:type', ns)
            type_ = 'checkpoint'
            if type_elem is not None:
                if 'базовый' in type_elem.text.lower() or 'лагерь' in type_elem.text.lower():
                    type_ = 'basecamp'
            categories_list = []
            ext = wpt.find('gpx:extensions', ns)
            if ext is not None:
                cat_elem = ext.find('categories')
                if cat_elem is not None and cat_elem.text:
                    categories_list = [c.strip() for c in cat_elem.text.split(',') if c.strip()]
                for cat in ext.findall('category'):
                    if cat.text:
                        categories_list.append(cat.text.strip())
            if not categories_list:
                if type_ == 'basecamp':
                    categories_list = ['Базовый лагерь']
                else:
                    categories_list = ['Туризм']
            await add_waypoint(name, lat, lon, categories_list, type_, description)
            added += 1
        return added
    except Exception as e:
        raise ValueError(f"Ошибка парсинга GPX: {e}")

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
        return True
    return False

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
    if request.method == 'GET':
        return web.json_response(CATEGORY_COLORS)
    elif request.method == 'POST':
        try:
            new_colors = await request.json()
            save_category_colors(new_colors)
            return web.json_response({"status": "ok"})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

async def api_node_info(request):
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
            await update_node_info(node_id, body)
            return web.json_response({"status": "ok"})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

async def api_ack(request):
    try:
        data = await request.json()
        target_id = data.get("node_id")
        sos_seq = data.get("sos_seq", 0)
        if target_id is None:
            return web.json_response({"error": "node_id required"}, status=400)
        success = await send_ack(target_id, sos_seq)
        return web.json_response({"status": "ok" if success else "failed"})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

# API для точек – без изменений
async def api_waypoints(request):
    if request.method == 'GET':
        wps = await get_all_waypoints()
        return web.json_response(wps)
    elif request.method == 'POST':
        try:
            data = await request.json()
            wp_id = data.get('id')
            name = data.get('name')
            lat = data.get('lat')
            lon = data.get('lon')
            categories = data.get('categories', [])
            if isinstance(categories, str):
                categories = [c.strip() for c in categories.split(',') if c.strip()]
            elif not isinstance(categories, list):
                categories = []
            type_ = data.get('type', 'checkpoint')
            description = data.get('description', '')
            if not name or lat is None or lon is None:
                return web.json_response({"error": "name, lat, lon required"}, status=400)
            if wp_id:
                await update_waypoint(wp_id, name, lat, lon, categories, type_, description)
            else:
                await add_waypoint(name, lat, lon, categories, type_, description)
            return web.json_response({"status": "ok"})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

async def api_waypoint_delete(request):
    wp_id = int(request.match_info.get('id', 0))
    if not wp_id:
        return web.json_response({"error": "id required"}, status=400)
    await delete_waypoint(wp_id)
    return web.json_response({"status": "ok"})

async def api_export_gpx(request):
    gpx_data = await async_export_gpx()
    return web.Response(text=gpx_data, content_type='application/gpx+xml',
                        headers={'Content-Disposition': 'attachment; filename="waypoints.gpx"'})

async def api_import_gpx(request):
    reader = await request.multipart()
    field = await reader.next()
    if field.name != 'file':
        return web.json_response({"error": "file field expected"}, status=400)
    content = await field.read()
    try:
        count = await async_import_gpx(content.decode('utf-8'))
        return web.json_response({"status": "ok", "imported": count})
    except ValueError as e:
        return web.json_response({"error": str(e)}, status=400)

# ----------------------------------------------------------------------
# HTML страницы
# ----------------------------------------------------------------------
# Главная страница (карта) – добавляем звук и мигание SOS
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
            transition: box-shadow 0.2s;
        }
        .sos-alert {
            box-shadow: 0 0 0 4px red, 0 0 0 8px orange;
            animation: pulse 1s infinite;
        }
        @keyframes pulse {
            0% { box-shadow: 0 0 0 0 red; }
            100% { box-shadow: 0 0 0 10px rgba(255,0,0,0); }
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
        .layer-switch {
            margin-top: 8px;
            font-size: 12px;
        }
        .layer-switch label {
            display: block;
            margin: 3px 0;
        }
        .sos-marker {
            box-shadow: 0 0 0 4px red, 0 0 0 8px orange;
            animation: pulse 1s infinite;
        }
        @keyframes sosPulse {
            0% { box-shadow: 0 0 0 0 red; }
            100% { box-shadow: 0 0 0 10px rgba(255,0,0,0); }
        }
    </style>
</head>
<body>
<div id="map"></div>
<div id="controls">
    <b>🏁 Трекер соревнований</b><br>
    <span id="nodeCount">0</span> экипажей<br>
    <button id="zoomToAllBtn">🔍 Показать всех</button>
    <div class="layer-switch">
        <label><input type="checkbox" id="toggleNodes" checked> Экипажи</label>
        <label><input type="checkbox" id="toggleWaypoints" checked> КП и лагерь</label>
    </div>
    <div class="legend">
        <span style="background:#33cc33;"></span> >50%<br>
        <span style="background:#ffaa00;"></span> 15-50%<br>
        <span style="background:#ff4444;"></span> <15%<br>
        <span style="background:#ff0000;"></span> SOS
    </div>
    <button id="manageWaypointsBtn" style="background:#4CAF50; color:white;">📌 Управление точками</button>
    <button id="manageNodesBtn" style="background:#2196F3; color:white;">📡 Управление узлами</button>
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
    var map = L.map('map').setView([59.56, 56.59], 12);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> & CartoDB'
    }).addTo(map);

    var markers = new Map();     
    var polylines = new Map();   
    var waypointMarkers = new Map();
    var categoryColors = {};
    var waypointsData = [];
    
    // --- Управление треком (динамическое обновление) ---
    var currentTrackNodeId = null;
    var trackUpdateInterval = null;

    // --- Для звука и мигания SOS ---
    let lastSosNodes = new Set(); // id узлов, у которых SOS был активен при прошлом обновлении
    let audioCtx = null;
    function playBeep() {
        if (!audioCtx) {
            audioCtx = new (window.AudioContext || window.webkitAudioContext)();
        }
        const oscillator = audioCtx.createOscillator();
        const gain = audioCtx.createGain();
        oscillator.connect(gain);
        gain.connect(audioCtx.destination);
        oscillator.frequency.value = 880;
        gain.gain.value = 0.3;
        oscillator.start();
        gain.gain.exponentialRampToValueAtTime(0.00001, audioCtx.currentTime + 1);
        oscillator.stop(audioCtx.currentTime + 1);
        // разрешить звук после первого взаимодействия пользователя (но это на карте, можно использовать)
    }

    function checkSosAlert(nodes) {
        const currentSosIds = new Set();
        for (let n of nodes) {
            if (n.sos) currentSosIds.add(n.id);
        }
        // Если появились новые SOS (которых не было в прошлый раз) – звук
        let newSos = false;
        for (let id of currentSosIds) {
            if (!lastSosNodes.has(id)) {
                newSos = true;
                break;
            }
        }
        if (newSos) {
            playBeep();
        }
        // Обновляем состояние
        lastSosNodes = currentSosIds;
        // Мигание панели управления при любом активном SOS
        const controlsDiv = document.getElementById('controls');
        if (currentSosIds.size > 0) {
            controlsDiv.classList.add('sos-alert');
        } else {
            controlsDiv.classList.remove('sos-alert');
        }
    }

    function stopTrackUpdates() {
        if (trackUpdateInterval) {
            clearInterval(trackUpdateInterval);
            trackUpdateInterval = null;
        }
    }

    function refreshTrack(nodeId) {
        if (currentTrackNodeId !== nodeId) return;
        fetch('/api/tracks?node_id=' + nodeId + '&hours=24')
            .then(resp => resp.json())
            .then(tracks => {
                if (tracks.length > 0 && polylines.has(nodeId)) {
                    var points = tracks.map(t => [t.lat, t.lon]);
                    var polyline = polylines.get(nodeId);
                    polyline.setLatLngs(points);
                } else if (tracks.length === 0 && polylines.has(nodeId)) {
                    map.removeLayer(polylines.get(nodeId));
                    polylines.delete(nodeId);
                    stopTrackUpdates();
                    currentTrackNodeId = null;
                }
            })
            .catch(e => console.error("Ошибка обновления трека", e));
    }

    function hideTrack() {
        if (currentTrackNodeId !== null && polylines.has(currentTrackNodeId)) {
            map.removeLayer(polylines.get(currentTrackNodeId));
            polylines.delete(currentTrackNodeId);
        }
        stopTrackUpdates();
        currentTrackNodeId = null;
    }

    function showTrack(nodeId) {
        if (currentTrackNodeId === nodeId) return;
        hideTrack();
        currentTrackNodeId = nodeId;
        fetch('/api/tracks?node_id=' + nodeId + '&hours=24')
            .then(resp => resp.json())
            .then(tracks => {
                if (tracks.length > 0) {
                    var points = tracks.map(t => [t.lat, t.lon]);
                    var polyline = L.polyline(points, { color: '#3388ff', weight: 4, opacity: 0.7 }).addTo(map);
                    polylines.set(nodeId, polyline);
                    map.fitBounds(polyline.getBounds());
                    trackUpdateInterval = setInterval(() => refreshTrack(nodeId), 5000);
                } else {
                    alert('Нет данных трека за 24 часа');
                    currentTrackNodeId = null;
                }
            })
            .catch(e => {
                console.error(e);
                currentTrackNodeId = null;
            });
    }

    window.toggleTrack = function(nodeId) {
        if (currentTrackNodeId === nodeId) {
            hideTrack();
        } else {
            showTrack(nodeId);
        }
    };

    async function loadCategoryColors() {
        try {
            var resp = await fetch('/api/categories');
            var colors = await resp.json();
            categoryColors = colors;
        } catch(e) { console.error("Ошибка загрузки цветов категорий", e); }
    }

    function getColor(category) {
        if (categoryColors[category]) return categoryColors[category];
        return "#888888";
    }

    function createIcon(startNumber, category, sos) {
        var color = getColor(category);
        var displayNumber = startNumber && startNumber !== "" ? startNumber : "?";
        var extraClass = sos ? 'sos-marker' : '';
        return L.divIcon({
            className: '',
            html: '<div class="' + extraClass + '" style="background:'+color+'; width:20px; height:20px; border-radius:50%; border:2px solid white; text-align:center; line-height:20px; font-weight:bold; color:white; font-size:14px; text-shadow: 0 0 3px rgba(0,0,0,0.8); box-shadow:0 1px 3px rgba(0,0,0,0.6);">' + displayNumber + '</div>',
            iconSize: [20, 20],
            popupAnchor: [0, -10]
        });
    }

    function createWaypointIcon(category, type, name) {
        if (type === 'basecamp') category = 'Базовый лагерь';
        var color = categoryColors[category] || '#888888';
        var iconHtml = '<div style="background:'+color+'; width:20px; height:20px; border-radius:50%; border:2px solid black; text-align:center; line-height:20px; font-weight:bold; color:black; font-size:14px; text-shadow: 0 0 3px rgba(255,255,255,0.8); box-shadow:0 1px 3px rgba(0,0,0,0.6);">' + name + '</div>';
        if (type === 'basecamp') {
        var iconHtml = '<div style="background: rgba(0,0,0,0); width:20px; height:20px; text-align:center; line-height:20px; color:black; font-size:16px;">🏁</div>';
        }
        return L.divIcon({
            className: '',
            html: iconHtml,
            iconSize: [20, 20],
            popupAnchor: [0, -10]
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

    // ---------- Редактирование экипажа ----------
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
                updateNodes();
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

    // ---------- Обновление экипажей ----------
    async function updateNodes() {
        try {
            var resp = await fetch('/api/nodes');
            var nodes = await resp.json();
            document.getElementById('nodeCount').innerText = nodes.length;
            var nodesLayerVisible = document.getElementById('toggleNodes').checked;
            
            // Проверка SOS для звука и мигания
            checkSosAlert(nodes);
            
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
                    if (!nodesLayerVisible && map.hasLayer(marker)) map.removeLayer(marker);
                    else if (nodesLayerVisible && !map.hasLayer(marker)) marker.addTo(map);
                } else {
                    var marker = L.marker([lat, lon], { icon: icon }).addTo(map);
                    marker.bindPopup(popupContent);
                    markers.set(id, marker);
                    if (!nodesLayerVisible) map.removeLayer(marker);
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
                    if (currentTrackNodeId === id) {
                        hideTrack();
                    }
                }
            }
        } catch(e) { console.error(e); }
    }

    // ---------- Управление точками ----------
    async function loadWaypoints() {
        try {
            var resp = await fetch('/api/waypoints');
            waypointsData = await resp.json();
            updateWaypointsLayer();
        } catch(e) { console.error(e); }
    }

    function updateWaypointsLayer() {
        var visible = document.getElementById('toggleWaypoints').checked;
        for (var [id, marker] of waypointMarkers.entries()) {
            map.removeLayer(marker);
        }
        waypointMarkers.clear();
        if (!visible) return;
        for (var wp of waypointsData) {
            var firstCategory = (wp.categories && wp.categories.length) ? wp.categories[0] : '';
            var icon = createWaypointIcon(firstCategory, wp.type, wp.name);
            var marker = L.marker([wp.lat, wp.lon], { icon: icon }).addTo(map);
            var categoriesStr = (wp.categories || []).join(', ');
            var popupContent = '<b>' + escapeHtml(wp.name) + '</b><br>' +
                               (wp.type === 'basecamp' ? '🏕️ Базовый лагерь' : '📍 Контрольный пункт') + '<br>' +
                               (categoriesStr ? 'Категории: ' + escapeHtml(categoriesStr) : '') +
                               (wp.description ? '<br>' + escapeHtml(wp.description) : '');
            marker.bindPopup(popupContent);
            waypointMarkers.set(wp.id, marker);
        }
    }

    window.editNodeData = async function(nodeId) {
        try {
            var resp = await fetch('/api/node?node_id=' + nodeId);
            if (resp.ok) {
                var data = await resp.json();
                openEditModal(nodeId, data);
            } else {
                openEditModal(nodeId, {});
            }
        } catch(e) { console.error(e); }
    };

    function zoomToAll() {
        var bounds = [];
        for (var [id, marker] of markers.entries()) {
            var latlng = marker.getLatLng();
            if (latlng) bounds.push(latlng);
        }
        for (var [id, marker] of waypointMarkers.entries()) {
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

    function manageWaypoints() {
        window.location.href = '/waypoints/manage';
    }
    function manageNodes() {
        window.location.href = '/nodes';
    }

    document.getElementById('toggleNodes').addEventListener('change', function() {
        var visible = this.checked;
        for (var [id, marker] of markers.entries()) {
            if (visible && !map.hasLayer(marker)) marker.addTo(map);
            else if (!visible && map.hasLayer(marker)) map.removeLayer(marker);
        }
    });
    document.getElementById('toggleWaypoints').addEventListener('change', function() {
        updateWaypointsLayer();
    });
    document.getElementById('zoomToAllBtn').addEventListener('click', zoomToAll);
    document.getElementById('saveEditBtn').addEventListener('click', saveEdit);
    document.getElementById('cancelEditBtn').addEventListener('click', closeModal);
    document.getElementById('manageWaypointsBtn').addEventListener('click', manageWaypoints);
    document.getElementById('manageNodesBtn').addEventListener('click', manageNodes);
    window.onclick = function(event) {
        var modal = document.getElementById('editModal');
        if (event.target == modal) closeModal();
    };

    loadCategoryColors().then(() => {
        updateNodes();
        loadWaypoints();
        setInterval(updateNodes, 5000);
        setInterval(loadWaypoints, 15000);
    });
</script>
</body>
</html>'''
    return web.Response(text=html, content_type='text/html')

# Страница управления узлами
async def nodes_page(request):
    html = '''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Управление узлами | Offroad Tracker</title>
    <style>
        body { font-family: system-ui; background: #1e2a36; color: #eee; margin: 20px; }
        h1, h2 { color: #ffaa44; }
        table { border-collapse: collapse; width: 100%; background: #2c3e44; }
        th, td { border: 1px solid #4a6a7a; padding: 8px; text-align: left; }
        th { background: #1e3a4a; }
        tr.sos { background: #a22; animation: blink 1s step-end infinite; }
        @keyframes blink { 50% { background: #f00; } }
        button { padding: 6px 12px; margin: 4px; background: #4caf50; color: white; border: none; border-radius: 4px; cursor: pointer; }
        button.ack { background: #f44336; }
        button.edit { background: #2196f3; }
        .color-picker { display: flex; gap: 20px; flex-wrap: wrap; margin: 20px 0; background: #2c3e44; padding: 15px; border-radius: 8px; }
        .color-item { display: flex; align-items: center; gap: 10px; }
        .modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.7); justify-content: center; align-items: center; z-index: 1000; }
        .modal-content { background: #2c3e44; padding: 20px; border-radius: 8px; width: 300px; }
        .modal-content input, .modal-content select { width: 100%; margin: 5px 0; padding: 6px; }
        .modal-buttons { display: flex; justify-content: space-between; margin-top: 15px; }
        .refresh { background: #ffaa44; color: black; margin-bottom: 20px; }
        .nav { margin-bottom: 20px; }
        .nav a { color: #ffaa44; text-decoration: none; margin-right: 15px; }
    </style>
</head>
<body>
<div class="nav">
    <a href="/">← Карта</a>
    <a href="/waypoints/manage">Управление точками</a>
</div>
<h1>📡 Управление экипажами и SOS</h1>
<div id="nodesTable">
    <button class="refresh" onclick="loadNodes()">🔄 Обновить</button>
    <table id="nodeTable">
        <thead>
            <tr><th>ID</th><th>Старт. №</th><th>Пилот</th><th>Штурман</th><th>Категория</th><th>Батарея</th><th>Координаты</th><th>SOS</th><th>Действия</th></tr>
        </thead>
        <tbody id="nodeTableBody"><tr><td colspan="9">Загрузка...</td></tr></tbody>
    </table>
</div>

<h2>🎨 Настройка цветов категорий</h2>
<div id="colorSettings" class="color-picker"></div>
<button onclick="saveColors()">💾 Сохранить цвета</button>

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
        <select id="editCategory"></select>
        <label>Отображаемое имя (alias):</label>
        <input type="text" id="editAlias" placeholder="необязательно">
        <div class="modal-buttons">
            <button id="saveEditBtn">💾 Сохранить</button>
            <button id="cancelEditBtn">❌ Отмена</button>
        </div>
    </div>
</div>

<script>
    let categoriesColors = {};
    
    async function loadColors() {
        const resp = await fetch('/api/categories');
        categoriesColors = await resp.json();
        renderColorPickers();
        // заполнить select категорий в модальном окне
        const select = document.getElementById('editCategory');
        select.innerHTML = '';
        for (let cat in categoriesColors) {
            const option = document.createElement('option');
            option.value = cat;
            option.textContent = cat;
            select.appendChild(option);
        }
    }
    
    function renderColorPickers() {
        const container = document.getElementById('colorSettings');
        container.innerHTML = '';
        for (let [cat, color] of Object.entries(categoriesColors)) {
            const div = document.createElement('div');
            div.className = 'color-item';
            div.innerHTML = `
                <label>${cat}</label>
                <input type="color" value="${color}" data-category="${cat}" class="color-input">
            `;
            container.appendChild(div);
        }
    }
    
    async function saveColors() {
        const inputs = document.querySelectorAll('.color-input');
        const newColors = {};
        inputs.forEach(input => {
            const cat = input.getAttribute('data-category');
            newColors[cat] = input.value;
        });
        const resp = await fetch('/api/categories', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(newColors)
        });
        if (resp.ok) {
            categoriesColors = newColors;
            alert('Цвета сохранены');
            loadNodes(); // обновить таблицу, чтобы отобразились новые цвета маркеров (хотя на странице узлов нет маркеров, но для единообразия)
        } else {
            alert('Ошибка сохранения');
        }
    }
    
    async function loadNodes() {
        const resp = await fetch('/api/nodes');
        const nodes = await resp.json();
        const tbody = document.getElementById('nodeTableBody');
        tbody.innerHTML = '';
        for (const node of nodes) {
            const row = tbody.insertRow();
            if (node.sos) row.classList.add('sos');
            row.insertCell(0).innerText = node.id;
            row.insertCell(1).innerText = node.start_number || '—';
            row.insertCell(2).innerText = node.pilot1 || '—';
            row.insertCell(3).innerText = node.pilot2 || '—';
            row.insertCell(4).innerText = node.category || '—';
            row.insertCell(5).innerText = (node.battery !== undefined ? node.battery + '%' : '—');
            row.insertCell(6).innerHTML = `<span title="lat=${node.lat}, lon=${node.lon}">${node.lat?.toFixed(5) || '?'}, ${node.lon?.toFixed(5) || '?'}</span>`;
            const sosCell = row.insertCell(7);
            sosCell.innerText = node.sos ? 'АКТИВЕН' : '—';
            const actionsCell = row.insertCell(8);
            const editBtn = document.createElement('button');
            editBtn.className = 'edit';
            editBtn.innerText = '✏️';
            editBtn.onclick = () => openEditModal(node.id);
            const ackBtn = document.createElement('button');
            ackBtn.className = 'ack';
            ackBtn.innerText = '✅ Снять SOS';
            ackBtn.onclick = () => sendAck(node.id, node.sos_seq);
            if (!node.sos) ackBtn.disabled = true;
            actionsCell.appendChild(editBtn);
            actionsCell.appendChild(ackBtn);
        }
    }
    
    async function sendAck(nodeId, sosSeq) {
        if (!confirm(`Отправить подтверждение SOS узлу ${nodeId}?`)) return;
        const resp = await fetch('/api/ack', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ node_id: nodeId, sos_seq: sosSeq })
        });
        const result = await resp.json();
        if (result.status === 'ok') {
            alert('ACK отправлен');
            setTimeout(loadNodes, 1000);
        } else {
            alert('Ошибка: ' + (result.error || 'неизвестно'));
        }
    }
    
    let currentEditId = null;
    async function openEditModal(nodeId) {
        currentEditId = nodeId;
        const resp = await fetch(`/api/node?node_id=${nodeId}`);
        let data = {};
        if (resp.ok) data = await resp.json();
        document.getElementById('editNodeId').value = nodeId;
        document.getElementById('editStartNumber').value = data.start_number || '';
        document.getElementById('editPilot1').value = data.pilot1 || '';
        document.getElementById('editPilot2').value = data.pilot2 || '';
        document.getElementById('editCategory').value = data.category || '';
        document.getElementById('editAlias').value = data.alias || '';
        document.getElementById('editModal').style.display = 'flex';
    }
    
    async function saveNodeEdit() {
        const nodeId = parseInt(document.getElementById('editNodeId').value);
        const payload = {
            node_id: nodeId,
            start_number: document.getElementById('editStartNumber').value.trim(),
            pilot1: document.getElementById('editPilot1').value.trim(),
            pilot2: document.getElementById('editPilot2').value.trim(),
            category: document.getElementById('editCategory').value,
            alias: document.getElementById('editAlias').value.trim()
        };
        const resp = await fetch('/api/node', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if (resp.ok) {
            closeModal();
            loadNodes();
        } else {
            alert('Ошибка сохранения');
        }
    }
    
    function closeModal() {
        document.getElementById('editModal').style.display = 'none';
    }
    
    document.getElementById('saveEditBtn').onclick = saveNodeEdit;
    document.getElementById('cancelEditBtn').onclick = closeModal;
    window.onclick = (e) => { if (e.target === document.getElementById('editModal')) closeModal(); };
    
    loadColors();
    loadNodes();
    setInterval(loadNodes, 5000);
</script>
</body>
</html>'''
    return web.Response(text=html, content_type='text/html')



# Страница управления точками – без изменений (оставляем как было)
async def waypoints_manage_page(request):
    html = '''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Управление точками (КП/лагерь)</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <style>
        body { margin:0; padding:0; font-family: sans-serif; display: flex; flex-direction: column; height: 100vh; }
        #topbar {
            background: #2c3e50;
            color: white;
            padding: 10px;
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            align-items: center;
        }
        #topbar button, #topbar input, #topbar select {
            padding: 6px 12px;
            border: none;
            border-radius: 4px;
        }
        #topbar button {
            background: #27ae60;
            color: white;
            cursor: pointer;
        }
        #topbar button:hover { background: #2ecc71; }
        #map { flex: 1; height: 60%; }
        #table-container {
            height: 40%;
            overflow: auto;
            background: #ecf0f1;
            padding: 10px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            background: white;
        }
        th, td {
            border: 1px solid #bdc3c7;
            padding: 6px;
            text-align: left;
        }
        th { background: #34495e; color: white; }
        .actions button {
            margin: 0 2px;
            padding: 2px 6px;
            cursor: pointer;
        }
        .form-row {
            display: flex;
            gap: 10px;
            align-items: center;
            flex-wrap: wrap;
        }
        .form-row input, .form-row select {
            padding: 5px;
        }
        hr { margin: 10px 0; }
    </style>
</head>
<body>
<div id="topbar">
    <h3 style="margin:0;">📌 Редактор точек</h3>
    <a href="/" style="color:white; text-decoration:none; background:#e67e22; padding:6px 12px; border-radius:4px;">← Карта</a>
    <span style="flex:1"></span>
    <button id="exportGpxBtn">📥 Экспорт GPX</button>
    <label>📂 Импорт GPX: <input type="file" id="gpxFile" accept=".gpx"></label>
    <button id="importGpxBtn">Загрузить</button>
</div>
<div id="map"></div>
<div id="table-container">
    <h4>Добавить / редактировать точку</h4>
    <div class="form-row">
        <input type="hidden" id="editId">
        <input type="text" id="name" placeholder="Название" required>
        <input type="text" id="lat" placeholder="Широта" step="any">
        <input type="text" id="lon" placeholder="Долгота" step="any">
        <select id="type">
            <option value="checkpoint">Контрольный пункт (КП)</option>
            <option value="basecamp">Базовый лагерь</option>
        </select>
        <select id="categories" multiple size="5">
            <option value="Полироль">Полироль</option>
            <option value="Стандарт">Стандарт</option>
            <option value="Туризм">Туризм</option>
            <option value="Спорт">Спорт</option>
            <option value="Организатор">Организатор</option>
            <option value="Базовый лагерь">Базовый лагерь</option>
        </select>
        <input type="text" id="description" placeholder="Описание">
        <button id="saveBtn">💾 Сохранить</button>
        <button id="clearBtn">🗑 Очистить</button>
    </div>
    <hr>
    <h4>Список точек</h4>
    <table id="waypointsTable">
        <thead>
            <tr><th>ID</th><th>Название</th><th>Широта</th><th>Долгота</th><th>Тип</th><th>Категория</th><th>Описание</th><th>Действия</th></tr>
        </thead>
        <tbody></tbody>
    </table>
</div>
<script>
    var map = L.map('map').setView([55.751244, 37.618423], 10);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>'
    }).addTo(map);
    var markerLayer = L.layerGroup().addTo(map);
    var categoryColors = {};

    async function loadColors() {
        const resp = await fetch('/api/categories');
        categoryColors = await resp.json();
    }

    function getColor(cat) {
        return categoryColors[cat] || '#888888';
    }

    function createIcon(category, type) {
        var color = getColor(category);
        var html = '<div style="background:'+color+'; width:28px; height:28px; border-radius:4px; border:2px solid white; text-align:center; line-height:24px;">📍</div>';
        if (type === 'basecamp') {
            html = '<div style="background:'+color+'; width:32px; height:32px; border-radius:50%; border:2px solid white; text-align:center; line-height:28px;">🏕️</div>';
        }
        return L.divIcon({ className: '', html: html, iconSize: [28,28] });
    }

    async function loadWaypoints() {
    const resp = await fetch('/api/waypoints');
    const wps = await resp.json();
    markerLayer.clearLayers();
    const tbody = document.querySelector('#waypointsTable tbody');
    tbody.innerHTML = '';
    for (let wp of wps) {
        const firstCat = (wp.categories && wp.categories.length) ? wp.categories[0] : '';
        const icon = createIcon(firstCat, wp.type);
        const marker = L.marker([wp.lat, wp.lon], { icon: icon }).bindPopup(`<b>${wp.name}</b><br>${wp.type === 'basecamp' ? 'Лагерь' : 'КП'}<br>Категории: ${(wp.categories || []).join(', ')}`);
        markerLayer.addLayer(marker);
        const row = tbody.insertRow();
        row.insertCell(0).innerText = wp.id;
        row.insertCell(1).innerText = wp.name;
        row.insertCell(2).innerText = wp.lat;
        row.insertCell(3).innerText = wp.lon;
        row.insertCell(4).innerText = wp.type === 'basecamp' ? 'Лагерь' : 'КП';
        row.insertCell(5).innerText = (wp.categories || []).join(', ');
        row.insertCell(6).innerText = wp.description || '';
        const actions = row.insertCell(7);
        actions.className = 'actions';
        const editBtn = document.createElement('button');
        editBtn.innerText = '✏️';
        editBtn.onclick = () => {
            document.getElementById('editId').value = wp.id;
            document.getElementById('name').value = wp.name;
            document.getElementById('lat').value = wp.lat;
            document.getElementById('lon').value = wp.lon;
            document.getElementById('type').value = wp.type;
            // отметить выбранные категории
            const select = document.getElementById('categories');
            for (let i = 0; i < select.options.length; i++) {
                select.options[i].selected = wp.categories.includes(select.options[i].value);
            }
            document.getElementById('description').value = wp.description || '';
        };
        const delBtn = document.createElement('button');
        delBtn.innerText = '🗑️';
        delBtn.onclick = async () => {
            if (confirm('Удалить точку?')) {
                await fetch(`/api/waypoints/${wp.id}`, { method: 'DELETE' });
                loadWaypoints();
            }
        };
        actions.appendChild(editBtn);
        actions.appendChild(delBtn);
    }
}

    document.getElementById('saveBtn').onclick = async () => {
    const id = document.getElementById('editId').value;
    const name = document.getElementById('name').value.trim();
    const lat = parseFloat(document.getElementById('lat').value);
    const lon = parseFloat(document.getElementById('lon').value);
    if (!name || isNaN(lat) || isNaN(lon)) { alert('Заполните название и координаты'); return; }
    const categories = Array.from(document.getElementById('categories').selectedOptions).map(opt => opt.value);
    const payload = {
        name, lat, lon,
        categories: categories,
        type: document.getElementById('type').value,
        description: document.getElementById('description').value
    };
    if (id) payload.id = parseInt(id);
    const resp = await fetch('/api/waypoints', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    });
    if (resp.ok) {
        document.getElementById('clearBtn').click();
        loadWaypoints();
    } else { alert('Ошибка'); }
};
    document.getElementById('clearBtn').onclick = () => {
    document.getElementById('editId').value = '';
    document.getElementById('name').value = '';
    document.getElementById('lat').value = '';
    document.getElementById('lon').value = '';
    document.getElementById('type').value = 'checkpoint';
    const select = document.getElementById('categories');
    for (let i = 0; i < select.options.length; i++) {
        select.options[i].selected = false;
    }
    document.getElementById('description').value = '';
};
    document.getElementById('exportGpxBtn').onclick = () => {
        window.location.href = '/api/waypoints/export/gpx';
    };
    document.getElementById('importGpxBtn').onclick = async () => {
        const fileInput = document.getElementById('gpxFile');
        if (!fileInput.files.length) { alert('Выберите GPX файл'); return; }
        const file = fileInput.files[0];
        const formData = new FormData();
        formData.append('file', file);
        const resp = await fetch('/api/waypoints/import/gpx', { method: 'POST', body: formData });
        const result = await resp.json();
        if (resp.ok) {
            alert(`Импортировано ${result.imported} точек`);
            loadWaypoints();
        } else { alert('Ошибка: ' + result.error); }
    };
    loadColors().then(() => { loadWaypoints(); });
    map.on('click', (e) => {
        document.getElementById('lat').value = e.latlng.lat.toFixed(6);
        document.getElementById('lon').value = e.latlng.lng.toFixed(6);
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
    app.router.add_get('/nodes', nodes_page)
    app.router.add_get('/api/nodes', api_nodes)
    app.router.add_get('/api/tracks', api_tracks)
    app.router.add_get('/api/categories', api_categories)
    app.router.add_post('/api/categories', api_categories)
    app.router.add_get('/api/node', api_node_info)
    app.router.add_post('/api/node', api_node_info)
    app.router.add_post('/api/ack', api_ack)
    app.router.add_get('/api/waypoints', api_waypoints)
    app.router.add_post('/api/waypoints', api_waypoints)
    app.router.add_delete('/api/waypoints/{id}', api_waypoint_delete)
    app.router.add_get('/api/waypoints/export/gpx', api_export_gpx)
    app.router.add_post('/api/waypoints/import/gpx', api_import_gpx)
    app.router.add_get('/waypoints/manage', waypoints_manage_page)
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