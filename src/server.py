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
from aiohttp import web
import aiofiles

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
    async with aiofiles.open('templates/index.html', 'r', encoding='utf-8') as f:
        content = await f.read()
    return web.Response(text=content, content_type='text/html')

# Страница управления узлами
async def nodes_page(request):
    async with aiofiles.open('templates/nodes.html', 'r', encoding='utf-8') as f:
        content = await f.read()
    return web.Response(text=content, content_type='text/html')

# Страница управления точками – без изменений (оставляем как было)
async def waypoints_manage_page(request):
    async with aiofiles.open('templates/waypoints_manage.html', 'r', encoding='utf-8') as f:
        content = await f.read()
    return web.Response(text=content, content_type='text/html')

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