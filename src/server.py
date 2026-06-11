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
import aiofiles
import aiohttp
import shutil
from math import log, tan, pi, cos, atan, exp

HOST = "0.0.0.0"
WS_PORT = 6000
HTTP_PORT = 8080
DB_PATH = "tracker.db"
AUTO_ACK = False
CATEGORY_COLORS_FILE = "category_colors.json"

DEFAULT_CATEGORY_COLORS = {
    "Полироль": "#00ff00",
    "Стандарт": "#0066ff",
    "Туризм": "#ff8800",
    "Спорт": "#ff00ff",
    "Организатор": "#000000",
    "Базовый лагерь": "#8B4513",
}

# ----------------------------------------------------------------------
# Фоновые задачи скачивания тайлов с прогрессом
# ----------------------------------------------------------------------
download_tasks = {}
task_counter = 0

async def run_tile_download(task_id, sw_lat, sw_lon, ne_lat, ne_lon, min_zoom, max_zoom):
    """Фоновая задача, обновляет словарь download_tasks[task_id]"""
    tiles_dir = "templates/tiles"
    os.makedirs(tiles_dir, exist_ok=True)

    # Сначала соберём все тайлы, которые действительно нужно скачать (отсутствуют)
    all_tiles = []
    for z in range(min_zoom, max_zoom + 1):
        min_x, max_x, min_y, max_y = get_tile_range(sw_lat, sw_lon, ne_lat, ne_lon, z)
        for x in range(min_x, max_x + 1):
            for y in range(min_y, max_y + 1):
                # Проверяем, существует ли уже файл
                save_path = os.path.join(tiles_dir, str(z), str(x), f"{y}.png")
                if os.path.exists(save_path):
                    continue   # пропускаем существующий тайл
                all_tiles.append((z, x, y))

    total = len(all_tiles)
    # Обновляем запись задачи (уже должна быть создана в api_download_tiles)
    download_tasks[task_id]["total"] = total
    if total == 0:
        download_tasks[task_id]["finished"] = True
        return

    semaphore = asyncio.Semaphore(5)
    downloaded = 0
    failed = 0

    async def download_one(z, x, y):
        nonlocal downloaded, failed
        async with semaphore:
            if download_tasks[task_id].get("cancelled", False):
                return
            url = f"https://a.basemaps.cartocdn.com/rastertiles/voyager_labels_under/{z}/{x}/{y}.png"
            save_path = os.path.join(tiles_dir, str(z), str(x), f"{y}.png")
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.get(url) as resp:
                        if resp.status == 200:
                            os.makedirs(os.path.dirname(save_path), exist_ok=True)
                            with open(save_path, 'wb') as f:
                                f.write(await resp.read())
                            downloaded += 1
                        else:
                            failed += 1
                            print(f"Tile {z}/{x}/{y} HTTP {resp.status}")
            except Exception as e:
                failed += 1
                print(f"Error {z}/{x}/{y}: {e}")
            finally:
                download_tasks[task_id]["downloaded"] = downloaded
                download_tasks[task_id]["failed"] = failed

    # Запускаем все задачи конкурентно с ограничением
    await asyncio.gather(*[download_one(z, x, y) for (z, x, y) in all_tiles])
    download_tasks[task_id]["finished"] = True

# Новый эндпоинт – запуск скачивания
async def api_download_tiles(request):
    global task_counter
    try:
        data = await request.json()
        bounds = data.get("bounds")
        min_zoom = int(data.get("minZoom"))
        max_zoom = int(data.get("maxZoom"))
        if not bounds or len(bounds) != 4:
            return web.json_response({"error": "bounds must be [south, west, north, east]"}, status=400)
        sw_lat, sw_lon, ne_lat, ne_lon = bounds
        task_counter += 1
        task_id = f"task_{task_counter}"
        # Создаём запись задачи сразу, чтобы статус был доступен
        download_tasks[task_id] = {
            "total": 0,
            "downloaded": 0,
            "failed": 0,
            "finished": False,
            "cancelled": False,
            "min_zoom": min_zoom,
            "max_zoom": max_zoom
        }
        # Запускаем фоновую задачу (она дополнит информацию)
        asyncio.create_task(run_tile_download(task_id, sw_lat, sw_lon, ne_lat, ne_lon, min_zoom, max_zoom))
        return web.json_response({"status": "started", "task_id": task_id})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)
        
async def api_download_status(request):
    task_id = request.query.get("task_id")
    if not task_id or task_id not in download_tasks:
        return web.json_response({"error": "task not found"}, status=404)
    return web.json_response(download_tasks[task_id])

async def api_cancel_download(request):
    task_id = request.query.get("task_id")
    if not task_id or task_id not in download_tasks:
        return web.json_response({"error": "task not found"}, status=404)
    download_tasks[task_id]["cancelled"] = True
    return web.json_response({"status": "cancelled"})

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
    # Расширенная таблица nodes
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
            altitude REAL,
            speed REAL,
            course REAL,
            battery_mv INTEGER,
            battery_percent INTEGER,
            satellites INTEGER,
            rssi INTEGER,
            hops INTEGER,
            flags INTEGER,
            sos_seq INTEGER,
            role INTEGER,
            uptime INTEGER,
            link_quality INTEGER,
            ttl INTEGER
        )
    ''')
    # Расширенная таблица tracks
    c.execute('''
        CREATE TABLE IF NOT EXISTS tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id INTEGER,
            timestamp INTEGER,
            lat REAL,
            lon REAL,
            altitude REAL,
            speed REAL,
            course REAL,
            battery_mv INTEGER,
            battery_percent INTEGER,
            satellites INTEGER,
            rssi INTEGER,
            hops INTEGER,
            flags INTEGER,
            sos_seq INTEGER,
            uptime INTEGER,
            ttl INTEGER,
            link_quality INTEGER
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
    
    # Миграция для старых БД: добавляем недостающие столбцы
    new_columns_nodes = [
        ('altitude', 'REAL'),
        ('course', 'REAL'),
        ('battery_mv', 'INTEGER'),
        ('uptime', 'INTEGER'),
        ('link_quality', 'INTEGER'),
        ('ttl', 'INTEGER')
    ]
    for col, col_type in new_columns_nodes:
        try:
            c.execute(f'ALTER TABLE nodes ADD COLUMN {col} {col_type}')
        except sqlite3.OperationalError:
            pass
    new_columns_tracks = [
        ('altitude', 'REAL'),
        ('course', 'REAL'),
        ('battery_mv', 'INTEGER'),
        ('uptime', 'INTEGER'),
        ('ttl', 'INTEGER'),
        ('link_quality', 'INTEGER')
    ]
    for col, col_type in new_columns_tracks:
        try:
            c.execute(f'ALTER TABLE tracks ADD COLUMN {col} {col_type}')
        except sqlite3.OperationalError:
            pass
    
    conn.commit()
    conn.close()
    if os.name == 'nt':
        os.chmod(DB_PATH, stat.S_IWRITE)
    else:
        os.chmod(DB_PATH, 0o666)
    print("[DB] Инициализирована (расширенные nodes, tracks, waypoints)")

# ----------------------------------------------------------------------
# Работа с БД (телеметрия) – расширенная
# ----------------------------------------------------------------------
async def update_node_telemetry(data):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT OR IGNORE INTO nodes (node_id) VALUES (?)
        ''', (data["nodeId"],))
        await db.execute('''
            UPDATE nodes SET
                last_seen = ?,
                lat = ?,
                lon = ?,
                altitude = ?,
                speed = ?,
                course = ?,
                battery_mv = ?,
                battery_percent = ?,
                satellites = ?,
                rssi = ?,
                hops = ?,
                flags = ?,
                sos_seq = ?,
                role = ?,
                uptime = ?,
                link_quality = ?,
                ttl = ?
            WHERE node_id = ?
        ''', (
            int(datetime.now().timestamp()),
            data.get("lat"),
            data.get("lon"),
            data.get("altitude"),
            data.get("speed"),
            data.get("course"),
            data.get("batteryMv"),
            data.get("batteryPercent"),
            data.get("satellites"),
            data.get("rssi"),
            data.get("hops"),
            data.get("flags", 0),
            data.get("sosSequence", 0),
            data.get("role", 0),
            data.get("uptime", 0),
            data.get("lq", 0),
            data.get("ttl", 0),
            data["nodeId"]
        ))
        await db.commit()

async def insert_track(data):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT INTO tracks 
            (node_id, timestamp, lat, lon, altitude, speed, course,
             battery_mv, battery_percent, satellites, rssi, hops,
             flags, sos_seq, uptime, ttl, link_quality)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data["nodeId"],
            int(datetime.now().timestamp()),
            data.get("lat"),
            data.get("lon"),
            data.get("altitude"),
            data.get("speed"),
            data.get("course"),
            data.get("batteryMv"),
            data.get("batteryPercent"),
            data.get("satellites"),
            data.get("rssi"),
            data.get("hops"),
            data.get("flags", 0),
            data.get("sosSequence", 0),
            data.get("uptime", 0),
            data.get("ttl", 0),
            data.get("lq", 0)
        ))
        await db.commit()

# ----------------------------------------------------------------------
# API для получения данных (возвращаем все поля)
# ----------------------------------------------------------------------
async def get_all_nodes():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('''
            SELECT node_id, alias, start_number, pilot1, pilot2, category,
                   lat, lon, altitude, speed, course, battery_mv, battery_percent,
                   satellites, rssi, hops, flags, sos_seq, role, uptime, link_quality, ttl,
                   last_seen
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
            "altitude": row[8],
            "speed": row[9],
            "course": row[10],
            "battery_mv": row[11],
            "battery": row[12],
            "satellites": row[13],
            "rssi": row[14],
            "hops": row[15],
            "flags": row[16],
            "sos_seq": row[17],
            "role": row[18],
            "uptime": row[19],
            "link_quality": row[20],
            "ttl": row[21],
            "last_seen": row[22],
            "sos": bool(row[16] & 2)
        })
    return nodes

async def get_tracks(node_id, hours=24):
    since = int((datetime.now() - timedelta(hours=hours)).timestamp())
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('''
            SELECT timestamp, lat, lon, altitude, speed, course, battery_percent,
                   satellites, rssi, hops, flags, uptime, link_quality
            FROM tracks 
            WHERE node_id = ? AND timestamp >= ?
            ORDER BY timestamp ASC
        ''', (node_id, since)) as cursor:
            rows = await cursor.fetchall()
    return [{
        "timestamp": r[0],
        "lat": r[1],
        "lon": r[2],
        "altitude": r[3],
        "speed": r[4],
        "course": r[5],
        "battery": r[6],
        "satellites": r[7],
        "rssi": r[8],
        "hops": r[9],
        "flags": r[10],
        "uptime": r[11],
        "link_quality": r[12]
    } for r in rows]

async def get_node_info(node_id):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('''
            SELECT node_id, alias, start_number, pilot1, pilot2, category,
                   lat, lon, altitude, speed, course, battery_percent, uptime, link_quality
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
            "category": row[5],
            "lat": row[6],
            "lon": row[7],
            "altitude": row[8],
            "speed": row[9],
            "course": row[10],
            "battery_percent": row[11],
            "uptime": row[12],
            "link_quality": row[13]
        }
    return None

async def update_node_info(node_id, data):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT OR IGNORE INTO nodes (node_id) VALUES (?)
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
    print(f"[TELEMETRY] Узел {node_id}: {data.get('lat')}, {data.get('lon')} | "
          f"выс={data.get('altitude')}м | скор={data.get('speed')}км/ч | "
          f"бат={data.get('batteryPercent')}% | LQ={data.get('lq')}%")
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
                else:
                    # Игнорируем другие сообщения (например, от эмулятора могут быть лишние)
                    pass
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
    
async def map_css(request):
    async with aiofiles.open('templates/map.css', 'r', encoding='utf-8') as f:
        content = await f.read()
    return web.Response(text=content, content_type='text/css')
    
async def map_js(request):
    async with aiofiles.open('templates/map.js', 'r', encoding='utf-8') as f:
        content = await f.read()
    return web.Response(text=content, content_type='application/json')

async def tile_download_page(request):
    async with aiofiles.open('templates/tile_download.html', 'r', encoding='utf-8') as f:
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
# Функции для работы с тайлами (Web Mercator)
# ----------------------------------------------------------------------
def lon_to_tile_x(lon, zoom):
    n = 2 ** zoom
    return int((lon + 180) / 360 * n)

def lat_to_tile_y(lat, zoom):
    n = 2 ** zoom
    lat_rad = lat * pi / 180
    y = (1 - log(tan(lat_rad) + 1 / cos(lat_rad)) / pi) / 2
    return int(y * n)

def get_tile_range(sw_lat, sw_lon, ne_lat, ne_lon, zoom):
    """Возвращает minX, maxX, minY, maxY для области в заданном зуме"""
    min_x = lon_to_tile_x(sw_lon, zoom)
    max_x = lon_to_tile_x(ne_lon, zoom)
    min_y = lat_to_tile_y(ne_lat, zoom)   # y идёт сверху вниз
    max_y = lat_to_tile_y(sw_lat, zoom)
    # упорядочиваем
    if min_x > max_x:
        min_x, max_x = max_x, min_x
    if min_y > max_y:
        min_y, max_y = max_y, min_y
    # ограничиваем допустимыми значениями
    max_tile = 2 ** zoom - 1
    min_x = max(0, min_x)
    max_x = min(max_tile, max_x)
    min_y = max(0, min_y)
    max_y = min(max_tile, max_y)
    return min_x, max_x, min_y, max_y

async def download_tile(session, z, x, y, save_path):
    """Скачивает один тайл и сохраняет в файл. Возвращает True при успехе."""
    url = f"https://a.basemaps.cartocdn.com/rastertiles/voyager_labels_under/{z}/{x}/{y}.png"
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                os.makedirs(os.path.dirname(save_path), exist_ok=True)
                with open(save_path, 'wb') as f:
                    f.write(await resp.read())
                return True
            else:
                print(f"Tile {z}/{x}/{y} HTTP {resp.status}")
                return False
    except Exception as e:
        print(f"Error downloading {z}/{x}/{y}: {e}")
        return False

async def download_tiles_for_area(sw_lat, sw_lon, ne_lat, ne_lon, min_zoom, max_zoom):
    """Скачивает все тайлы в заданной области и диапазоне зумов"""
    tiles_dir = "templates/tiles"
    os.makedirs(tiles_dir, exist_ok=True)
    
    total = 0
    failed = 0
    # Сначала соберём список всех (z,x,y)
    tasks = []
    for z in range(min_zoom, max_zoom + 1):
        min_x, max_x, min_y, max_y = get_tile_range(sw_lat, sw_lon, ne_lat, ne_lon, z)
        for x in range(min_x, max_x + 1):
            for y in range(min_y, max_y + 1):
                total += 1
                tasks.append((z, x, y))
    
    if not tasks:
        return 0, 0
    
    # Ограничиваем параллельные запросы, чтобы не перегружать сервер
    semaphore = asyncio.Semaphore(5)
    
    async def limited_download(z, x, y):
        async with semaphore:
            save_path = os.path.join(tiles_dir, str(z), str(x), f"{y}.png")
            return await download_tile(session, z, x, y, save_path)
    
    async with aiohttp.ClientSession() as session:
        # Запускаем все задачи с ограничением
        results = await asyncio.gather(*[limited_download(z, x, y) for (z, x, y) in tasks])
    
    failed = results.count(False)
    return total - failed, failed

# ----------------------------------------------------------------------
# Запуск HTTP и WebSocket серверов
# ----------------------------------------------------------------------
async def start_http_server():
    app = web.Application()
    app.router.add_get('/', index_page)
    app.router.add_get('/map.css', map_css)
    app.router.add_get('/map.js', map_js)
    app.router.add_get('/tile_download', tile_download_page)
    app.router.add_static('/tiles', 'templates/tiles')
    app.router.add_post('/api/download_tiles', api_download_tiles)
    app.router.add_get('/api/download_status', api_download_status)
    app.router.add_post('/api/cancel_download', api_cancel_download)
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