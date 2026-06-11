#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Эмулятор ESP32 для тестирования трекера (расширенная версия).
Отправляет полную телеметрию и статус на WebSocket сервер.
Поддерживает несколько виртуальных узлов.
Принимает команды ACK от сервера и выключает SOS на соответствующем узле.
"""

import asyncio
import websockets
import json
import random
import math
import argparse
from datetime import datetime

# Конфигурация по умолчанию
DEFAULT_HOST = "localhost"
DEFAULT_WS_PORT = 6000
DEFAULT_NODES = 4
DEFAULT_INTERVAL = 3  # секунд между отправками для каждого узла
DEFAULT_START_LAT = 59.46059
DEFAULT_START_LON = 56.55113

class SimulatedNode:
    def __init__(self, node_id, start_lat, start_lon):
        self.node_id = node_id
        self.lat = start_lat + random.uniform(-0.00001, 0.0001)
        self.lon = start_lon + random.uniform(-0.00001, 0.0001)
        self.altitude = random.uniform(100, 500)  # метры
        self.speed = random.uniform(0, 100)       # км/ч
        self.course = random.uniform(0, 360)      # градусы
        self.battery_percent = random.randint(30, 100)
        self.battery_mv = self.battery_percent * 42  # 4.2V при 100%
        self.satellites = random.randint(3, 19)
        self.rssi = random.randint(-90, -40)
        self.hops = random.randint(0, 3)
        self.flags = 0        # бит 1 (0x02) = SOS
        self.sos_seq = 0
        self.role = 0
        self.uptime = random.randint(100, 10000)   # секунды
        self.ttl = random.randint(1, 5)           # Time To Live
        self.link_quality = random.randint(50, 100)  # %
        self.angle = random.uniform(0, 360)       # для движения
        self.radius = random.uniform(0.0001, 0.0005)  # ~10-50 метров
        
        # Для статуса
        self.free_heap = random.randint(10000, 50000)
        self.visible_nodes = random.sample(range(1, 20), random.randint(0, 8))
        
    def update(self):
        # Движение: меняем угол, обновляем координаты
        self.angle += random.uniform(-5, 5)
        self.lat += math.cos(math.radians(self.angle)) * self.radius * random.uniform(0.5, 1.5)
        self.lon += math.sin(math.radians(self.angle)) * self.radius * random.uniform(0.5, 1.5)
        
        # Случайные изменения параметров
        self.speed = max(0, min(120, self.speed + random.uniform(-5, 5)))
        self.course = (self.course + random.uniform(-10, 10)) % 360
        self.altitude = max(0, min(2000, self.altitude + random.uniform(-10, 10)))
        self.battery_percent = max(0, min(100, self.battery_percent - random.uniform(0, 0.5)))
        self.battery_mv = self.battery_percent * 42  # упрощённо
        self.satellites = max(0, min(15, self.satellites + random.randint(-1, 1)))
        self.rssi = max(-110, min(-30, self.rssi + random.randint(-5, 5)))
        self.hops = max(0, min(5, self.hops + random.randint(-1, 1)))
        self.uptime += 3
        self.ttl = max(1, min(10, self.ttl + random.randint(-1, 1)))
        self.link_quality = max(0, min(100, self.link_quality + random.randint(-5, 5)))
        self.free_heap += random.randint(-1000, 1000)
        self.free_heap = max(1000, min(100000, self.free_heap))
        
        # Иногда меняем видимые узлы
        if random.random() < 0.1:
            self.visible_nodes = random.sample(range(1, 20), random.randint(0, 10))
    
    def toggle_sos(self):
        if self.flags & 2:
            self.flags &= ~2  # выключить SOS
            self.sos_seq = 0
            print(f"[NODE {self.node_id}] SOS выключен")
        else:
            self.flags |= 2   # включить SOS
            self.sos_seq = random.randint(1, 65535)
            print(f"[NODE {self.node_id}] SOS АКТИВИРОВАН! seq={self.sos_seq}")
    
    def ack_sos(self, received_seq):
        """Принять ACK: выключить SOS, если последовательность совпадает."""
        if (self.flags & 2) and (received_seq == 0 or received_seq == self.sos_seq):
            self.flags &= ~2
            self.sos_seq = 0
            print(f"[NODE {self.node_id}] ACK принят! SOS выключен.")
            return True
        return False
    
    def get_telemetry(self):
        """Возвращает полный пакет телеметрии, соответствующий протоколу."""
        return {
            "type": "telemetry",
            "nodeId": self.node_id,
            "targetId": random.randint(0, 65535),
            "sequence": random.randint(0, 65535),
            "gpsTime": int(datetime.now().timestamp()),
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "altitude": round(self.altitude, 1),
            "speed": round(self.speed, 1),
            "course": round(self.course, 1),
            "batteryMv": int(self.battery_mv),
            "batteryPercent": int(self.battery_percent),
            "satellites": self.satellites,
            "flags": self.flags,
            "ttl": self.ttl,
            "hops": self.hops,
            "sosSequence": self.sos_seq,
            "uptime": int(self.uptime),
            "role": self.role,
            "rssi": self.rssi,
            "lq": self.link_quality,      # качество связи
            "timestamp": int(datetime.now().timestamp() * 1000)
        }
    
    def get_status(self):
        """Статусное сообщение (отправляется реже)."""
        return {
            "type": "status",
            "uptime": self.uptime,
            "freeHeap": self.free_heap,
            "nodes": self.visible_nodes
        }

async def send_loop(websocket, node, interval):
    """Отправляет телеметрию и периодически статус для одного узла."""
    try:
        status_counter = 0
        while True:
            node.update()
            telemetry = node.get_telemetry()
            await websocket.send(json.dumps(telemetry))
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Узел {node.node_id}: "
                  f"{node.lat:.5f}, {node.lon:.5f} | {node.speed:.1f} км/ч | "
                  f"выс={node.altitude:.0f}м | бат={node.battery_percent}% | "
                  f"RSSI={node.rssi} | LQ={node.link_quality}% | "
                  f"SOS={'ДА' if (node.flags&2) else 'НЕТ'}")
            
            # Статус отправляем раз в 10-15 секунд
            status_counter += interval
            if status_counter >= 12:  # примерно каждые 12-15 секунд
                status = node.get_status()
                await websocket.send(json.dumps(status))
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Статус узла {node.node_id}: "
                      f"uptime={node.uptime}c, heap={node.free_heap}, видимых={len(node.visible_nodes)}")
                status_counter = 0
            
            await asyncio.sleep(interval)
    except websockets.exceptions.ConnectionClosed:
        print(f"[NODE {node.node_id}] Соединение закрыто")
    except Exception as e:
        print(f"[NODE {node.node_id}] Ошибка: {e}")

async def recv_loop(websocket, nodes):
    """Принимает сообщения от сервера (команды ACK) и обрабатывает их."""
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
                if data.get("command") == "send_ack":
                    target_id = data.get("targetId")
                    sos_seq = data.get("sosSequence", 0)
                    print(f"[ACK] Получена команда для узла {target_id}, seq={sos_seq}")
                    node = next((n for n in nodes if n.node_id == target_id), None)
                    if node:
                        node.ack_sos(sos_seq)
                    else:
                        print(f"[ACK] Узел {target_id} не найден")
                else:
                    print(f"[RECV] Неизвестное сообщение: {data}")
            except json.JSONDecodeError:
                print(f"[RECV] Некорректный JSON: {message}")
    except websockets.exceptions.ConnectionClosed:
        print("[RECV] Соединение закрыто (приём)")

async def handle_input(nodes):
    """Обработка команд с клавиатуры для управления SOS."""
    loop = asyncio.get_event_loop()
    print("\n=== Управление эмулятором ===")
    print("Команды: sos <node_id>  - включить/выключить SOS")
    print("         list           - показать все узлы")
    print("         quit           - выход")
    while True:
        try:
            line = await loop.run_in_executor(None, input, "> ")
            if not line:
                continue
            parts = line.strip().split()
            if not parts:
                continue
            cmd = parts[0].lower()
            if cmd == "quit" or cmd == "exit":
                print("Завершение эмуляции...")
                return False
            elif cmd == "list":
                for node in nodes:
                    sos_status = "SOS" if (node.flags & 2) else "OK"
                    print(f"Узел {node.node_id}: {sos_status}, бат={node.battery_percent}%, "
                          f"коорд={node.lat:.5f},{node.lon:.5f}, выс={node.altitude:.0f}м, "
                          f"скорость={node.speed:.1f}км/ч, LQ={node.link_quality}%")
            elif cmd == "sos" and len(parts) >= 2:
                try:
                    node_id = int(parts[1])
                    node = next((n for n in nodes if n.node_id == node_id), None)
                    if node:
                        node.toggle_sos()
                    else:
                        print(f"Узел {node_id} не найден")
                except ValueError:
                    print("ID узла должно быть числом")
            else:
                print("Неизвестная команда. Доступно: sos <id>, list, quit")
        except EOFError:
            break
    return True

async def main():
    parser = argparse.ArgumentParser(description="Эмулятор ESP32 для трекера (расширенный, со всеми полями)")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"WebSocket хост (по умолч. {DEFAULT_HOST})")
    parser.add_argument("--port", type=int, default=DEFAULT_WS_PORT, help=f"WebSocket порт (по умолч. {DEFAULT_WS_PORT})")
    parser.add_argument("--nodes", type=int, default=DEFAULT_NODES, help=f"Количество эмулируемых узлов (по умолч. {DEFAULT_NODES})")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL, help=f"Интервал отправки телеметрии (сек) (по умолч. {DEFAULT_INTERVAL})")
    parser.add_argument("--lat", type=float, default=DEFAULT_START_LAT, help="Начальная широта")
    parser.add_argument("--lon", type=float, default=DEFAULT_START_LON, help="Начальная долгота")
    args = parser.parse_args()
    
    uri = f"ws://{args.host}:{args.port}"
    print(f"Подключение к {uri} ...")
    
    try:
        async with websockets.connect(uri) as websocket:
            print("Подключено к WebSocket серверу!")
            
            # Создаём эмулируемые узлы
            nodes = []
            for i in range(1, args.nodes + 1):
                node = SimulatedNode(i, args.lat, args.lon)
                nodes.append(node)
            print(f"Создано {len(nodes)} виртуальных узлов (ID: {', '.join(str(n.node_id) for n in nodes)})")
            print("Отправляются все поля: lat, lon, altitude, speed, course, battery_mv, battery_percent,")
            print("satellites, flags (SOS), ttl, hops, sos_seq, uptime, rssi, link_quality")
            
            # Запускаем задачи отправки для каждого узла
            send_tasks = []
            for node in nodes:
                task = asyncio.create_task(send_loop(websocket, node, args.interval))
                send_tasks.append(task)
            
            # Запускаем задачу приёма сообщений (одна на всё соединение)
            recv_task = asyncio.create_task(recv_loop(websocket, nodes))
            
            # Запускаем обработку команд с клавиатуры
            should_exit = await handle_input(nodes)
            
            if should_exit is False:
                for task in send_tasks:
                    task.cancel()
                recv_task.cancel()
                await asyncio.gather(*send_tasks, recv_task, return_exceptions=True)
                
    except websockets.exceptions.ConnectionRefusedError:
        print(f"Ошибка: Не удалось подключиться к {uri}. Убедитесь, что сервер запущен.")
    except KeyboardInterrupt:
        print("\nПрерывание пользователем")

if __name__ == "__main__":
    asyncio.run(main())