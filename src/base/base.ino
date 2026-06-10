#include <Arduino.h>
#include <HardwareSerial.h>
#include <Preferences.h>
#include <WiFi.h>
#include <WebServer.h>
#include <WebSocketsClient.h>
#include <ArduinoJson.h>
#include "tracker_config.h"

// ======================== Глобальные объекты ========================
HardwareSerial RadioSerial(2);
Preferences prefs;
WebSocketsClient webSocket;
WebServer server(80);          // Веб-сервер для режима STA (основной интерфейс)
WebServer configServer(80);    // Веб-сервер для режима AP (первоначальная настройка)

bool wsConnected = false;
bool webServerStarted = false; // Флаг, запущен ли основной веб-сервер

// ======================== Структуры ========================
struct __attribute__((packed)) TrackerPacket {
  uint16_t magic;
  uint8_t version;
  uint8_t type;
  uint16_t nodeId;
  uint16_t targetId;
  uint32_t sequence;
  uint32_t gpsTime;
  int32_t latE7;
  int32_t lonE7;
  int32_t altitudeDm;
  uint16_t speedCentiKmh;
  uint16_t courseCentideg;
  uint16_t batteryMv;
  uint8_t batteryPercent;
  uint8_t satellites;
  uint16_t flags;
  uint8_t ttl;
  uint8_t hops;
  uint8_t linkQuality;
  uint16_t sosSequence;
  uint32_t uptimeSeconds;
  uint8_t role;
  uint16_t crc;
};

struct NodeInfo {
  uint16_t id = 0;
  int32_t latE7 = 0;
  int32_t lonE7 = 0;
  uint16_t speedCentiKmh = 0;
  uint16_t batteryMv = 0;
  uint8_t batteryPercent = 0;
  uint8_t satellites = 0;
  uint32_t lastSeen = 0;
  int16_t rssi = 0;
  uint8_t hops = 0;
  uint16_t flags = 0;
  uint32_t lastSequence = 0;
  uint32_t packetsReceived = 0;
  uint32_t packetsMissed = 0;
  uint8_t linkQuality = 0;
  uint32_t uptimeSeconds = 0;
  uint8_t role = 0;
  bool used = false;
};

struct SeenPacket {
  uint16_t nodeId = 0;
  uint32_t sequence = 0;
  uint32_t seenAt = 0;
  SeenPacket() {}
  SeenPacket(uint16_t nid, uint32_t seq, uint32_t seen) : nodeId(nid), sequence(seq), seenAt(seen) {}
};

// Константы
static constexpr uint16_t PACKET_MAGIC = 0x4F54;
static constexpr size_t SEEN_CACHE_SIZE = 96;
static constexpr uint32_t SEEN_CACHE_TTL_MS = 300000;

// Глобальные данные
NodeInfo nodes[MAX_NODE_COUNT];
SeenPacket seenPackets[SEEN_CACHE_SIZE];

SemaphoreHandle_t stateMutex;
SemaphoreHandle_t radioTxMutex;

// ======================== CRC ========================
uint16_t crc16Ccitt(const uint8_t *data, size_t length) {
  uint16_t crc = 0xFFFF;
  for (size_t i = 0; i < length; ++i) {
    crc ^= static_cast<uint16_t>(data[i]) << 8;
    for (uint8_t bit = 0; bit < 8; ++bit) {
      crc = (crc & 0x8000) ? (crc << 1) ^ 0x1021 : crc << 1;
    }
  }
  return crc;
}

uint16_t packetCrc(const TrackerPacket &packet) {
  TrackerPacket copy = packet;
  copy.crc = 0;
  return crc16Ccitt(reinterpret_cast<const uint8_t *>(&copy), sizeof(copy));
}

bool validPacket(const TrackerPacket &packet) {
  return packet.magic == PACKET_MAGIC && packet.version == TRACKER_FIRMWARE_VERSION &&
         packetCrc(packet) == packet.crc;
}

// ======================== E220 ========================
void setE220NormalMode() {
  pinMode(E220_M0_PIN, OUTPUT);
  pinMode(E220_M1_PIN, OUTPUT);
  pinMode(E220_AUX_PIN, INPUT);
  digitalWrite(E220_M0_PIN, LOW);
  digitalWrite(E220_M1_PIN, LOW);
  delay(40);
}

void waitRadioReady(uint32_t timeoutMs = 200) {
  uint32_t start = millis();
  while (digitalRead(E220_AUX_PIN) == LOW && millis() - start < timeoutMs) delay(1);
}

void sendRadioPacket(const TrackerPacket &packet) {
  xSemaphoreTake(radioTxMutex, portMAX_DELAY);
  waitRadioReady();
  RadioSerial.write(reinterpret_cast<const uint8_t *>(&packet), sizeof(packet));
  RadioSerial.flush();
  xSemaphoreGive(radioTxMutex);
}

// ======================== Дубликаты и таблица узлов ========================
bool isDuplicate(uint16_t sourceNodeId, uint32_t sequence) {
  uint32_t now = millis();
  int freeSlot = -1, oldestSlot = 0;
  uint32_t oldestAge = 0;
  for (size_t i = 0; i < SEEN_CACHE_SIZE; ++i) {
    bool expired = (now - seenPackets[i].seenAt) > SEEN_CACHE_TTL_MS;
    if (seenPackets[i].nodeId == sourceNodeId && seenPackets[i].sequence == sequence && !expired)
      return true;
    if ((seenPackets[i].seenAt == 0 || expired) && freeSlot < 0) freeSlot = i;
    uint32_t age = now - seenPackets[i].seenAt;
    if (age > oldestAge) { oldestAge = age; oldestSlot = i; }
  }
  int slot = freeSlot >= 0 ? freeSlot : oldestSlot;
  seenPackets[slot] = SeenPacket(sourceNodeId, sequence, now);
  return false;
}

void updateNodeTable(const TrackerPacket &packet, int16_t rssi) {
  int freeSlot = -1, targetSlot = -1;
  uint32_t oldestSeen = UINT32_MAX;
  int oldestSlot = 0;
  for (size_t i = 0; i < MAX_NODE_COUNT; ++i) {
    if (nodes[i].used && nodes[i].id == packet.nodeId) { targetSlot = i; break; }
    if (!nodes[i].used && freeSlot < 0) freeSlot = i;
    if (nodes[i].used && nodes[i].lastSeen < oldestSeen) {
      oldestSeen = nodes[i].lastSeen; oldestSlot = i;
    }
  }
  if (targetSlot < 0) targetSlot = freeSlot >= 0 ? freeSlot : oldestSlot;
  NodeInfo &node = nodes[targetSlot];
  bool sameNode = node.used && node.id == packet.nodeId;
  if (sameNode && packet.sequence < node.lastSequence) {
    node.packetsReceived = 0; node.packetsMissed = 0;
  } else if (sameNode && packet.sequence > node.lastSequence + 1) {
    node.packetsMissed += packet.sequence - node.lastSequence - 1;
  }
  if (!sameNode) { node.packetsReceived = 0; node.packetsMissed = 0; }
  node.used = true;
  node.id = packet.nodeId;
  node.latE7 = packet.latE7;
  node.lonE7 = packet.lonE7;
  node.speedCentiKmh = packet.speedCentiKmh;
  node.batteryMv = packet.batteryMv;
  node.batteryPercent = packet.batteryPercent;
  node.satellites = packet.satellites;
  node.lastSeen = millis();
  node.rssi = rssi;
  node.hops = packet.hops;
  node.flags = packet.flags;
  node.uptimeSeconds = packet.uptimeSeconds;
  node.role = packet.role;
  node.lastSequence = packet.sequence;
  node.packetsReceived++;
  uint32_t total = node.packetsReceived + node.packetsMissed;
  node.linkQuality = total ? (node.packetsReceived * 100UL) / total : 0;
}

// ======================== WebSocket ========================
void sendPacketToServer(const TrackerPacket &pkt, int16_t rssi) {
  if (!wsConnected) return;
  StaticJsonDocument<512> doc;
  doc["type"] = "telemetry";
  doc["nodeId"] = pkt.nodeId;
  doc["targetId"] = pkt.targetId;
  doc["sequence"] = pkt.sequence;
  doc["gpsTime"] = pkt.gpsTime;
  doc["lat"] = pkt.latE7 / 10000000.0;
  doc["lon"] = pkt.lonE7 / 10000000.0;
  doc["altitude"] = pkt.altitudeDm / 10.0;
  doc["speed"] = pkt.speedCentiKmh / 100.0;
  doc["course"] = pkt.courseCentideg / 100.0;
  doc["batteryMv"] = pkt.batteryMv;
  doc["batteryPercent"] = pkt.batteryPercent;
  doc["satellites"] = pkt.satellites;
  doc["flags"] = pkt.flags;
  doc["ttl"] = pkt.ttl;
  doc["hops"] = pkt.hops;
  doc["sosSequence"] = pkt.sosSequence;
  doc["uptime"] = pkt.uptimeSeconds;
  doc["role"] = pkt.role;
  doc["rssi"] = rssi;
  doc["timestamp"] = millis();

  String json;
  serializeJson(doc, json);
  webSocket.sendTXT(json);
}

void handleServerCommand(const String &payload) {
  StaticJsonDocument<512> doc;
  DeserializationError err = deserializeJson(doc, payload);
  if (err) return;
  const char* cmd = doc["command"];
  if (!cmd) return;

  if (strcmp(cmd, "send_ack") == 0) {
    uint16_t target = doc["targetId"] | 0;
    uint16_t sosSeq = doc["sosSequence"] | 0;
    TrackerPacket ack{};
    ack.magic = PACKET_MAGIC;
    ack.version = TRACKER_FIRMWARE_VERSION;
    ack.type = 3; // PACKET_ACK_SOS
    ack.nodeId = BASE_NODE_ID;
    ack.targetId = target;
    ack.sequence = random(0xFFFF);
    ack.sosSequence = sosSeq;
    ack.ttl = 3;
    ack.crc = packetCrc(ack);
    sendRadioPacket(ack);
    Serial.printf("Sent ACK to node %d, SOS seq %d\n", target, sosSeq);
  }
}

void webSocketEvent(WStype_t type, uint8_t *payload, size_t length) {
  switch (type) {
    case WStype_CONNECTED:
      wsConnected = true;
      Serial.println("[WS] Connected to server");
      break;
    case WStype_DISCONNECTED:
      wsConnected = false;
      Serial.println("[WS] Disconnected");
      break;
    case WStype_TEXT:
      handleServerCommand(String((char*)payload));
      break;
    default: break;
  }
}

// ======================== Веб-интерфейс (режим STA) ========================
String htmlHeader(const String &title) {
  String html;
  html += "<!DOCTYPE html><html lang='ru'><head>";
  html += "<meta charset='UTF-8'>";
  html += "<meta name='viewport' content='width=device-width,initial-scale=1'>";
  html += "<title>" + title + "</title>";

  html += R"rawliteral(
  <style>
  *{box-sizing:border-box}
  body{
      margin:0;
      padding:20px;
      background:#111827;
      color:#f3f4f6;
      font-family:Arial,sans-serif;
  }

  .container{
      max-width:1200px;
      margin:auto;
  }

  .card{
      background:#1f2937;
      border-radius:12px;
      padding:16px;
      margin-bottom:16px;
  }

  h1,h2{
      margin-top:0;
  }

  a{
      color:#60a5fa;
      text-decoration:none;
  }

  a:hover{
      text-decoration:underline;
  }

  .grid{
      display:grid;
      grid-template-columns:repeat(auto-fit,minmax(220px,1fr));
      gap:12px;
  }

  .value{
      font-size:24px;
      font-weight:bold;
      margin-top:8px;
  }

  .ok{
      color:#10b981;
  }

  .bad{
      color:#ef4444;
  }

  table{
      width:100%;
      border-collapse:collapse;
      margin-top:10px;
  }

  th{
      background:#374151;
  }

  td,th{
      padding:8px;
      border:1px solid #4b5563;
      text-align:center;
  }

  tr:nth-child(even){
      background:#1f2937;
  }

  .sos{
      background:#7f1d1d !important;
      color:white;
      font-weight:bold;
  }

  input{
      width:100%;
      padding:10px;
      margin:6px 0 12px;
      border:none;
      border-radius:8px;
      background:#374151;
      color:white;
  }

  button{
      padding:10px 20px;
      border:none;
      border-radius:8px;
      background:#2563eb;
      color:white;
      cursor:pointer;
  }

  button:hover{
      background:#1d4ed8;
  }
  </style>
  )rawliteral";

  html += "</head><body><div class='container'>";
  return html;
}

void handleRoot() {

  int nodeCount = 0;
  uint32_t lastPacketAge = 0xFFFFFFFF;
  
  xSemaphoreTake(stateMutex, portMAX_DELAY);
  
  for(size_t i=0;i<MAX_NODE_COUNT;i++)
  {
	  if(nodes[i].used)
		  {
			  nodeCount++;
			  
			  uint32_t age = (millis() - nodes[i].lastSeen) / 1000;
			  
			  if(age < lastPacketAge) lastPacketAge = age;
		  }
  }
  
  xSemaphoreGive(stateMutex);
  if(lastPacketAge == 0xFFFFFFFF) lastPacketAge = 0;

  String page = htmlHeader("Tracker Base");

  page += "<h1>Offroad Tracker Base</h1>";

  page += "<div class='grid'>";

  page += "<div class='card'>";
  page += "<h2>Wi-Fi</h2>";
  page += "<div class='value " +
          String(WiFi.status()==WL_CONNECTED?"ok":"bad") +
          "'>" +
          String(WiFi.status()==WL_CONNECTED?"ONLINE":"OFFLINE") +
          "</div></div>";

  page += "<div class='card'>";
  page += "<h2>WebSocket</h2>";
  page += "<div class='value " +
          String(wsConnected?"ok":"bad") +
          "'>" +
          String(wsConnected?"ONLINE":"OFFLINE") +
          "</div></div>";

  page += "<div class='card'>";
  page += "<h2>Узлы</h2>";
  page += "<div class='value'>" + String(nodeCount) + "</div>";
  page += "</div>";
  
  page += "<div class='card'>";
  page += "<p><b>Последний пакет:</b> ";
  page += String(lastPacketAge);
  page += " сек назад</p>";
  page += "</div>";

  page += "<div class='card'>";
  page += "<h2>Память</h2>";
  page += "<div class='value'>" + String(ESP.getFreeHeap()/1024) + " KB</div>";
  page += "</div>";

  page += "</div>";

  page += "<div class='card'>";
  page += "<p><b>IP:</b> " + WiFi.localIP().toString() + "</p>";
  page += "<p><b>Uptime:</b> " + String(millis()/1000) + " сек</p>";
  page += "</div>";

  page += "<div class='card'>";
  page += "<a href='/nodes'>Узлы сети</a><br><br>";
  page += "<a href='/settings'>Настройки</a><br><br>";
  page += "<a href='/reset'>Сброс настроек</a>";
  page += "</div>";

  page += R"rawliteral(
  <script>
  setTimeout(()=>location.reload(),5000);
  </script>
  )rawliteral";

  page += "</div></body></html>";

  server.send(200,"text/html",page);
}

void handleNodes() {
  String page = htmlHeader("Узлы сети");
  page += "<h1>Узлы сети</h1>";
  page += "<table>";
  page += "<tr><th>ID</th><th>Lat</th><th>Lon</th><th>Speed</th><th>Battery</th><th>Sat</th><th>Role</th><th>RSSI</th><th>LQ</th><th>Hops</th><th>Uptime</th><th>LastSeen</th></tr>";
  xSemaphoreTake(stateMutex, portMAX_DELAY);
  for (size_t i = 0; i < MAX_NODE_COUNT; i++) {
    if (!nodes[i].used) continue;
    bool sos = nodes[i].flags & 0x0001;
	page += sos ? "<tr class='sos'>" : "<tr>";
    page += "<td>" + String(nodes[i].id) + "</td>";
    page += "<td>" + String(nodes[i].latE7 / 10000000.0, 6) + "</td>";
    page += "<td>" + String(nodes[i].lonE7 / 10000000.0, 6) + "</td>";
    page += "<td>" + String(nodes[i].speedCentiKmh / 100.0, 1) + "</td>";
    page += "<td>" + String(nodes[i].batteryPercent) + "%</td>";
    page += "<td>" + String(nodes[i].satellites) + "</td>";
    page += "<td>" + String(nodes[i].role) + "</td>";
    page += "<td>" + String(nodes[i].rssi) + "</td>";
    page += "<td>" + String(nodes[i].linkQuality) + "%</td>";
    page += "<td>" + String(nodes[i].hops) + "</td>";
    page += "<td>" + String(nodes[i].uptimeSeconds) + "s</td>";
    page += "<td>" + String((millis() - nodes[i].lastSeen) / 1000) + "s</td>";
    page += "</tr>";
  }
  xSemaphoreGive(stateMutex);
  page += "</table>";
  page += "<p><a href='/'>Назад</a></p>";
  
  page += R"rawliteral(<script>setTimeout(()=>location.reload(),3000);</script>)rawliteral";
  
  page += "</div></body></html>";
  server.send(200, "text/html", page);
}

void handleSettings() {
  prefs.begin("base", false);
  String ssid = prefs.getString("wifi_ssid", "");
  String pass = prefs.getString("wifi_pass", "");
  String host = prefs.getString("server_host", "");
  uint16_t port = prefs.getUShort("server_port", 5000);
  prefs.end();

  String page = htmlHeader("Настройки");
  page += "<h1>Настройки</h1>";
  page += "<form method='POST' action='/settings'>";
  page += "Wi-Fi SSID: <input name='ssid' value='" + ssid + "'><br>";
  page += "Wi-Fi пароль: <input name='pass' type='password' value='" + pass + "'><br>";
  page += "IP сервера: <input name='host' value='" + host + "'><br>";
  page += "Порт сервера: <input name='port' type='number' value='" + String(port) + "'><br>";
  page += "<button type='submit'>Сохранить и перезагрузить</button>";
  page += "</form>";
  page += "<p><a href='/'>Назад</a></p>";
  page += "</div></body></html>";
  server.send(200, "text/html", page);
}

void handleSaveSettings() {
  String ssid = server.arg("ssid");
  String pass = server.arg("pass");
  String host = server.arg("host");
  uint16_t port = server.arg("port").toInt();

  prefs.begin("base", false);
  if (ssid.length() > 0) prefs.putString("wifi_ssid", ssid);
  if (pass.length() > 0) prefs.putString("wifi_pass", pass);
  if (host.length() > 0) prefs.putString("server_host", host);
  if (port > 0) prefs.putUShort("server_port", port);
  prefs.end();

  server.send(200, "text/html", "<html><body><h2>Настройки сохранены. Перезагрузка...</h2></body></html>");
  delay(1000);
  ESP.restart();
}

void handleReset() {
  prefs.begin("base", false);
  prefs.clear();
  prefs.end();
  server.send(200, "text/html", "<html><body><h2>Настройки сброшены. Перезагрузка...</h2></body></html>");
  delay(1000);
  ESP.restart();
}

void startWebServer() {
  if (webServerStarted) return;
  server.on("/", handleRoot);
  server.on("/nodes", handleNodes);
  server.on("/settings", HTTP_GET, handleSettings);
  server.on("/settings", HTTP_POST, handleSaveSettings);
  server.on("/reset", handleReset);
  server.begin();
  webServerStarted = true;
  Serial.println("Web server started");
}

// ======================== Подключение к WiFi и WS ========================
void connectToWiFiAndWS() {
  prefs.begin("base", false);
  String ssid = prefs.getString("wifi_ssid", "");
  String pass = prefs.getString("wifi_pass", "");
  String serverHost = prefs.getString("server_host", "");
  uint16_t serverPort = prefs.getUShort("server_port", 0);
  prefs.end();

  if (ssid.length() > 0 && serverHost.length() > 0) {
    WiFi.mode(WIFI_STA);
    WiFi.begin(ssid.c_str(), pass.c_str());
    Serial.print("Connecting to WiFi");
    int attempts = 0;
    while (WiFi.status() != WL_CONNECTED && attempts < 30) {
      delay(500);
      Serial.print(".");
      attempts++;
    }
    if (WiFi.status() == WL_CONNECTED) {
      Serial.println("\nWiFi connected, IP: " + WiFi.localIP().toString());
      startWebServer();          // запускаем основной веб-сервер
      webSocket.begin(serverHost, serverPort, "/ws");
      webSocket.onEvent(webSocketEvent);
      webSocket.setReconnectInterval(5000);
      return;
    } else {
      Serial.println("\nFailed to connect, starting AP mode.");
    }
  }

  // Режим точки доступа с веб-порталом для первоначальной настройки
  WiFi.mode(WIFI_AP);
  WiFi.softAP("Tracker-Base", "baseconfig");
  Serial.println("AP started. SSID: Tracker-Base, password: baseconfig");
  Serial.println("Connect and open http://192.168.4.1");

  configServer.on("/", []() {
    String html = "<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Tracker Base Setup</title>";
    html += "<style>body{font-family:sans-serif;margin:40px;background:#f0f0f0;}";
    html += "input,button{display:block;margin:15px 0;padding:8px;width:300px;}";
    html += "button{background:#4CAF50;color:white;border:none;cursor:pointer;}</style>";
    html += "</head><body><h1>Настройка базы трекера</h1>";
    html += "<form method='POST' action='/save'>";
    html += "Wi-Fi SSID: <input type='text' name='ssid' required><br>";
    html += "Wi-Fi пароль: <input type='password' name='pass' required><br>";
    html += "IP сервера (Python): <input type='text' name='host' value='192.168.1.100' required><br>";
    html += "Порт сервера: <input type='number' name='port' value='5000' required><br>";
    html += "<button type='submit'>Сохранить и перезагрузить</button>";
    html += "</form></body></html>";
    configServer.send(200, "text/html", html);
  });
  configServer.on("/save", HTTP_POST, []() {
    String ssid = configServer.arg("ssid");
    String pass = configServer.arg("pass");
    String host = configServer.arg("host");
    uint16_t port = configServer.arg("port").toInt();
    prefs.begin("base", false);
    prefs.putString("wifi_ssid", ssid);
    prefs.putString("wifi_pass", pass);
    prefs.putString("server_host", host);
    prefs.putUShort("server_port", port);
    prefs.end();
    configServer.send(200, "text/html", "<html><body><h2>Сохранено. Перезагрузка...</h2></body></html>");
    delay(1000);
    ESP.restart();
  });
  configServer.begin();
}

// ======================== Задачи FreeRTOS ========================
void radioRxTask(void *) {
  enum RxState { WAIT_MAGIC1, WAIT_MAGIC2, RECEIVE_FRAME };
  const size_t frameSize = sizeof(TrackerPacket) + (E220_RSSI_BYTE_ENABLED ? 1 : 0);
  uint8_t buffer[frameSize];
  size_t offset = 0;
  RxState state = WAIT_MAGIC1;
  uint8_t magicLo = PACKET_MAGIC & 0xFF, magicHi = PACKET_MAGIC >> 8;

  for (;;) {
    while (RadioSerial.available()) {
      uint8_t b = RadioSerial.read();
      switch (state) {
        case WAIT_MAGIC1:
          if (b == magicLo) { buffer[0] = b; offset = 1; state = WAIT_MAGIC2; }
          break;
        case WAIT_MAGIC2:
          if (b == magicHi) { buffer[1] = b; offset = 2; state = RECEIVE_FRAME; }
          else if (b == magicLo) { buffer[0] = b; offset = 1; }
          else state = WAIT_MAGIC1;
          break;
        case RECEIVE_FRAME:
          buffer[offset++] = b;
          if (offset == frameSize) {
            TrackerPacket pkt;
            memcpy(&pkt, buffer, sizeof(pkt));
            int16_t rssi = 0;
            if (E220_RSSI_BYTE_ENABLED) rssi = -static_cast<int16_t>(buffer[sizeof(pkt)]);
            if (validPacket(pkt) && !isDuplicate(pkt.nodeId, pkt.sequence)) {
              xSemaphoreTake(stateMutex, portMAX_DELAY);
              updateNodeTable(pkt, rssi);
              xSemaphoreGive(stateMutex);
              sendPacketToServer(pkt, rssi);
            }
            state = WAIT_MAGIC1;
          }
          break;
      }
    }
    vTaskDelay(pdMS_TO_TICKS(5));
  }
}

void wsSendTask(void *) {
  for (;;) {
    webSocket.loop();
    vTaskDelay(pdMS_TO_TICKS(10));
  }
}

void statusReportTask(void *) {
  for (;;) {
    if (wsConnected) {
      StaticJsonDocument<1024> doc;
      doc["type"] = "status";
      doc["uptime"] = millis() / 1000;
      doc["freeHeap"] = ESP.getFreeHeap();
      JsonArray arr = doc.createNestedArray("nodes");
      xSemaphoreTake(stateMutex, portMAX_DELAY);
      for (size_t i = 0; i < MAX_NODE_COUNT; i++) {
        if (nodes[i].used) {
          JsonObject n = arr.createNestedObject();
          n["id"] = nodes[i].id;
          n["lat"] = nodes[i].latE7 / 10000000.0;
          n["lon"] = nodes[i].lonE7 / 10000000.0;
          n["lastSeen"] = (millis() - nodes[i].lastSeen) / 1000;
          n["rssi"] = nodes[i].rssi;
          n["lq"] = nodes[i].linkQuality;
        }
      }
      xSemaphoreGive(stateMutex);
      String json;
      serializeJson(doc, json);
      webSocket.sendTXT(json);
    }
    vTaskDelay(pdMS_TO_TICKS(30000));
  }
}

// ======================== Setup ========================
void setup() {
  Serial.begin(115200);
  delay(200);

  randomSeed(esp_random());

  stateMutex = xSemaphoreCreateMutex();
  radioTxMutex = xSemaphoreCreateMutex();
  if (!stateMutex || !radioTxMutex) {
    Serial.println("Mutex creation failed!");
    ESP.restart();
  }

  setE220NormalMode();
  RadioSerial.begin(E220_UART_BAUD, SERIAL_8N1, E220_RX_PIN, E220_TX_PIN);
  RadioSerial.setRxBufferSize(4096);

  connectToWiFiAndWS();

  xTaskCreatePinnedToCore(radioRxTask, "radioRx", 4096, NULL, 3, NULL, 1);
  xTaskCreatePinnedToCore(wsSendTask, "wsLoop", 4096, NULL, 2, NULL, 0);
  xTaskCreatePinnedToCore(statusReportTask, "status", 4096, NULL, 1, NULL, 1);

  Serial.println("Base station started");
}

void loop() {
  if (WiFi.getMode() == WIFI_AP) {
    configServer.handleClient(); // режим AP: портал настройки
  } else if (webServerStarted) {
    server.handleClient();       // режим STA: основной веб-интерфейс
  }
  vTaskDelay(pdMS_TO_TICKS(10));
}