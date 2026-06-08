#include <Arduino.h>
#include <HardwareSerial.h>
#include <Preferences.h>
#include <TinyGPSPlus.h>
#include <WebServer.h>
#include <WiFi.h>

#include "tracker_config.h"

HardwareSerial GPSSerial(1);
HardwareSerial RadioSerial(2);
TinyGPSPlus gps;
Preferences prefs;
WebServer server(80);

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
  bool used = false;
};

struct SeenPacket {
  uint16_t nodeId = 0;
  uint32_t sequence = 0;
  uint32_t seenAt = 0;
};

enum PacketType : uint8_t {
  PACKET_POSITION = 1,
  PACKET_SOS = 2,
  PACKET_ACK_SOS = 3,
  PACKET_PING = 4,
};

enum PacketFlags : uint16_t {
  FLAG_GPS_FIX = 1 << 0,
  FLAG_SOS_ACTIVE = 1 << 1,
  FLAG_LOW_BATTERY = 1 << 2,
};

static constexpr uint16_t PACKET_MAGIC = 0x4F54; // "OT" little-endian on ESP32.
static constexpr size_t SEEN_CACHE_SIZE = 96;
static constexpr uint32_t SEEN_CACHE_TTL_MS = 300000;

NodeInfo nodes[MAX_NODE_COUNT];
SeenPacket seenPackets[SEEN_CACHE_SIZE];
SemaphoreHandle_t stateMutex;
SemaphoreHandle_t radioTxMutex;
QueueHandle_t relayQueue;

uint16_t nodeId = DEFAULT_NODE_ID;
String callsign = "CAR-1";
uint32_t positionIntervalMs = DEFAULT_POSITION_INTERVAL_MS;
uint8_t configuredPositionTtl = POSITION_TTL;
uint8_t configuredSosTtl = SOS_TTL;
uint32_t sequenceCounter = 0;
uint16_t sosSequenceCounter = 0;
uint16_t activeSosSequence = 0;

volatile bool buttonInterrupt = false;
bool sosActive = false;
bool sosAcked = false;
uint32_t lastButtonChangeMs = 0;
uint32_t lastPositionSentMs = 0;
uint32_t lastSosSentMs = 0;
uint32_t lastBatteryReadMs = 0;
uint32_t lastTxMs = 0;
uint16_t batteryMv = 0;
uint8_t batteryPercent = 0;

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

void setE220NormalMode() {
  pinMode(E220_M0_PIN, OUTPUT);
  pinMode(E220_M1_PIN, OUTPUT);
  pinMode(E220_AUX_PIN, INPUT);
  digitalWrite(E220_M0_PIN, LOW);
  digitalWrite(E220_M1_PIN, LOW);
  delay(40);
}

void waitRadioReady(uint32_t timeoutMs = 200) {
  const uint32_t start = millis();
  while (digitalRead(E220_AUX_PIN) == LOW && millis() - start < timeoutMs) {
    delay(1);
  }
}

uint8_t batteryPercentFromMv(uint16_t mv) {
  if (mv <= 3300) {
    return 0;
  }
  if (mv >= 4200) {
    return 100;
  }
  return static_cast<uint8_t>((static_cast<uint32_t>(mv - 3300) * 100U) / 900U);
}

void readBattery() {
  uint32_t mvAtAdc = analogReadMilliVolts(BATTERY_ADC_PIN);
  uint16_t calculated = static_cast<uint16_t>(mvAtAdc * BATTERY_DIVIDER_RATIO);
  xSemaphoreTake(stateMutex, portMAX_DELAY);
  batteryMv = calculated;
  batteryPercent = batteryPercentFromMv(calculated);
  xSemaphoreGive(stateMutex);
}

bool hasGpsFix() {
  return gps.location.isValid() && gps.location.age() < 5000 && gps.satellites.value() >= 3;
}

uint32_t gpsTimePacked() {
  if (!gps.date.isValid() || !gps.time.isValid()) {
    return 0;
  }
  return gps.time.hour() * 10000UL + gps.time.minute() * 100UL + gps.time.second();
}

uint16_t currentFlags() {
  uint16_t flags = 0;
  if (hasGpsFix()) {
    flags |= FLAG_GPS_FIX;
  }
  if (sosActive) {
    flags |= FLAG_SOS_ACTIVE;
  }
  if (batteryPercent <= 15) {
    flags |= FLAG_LOW_BATTERY;
  }
  return flags;
}

TrackerPacket buildPacket(PacketType type, uint16_t targetId = 0) {
  TrackerPacket packet{};
  packet.magic = PACKET_MAGIC;
  packet.version = TRACKER_FIRMWARE_VERSION;
  packet.type = type;
  packet.nodeId = nodeId;
  packet.targetId = targetId;
  packet.sequence = ++sequenceCounter;
  packet.gpsTime = gpsTimePacked();
  packet.latE7 = hasGpsFix() ? static_cast<int32_t>(gps.location.lat() * 10000000.0) : 0;
  packet.lonE7 = hasGpsFix() ? static_cast<int32_t>(gps.location.lng() * 10000000.0) : 0;
  packet.altitudeDm = gps.altitude.isValid() ? static_cast<int32_t>(gps.altitude.meters() * 10.0) : 0;
  packet.speedCentiKmh = gps.speed.isValid() ? static_cast<uint16_t>(gps.speed.kmph() * 100.0) : 0;
  packet.courseCentideg = gps.course.isValid() ? static_cast<uint16_t>(gps.course.deg() * 100.0) : 0;
  packet.batteryMv = batteryMv;
  packet.batteryPercent = batteryPercent;
  packet.satellites = gps.satellites.isValid() ? gps.satellites.value() : 0;
  packet.flags = currentFlags();
  packet.ttl = type == PACKET_SOS ? configuredSosTtl : configuredPositionTtl;
  packet.hops = 0;
  packet.linkQuality = 100;
  packet.sosSequence = activeSosSequence;
  if (type == PACKET_SOS && activeSosSequence == 0) {
    activeSosSequence = ++sosSequenceCounter;
    packet.sosSequence = activeSosSequence;
  }
  packet.crc = packetCrc(packet);
  return packet;
}

void sendPacket(const TrackerPacket &packet) {
  xSemaphoreTake(radioTxMutex, portMAX_DELAY);
  waitRadioReady();
  RadioSerial.write(reinterpret_cast<const uint8_t *>(&packet), sizeof(packet));
  RadioSerial.flush();
  lastTxMs = millis();
  xSemaphoreGive(radioTxMutex);
}

bool isDuplicate(uint16_t sourceNodeId, uint32_t sequence) {
  const uint32_t now = millis();
  int freeSlot = -1;
  int oldestSlot = 0;
  uint32_t oldestAge = 0;

  for (size_t i = 0; i < SEEN_CACHE_SIZE; ++i) {
    const bool expired = now - seenPackets[i].seenAt > SEEN_CACHE_TTL_MS;
    if (seenPackets[i].nodeId == sourceNodeId && seenPackets[i].sequence == sequence && !expired) {
      return true;
    }
    if ((seenPackets[i].seenAt == 0 || expired) && freeSlot < 0) {
      freeSlot = static_cast<int>(i);
    }
    const uint32_t age = now - seenPackets[i].seenAt;
    if (age > oldestAge) {
      oldestAge = age;
      oldestSlot = static_cast<int>(i);
    }
  }

  const int slot = freeSlot >= 0 ? freeSlot : oldestSlot;
  seenPackets[slot] = {sourceNodeId, sequence, now};
  return false;
}

void updateNodeTable(const TrackerPacket &packet, int16_t rssi = 0) {
  int freeSlot = -1;
  int targetSlot = -1;
  uint32_t oldestSeen = UINT32_MAX;
  int oldestSlot = 0;

  for (size_t i = 0; i < MAX_NODE_COUNT; ++i) {
    if (nodes[i].used && nodes[i].id == packet.nodeId) {
      targetSlot = static_cast<int>(i);
      break;
    }
    if (!nodes[i].used && freeSlot < 0) {
      freeSlot = static_cast<int>(i);
    }
    if (nodes[i].used && nodes[i].lastSeen < oldestSeen) {
      oldestSeen = nodes[i].lastSeen;
      oldestSlot = static_cast<int>(i);
    }
  }

  if (targetSlot < 0) {
    targetSlot = freeSlot >= 0 ? freeSlot : oldestSlot;
  }

  NodeInfo &node = nodes[targetSlot];
  const bool sameNode = node.used && node.id == packet.nodeId;
  if (sameNode && packet.sequence > node.lastSequence + 1) {
    node.packetsMissed += packet.sequence - node.lastSequence - 1;
  }
  if (!sameNode) {
    node.packetsReceived = 0;
    node.packetsMissed = 0;
  }

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
  node.lastSequence = packet.sequence;
  node.packetsReceived++;
  const uint32_t totalPackets = node.packetsReceived + node.packetsMissed;
  node.linkQuality = totalPackets > 0 ? static_cast<uint8_t>((node.packetsReceived * 100UL) / totalPackets) : 0;
}

bool shouldRelayPacket(const TrackerPacket &packet, int16_t rssi) {
  if (packet.ttl == 0 || packet.nodeId == nodeId || packet.type == PACKET_ACK_SOS) {
    return false;
  }

  const bool rssiUnknown = rssi == 0;
  return packet.hops < RELAY_ALWAYS_UNTIL_HOPS || rssiUnknown || rssi < RELAY_WEAK_RSSI_DBM;
}

void enqueueRelayPacket(TrackerPacket packet, int16_t rssi) {
  if (!shouldRelayPacket(packet, rssi)) {
    return;
  }
  packet.ttl--;
  packet.hops++;
  packet.crc = packetCrc(packet);
  xQueueSend(relayQueue, &packet, 0);
}

void purgeStaleNodes() {
  const uint32_t now = millis();
  xSemaphoreTake(stateMutex, portMAX_DELAY);
  for (NodeInfo &node : nodes) {
    if (node.used && now - node.lastSeen > NODE_PURGE_TIMEOUT_MS) {
      node = NodeInfo{};
    }
  }
  xSemaphoreGive(stateMutex);
}

void handleIncomingPacket(const TrackerPacket &packet, int16_t rssi = 0) {
  if (!validPacket(packet) || isDuplicate(packet.nodeId, packet.sequence)) {
    return;
  }

  xSemaphoreTake(stateMutex, portMAX_DELAY);
  updateNodeTable(packet, rssi);
  if (packet.type == PACKET_ACK_SOS && packet.targetId == nodeId) {
    sosAcked = true;
  }
  xSemaphoreGive(stateMutex);

  if (packet.type == PACKET_SOS && nodeId == BASE_NODE_ID) {
    TrackerPacket ack = buildPacket(PACKET_ACK_SOS, packet.nodeId);
    ack.ttl = SOS_TTL;
    ack.crc = packetCrc(ack);
    sendPacket(ack);
  }

  enqueueRelayPacket(packet, rssi);
}

void IRAM_ATTR onButtonInterrupt() {
  buttonInterrupt = true;
}

void processButton() {
  if (!buttonInterrupt) {
    return;
  }
  buttonInterrupt = false;

  const uint32_t now = millis();
  if (now - lastButtonChangeMs < 250) {
    return;
  }
  lastButtonChangeMs = now;

  if (digitalRead(SOS_BUTTON_PIN) == LOW) {
    xSemaphoreTake(stateMutex, portMAX_DELAY);
    sosActive = !sosActive;
    sosAcked = false;
    if (!sosActive) {
      activeSosSequence = 0;
    }
    xSemaphoreGive(stateMutex);
  }
}

String htmlHeader(const String &title) {
  String html = "<!doctype html><html lang='ru'><head><meta charset='utf-8'>";
  html += "<meta name='viewport' content='width=device-width,initial-scale=1'>";
  html += String("<title>") + title + "</title>";
  html += "<style>body{font-family:system-ui;margin:24px;background:#111;color:#eee}";
  html += "a{color:#8fd}input,button{font-size:16px;padding:8px;margin:4px}";
  html += "table{border-collapse:collapse;width:100%;margin-top:16px}td,th{border:1px solid #444;padding:6px}";
  html += ".sos{background:#721;color:#fff}.ok{color:#8f8}.bad{color:#f88}</style></head><body>";
  return html;
}

void handleRoot() {
  xSemaphoreTake(stateMutex, portMAX_DELAY);
  const bool fix = hasGpsFix();
  const bool sos = sosActive;
  const bool ack = sosAcked;
  const uint16_t batMv = batteryMv;
  const uint8_t batPct = batteryPercent;
  const uint32_t lastTx = lastTxMs;
  xSemaphoreGive(stateMutex);

  String page = htmlHeader("Offroad Tracker");
  page += "<h1>Offroad Tracker v1.0</h1>";
  page += String("<p>ID: <b>") + String(nodeId) + "</b> / " + callsign + "</p>";
  page += String("<p>Батарея: <b>") + String(batMv) + " mV</b> (" + String(batPct) + "%)</p>";
  page += String("<p>GPS Fix: <b class='") + String(fix ? "ok" : "bad") + "'>" +
          String(fix ? "есть" : "нет") + "</b>, спутники: " + String(gps.satellites.value()) + "</p>";
  page += String("<p>SOS: <b class='") + String(sos ? "bad" : "ok") + "'>" +
          String(sos ? "АКТИВЕН" : "выключен") + "</b> ACK: " + String(ack ? "да" : "нет") + "</p>";
  page += String("<p>Последняя передача: ") + String(lastTx == 0 ? 0 : (millis() - lastTx) / 1000) + " сек назад</p>";
  page += "<form method='post' action='/sos'><button>Переключить SOS</button></form>";
  page += "<p><a href='/settings'>Настройки</a> · <a href='/nodes'>Узлы сети</a></p></body></html>";
  server.send(200, "text/html", page);
}

void handleSettings() {
  String page = htmlHeader("Настройки трекера");
  page += "<h1>Настройки</h1><form method='post' action='/settings'>";
  page += String("ID участника: <input name='id' type='number' min='0' max='65535' value='") + String(nodeId) + "'><br>";
  page += String("Позывной: <input name='callsign' maxlength='24' value='") + callsign + "'><br>";
  page += String("Интервал передачи, мс: <input name='interval' type='number' min='1000' value='") + String(positionIntervalMs) + "'><br>";
  page += String("TTL POSITION: <input name='pttl' type='number' min='1' max='10' value='") + String(configuredPositionTtl) + "'><br>";
  page += String("TTL SOS: <input name='sttl' type='number' min='1' max='10' value='") + String(configuredSosTtl) + "'><br>";
  page += "<button>Сохранить</button></form><p><a href='/'>Назад</a></p></body></html>";
  server.send(200, "text/html", page);
}

void handleSaveSettings() {
  nodeId = server.arg("id").toInt();
  callsign = server.arg("callsign");
  positionIntervalMs = max(1000UL, static_cast<uint32_t>(server.arg("interval").toInt()));
  configuredPositionTtl = constrain(server.arg("pttl").toInt(), 1, 10);
  configuredSosTtl = constrain(server.arg("sttl").toInt(), 1, 10);

  prefs.putUShort("node_id", nodeId);
  prefs.putString("callsign", callsign);
  prefs.putUInt("interval", positionIntervalMs);
  prefs.putUChar("pttl", configuredPositionTtl);
  prefs.putUChar("sttl", configuredSosTtl);
  server.sendHeader("Location", "/settings");
  server.send(303);
}

void handleSosToggle() {
  xSemaphoreTake(stateMutex, portMAX_DELAY);
  sosActive = !sosActive;
  sosAcked = false;
  if (!sosActive) {
    activeSosSequence = 0;
  }
  xSemaphoreGive(stateMutex);
  server.sendHeader("Location", "/");
  server.send(303);
}

void handleNodes() {
  String page = htmlHeader("Узлы сети");
  page += "<h1>Узлы сети</h1><table><tr><th>ID</th><th>Lat</th><th>Lon</th><th>Speed</th><th>Battery</th><th>Sat</th><th>RSSI</th><th>LQ</th><th>Hops</th><th>Age</th></tr>";
  xSemaphoreTake(stateMutex, portMAX_DELAY);
  for (const NodeInfo &node : nodes) {
    if (!node.used) {
      continue;
    }
    const bool nodeSos = node.flags & FLAG_SOS_ACTIVE;
    page += String("<tr") + (nodeSos ? " class='sos'" : "") + "><td>" + String(node.id) + "</td><td>" +
            String(node.latE7 / 10000000.0, 7) + "</td><td>" + String(node.lonE7 / 10000000.0, 7) +
            "</td><td>" + String(node.speedCentiKmh / 100.0, 1) + "</td><td>" + String(node.batteryPercent) +
            "%</td><td>" + String(node.satellites) + "</td><td>" + String(node.rssi) + "</td><td>" +
            String(node.linkQuality) + "%</td><td>" + String(node.hops) + "</td><td>" +
            String((millis() - node.lastSeen) / 1000) + "s</td></tr>";
  }
  xSemaphoreGive(stateMutex);
  page += "</table><p><a href='/'>Назад</a></p></body></html>";
  server.send(200, "text/html", page);
}

void loadSettings() {
  prefs.begin("tracker", false);
  nodeId = prefs.getUShort("node_id", DEFAULT_NODE_ID);
  callsign = prefs.getString("callsign", String("CAR-") + String(nodeId));
  positionIntervalMs = prefs.getUInt("interval", DEFAULT_POSITION_INTERVAL_MS);
  configuredPositionTtl = prefs.getUChar("pttl", POSITION_TTL);
  configuredSosTtl = prefs.getUChar("sttl", SOS_TTL);
}

void startWebUi() {
  String ssid = String("Tracker-") + String(nodeId, HEX);
  WiFi.mode(WIFI_AP);
  WiFi.softAP(ssid.c_str());
  server.on("/", HTTP_GET, handleRoot);
  server.on("/settings", HTTP_GET, handleSettings);
  server.on("/settings", HTTP_POST, handleSaveSettings);
  server.on("/sos", HTTP_POST, handleSosToggle);
  server.on("/nodes", HTTP_GET, handleNodes);
  server.begin();
}

void gpsTask(void *) {
  for (;;) {
    while (GPSSerial.available()) {
      gps.encode(GPSSerial.read());
    }
    vTaskDelay(pdMS_TO_TICKS(10));
  }
}

void radioRxTask(void *) {
  static constexpr size_t radioFrameSize =
      sizeof(TrackerPacket) + (E220_RSSI_BYTE_ENABLED ? 1 : 0);
  static constexpr uint8_t magicLo = PACKET_MAGIC & 0xFF;
  static constexpr uint8_t magicHi = PACKET_MAGIC >> 8;
  uint8_t buffer[radioFrameSize];
  size_t offset = 0;

  for (;;) {
    while (RadioSerial.available()) {
      const uint8_t byte = RadioSerial.read();
      if (offset == 0 && byte != magicLo) {
        continue;
      }
      if (offset == 1 && byte != magicHi) {
        offset = byte == magicLo ? 1 : 0;
        buffer[0] = magicLo;
        continue;
      }

      buffer[offset++] = byte;
      if (offset == radioFrameSize) {
        TrackerPacket packet{};
        memcpy(&packet, buffer, sizeof(packet));
        const int16_t rssi = E220_RSSI_BYTE_ENABLED ? -static_cast<int16_t>(buffer[sizeof(packet)]) : 0;
        handleIncomingPacket(packet, rssi);
        offset = 0;
      }
    }
    vTaskDelay(pdMS_TO_TICKS(5));
  }
}

void relayTxTask(void *) {
  TrackerPacket packet{};
  for (;;) {
    if (xQueueReceive(relayQueue, &packet, portMAX_DELAY) == pdTRUE) {
      vTaskDelay(pdMS_TO_TICKS(random(25, 140)));
      sendPacket(packet);
    }
  }
}

void radioTxTask(void *) {
  for (;;) {
    const uint32_t now = millis();
    processButton();

    xSemaphoreTake(stateMutex, portMAX_DELAY);
    const bool shouldSendPosition = now - lastPositionSentMs >= positionIntervalMs;
    const bool shouldSendSos = sosActive && !sosAcked && now - lastSosSentMs >= SOS_REPEAT_INTERVAL_MS;
    xSemaphoreGive(stateMutex);

    if (shouldSendPosition) {
      TrackerPacket packet = buildPacket(PACKET_POSITION);
      sendPacket(packet);
      lastPositionSentMs = now;
    }

    if (shouldSendSos) {
      TrackerPacket packet = buildPacket(PACKET_SOS);
      sendPacket(packet);
      lastSosSentMs = now;
    }

    vTaskDelay(pdMS_TO_TICKS(50));
  }
}

void batteryTask(void *) {
  for (;;) {
    if (millis() - lastBatteryReadMs >= BATTERY_READ_INTERVAL_MS) {
      readBattery();
      purgeStaleNodes();
      lastBatteryReadMs = millis();
    }
    vTaskDelay(pdMS_TO_TICKS(1000));
  }
}

void webTask(void *) {
  for (;;) {
    server.handleClient();
    vTaskDelay(pdMS_TO_TICKS(10));
  }
}

void setup() {
  Serial.begin(115200);
  delay(200);
  randomSeed(esp_random());
  stateMutex = xSemaphoreCreateMutex();
  radioTxMutex = xSemaphoreCreateMutex();
  relayQueue = xQueueCreate(RELAY_QUEUE_LENGTH, sizeof(TrackerPacket));

  loadSettings();

  pinMode(SOS_BUTTON_PIN, INPUT_PULLUP);
  attachInterrupt(digitalPinToInterrupt(SOS_BUTTON_PIN), onButtonInterrupt, FALLING);

  analogReadResolution(12);
  analogSetPinAttenuation(BATTERY_ADC_PIN, ADC_11db);
  readBattery();

  setE220NormalMode();
  RadioSerial.begin(E220_UART_BAUD, SERIAL_8N1, E220_RX_PIN, E220_TX_PIN);
  GPSSerial.begin(GPS_UART_BAUD, SERIAL_8N1, GPS_RX_PIN, GPS_TX_PIN);
  startWebUi();

  xTaskCreatePinnedToCore(gpsTask, "gps", 4096, nullptr, 2, nullptr, 1);
  xTaskCreatePinnedToCore(radioRxTask, "radio_rx", 4096, nullptr, 3, nullptr, 1);
  xTaskCreatePinnedToCore(radioTxTask, "radio_tx", 4096, nullptr, 2, nullptr, 1);
  xTaskCreatePinnedToCore(relayTxTask, "relay_tx", 4096, nullptr, 2, nullptr, 1);
  xTaskCreatePinnedToCore(batteryTask, "battery", 2048, nullptr, 1, nullptr, 1);
  xTaskCreatePinnedToCore(webTask, "web", 4096, nullptr, 1, nullptr, 0);

  Serial.println("Offroad Tracker v1.0 started");
  Serial.print("AP SSID: Tracker-");
  Serial.println(String(nodeId, HEX));
}

void loop() {
  vTaskDelay(pdMS_TO_TICKS(1000));
}
