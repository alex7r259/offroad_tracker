#pragma once

#include <Arduino.h>

// E220-400T22D wiring: module RXD -> ESP32 GPIO14, module TXD -> ESP32 GPIO27.
static constexpr int E220_M0_PIN = 32;
static constexpr int E220_M1_PIN = 33;
static constexpr int E220_RX_PIN = 27;  // ESP32 RX from E220 TXD
static constexpr int E220_TX_PIN = 14;  // ESP32 TX to E220 RXD
static constexpr int E220_AUX_PIN = 15;
static constexpr uint32_t E220_UART_BAUD = 9600;
static constexpr bool E220_RSSI_BYTE_ENABLED = true; // Set false if your E220 does not append RSSI bytes.

// ATGM336H wiring: module TX -> ESP32 GPIO16, module RX -> ESP32 GPIO17.
static constexpr int GPS_RX_PIN = 16;  // ESP32 RX from GPS TX
static constexpr int GPS_TX_PIN = 17;  // ESP32 TX to GPS RX
static constexpr uint32_t GPS_UART_BAUD = 9600;

static constexpr int SOS_BUTTON_PIN = 25;  // Button to GND, use INPUT_PULLUP.
static constexpr int BATTERY_ADC_PIN = 34; // 18650 via 47k/22k divider.

static constexpr float BATTERY_DIVIDER_HIGH_OHM = 47000.0F;
static constexpr float BATTERY_DIVIDER_LOW_OHM = 22000.0F;
static constexpr float BATTERY_DIVIDER_RATIO =
    (BATTERY_DIVIDER_HIGH_OHM + BATTERY_DIVIDER_LOW_OHM) / BATTERY_DIVIDER_LOW_OHM;

static constexpr uint8_t TRACKER_FIRMWARE_VERSION = 1;
static constexpr uint16_t DEFAULT_NODE_ID = 1;
static constexpr uint16_t BASE_NODE_ID = 0;
static constexpr uint16_t MAX_NODE_COUNT = 50;

enum TrackerRole : uint8_t {
  ROLE_PARTICIPANT = 0,
  ROLE_ORGANIZER = 1,
  ROLE_AMBULANCE = 2,
  ROLE_EVACUATION = 3,
  ROLE_BASE = 4,
  ROLE_RELAY = 5,
};

static constexpr uint8_t DEFAULT_NODE_ROLE = ROLE_PARTICIPANT;

static constexpr uint8_t POSITION_TTL = 3;
static constexpr uint8_t SOS_TTL = 5;

static constexpr uint32_t DEFAULT_POSITION_INTERVAL_MS = 5000;
static constexpr uint32_t SOS_REPEAT_INTERVAL_MS = 1500;
static constexpr uint32_t BATTERY_READ_INTERVAL_MS = 30000;
static constexpr uint32_t NODE_OFFLINE_TIMEOUT_MS = 120000;
static constexpr uint32_t NODE_PURGE_TIMEOUT_MS = 1800000;
static constexpr uint32_t WEB_STATUS_PUSH_INTERVAL_MS = 1000;

static constexpr int16_t RELAY_WEAK_RSSI_DBM = -90;
static constexpr uint8_t RELAY_ALWAYS_UNTIL_HOPS = 2;
static constexpr size_t RELAY_QUEUE_LENGTH = 16;
static constexpr size_t ACK_QUEUE_LENGTH = 8;
static constexpr uint32_t RADIO_RX_BUFFER_SIZE = 4096;
static constexpr const char *AP_PASSWORD_PREFIX = "tracker";
