package com.darkrockstudios.libs.meshcore.ble

import dev.bluefalcon.core.BlueFalcon
import dev.bluefalcon.core.BluetoothPeripheral

/**
 * Opens a MeshCore BLE connection for the current platform.
 *
 * Both Android and iOS route through [BlueFalconBleConnection]. BlueFalcon
 * 3.5.1+ tracks the `connectGatt` handle before STATE_CONNECTED, so a
 * timed-out direct connect can be released via `disconnect()` (the 2.5.x
 * orphan that previously required a raw Android GATT client).
 */
internal expect suspend fun platformOpenBleConnection(
	blueFalcon: BlueFalcon,
	peripheral: BluetoothPeripheral,
	deviceIdentifier: String,
): BleConnection
