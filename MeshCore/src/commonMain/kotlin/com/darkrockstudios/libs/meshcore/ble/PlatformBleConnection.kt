package com.darkrockstudios.libs.meshcore.ble

import dev.bluefalcon.BlueFalcon
import dev.bluefalcon.BluetoothPeripheral

/**
 * Opens a MeshCore BLE connection for the current platform.
 *
 * Android must track the [android.bluetooth.BluetoothGatt] returned by
 * `connectGatt` itself: BlueFalcon 2.5.x discards that handle and only
 * registers GATTs after STATE_CONNECTED, so a timed-out connect leaves an
 * orphaned client that keeps holding the peripheral (no advertise, no
 * reconnect, blocks other centrals) until airplane mode clears the stack.
 */
internal expect suspend fun platformOpenBleConnection(
	blueFalcon: BlueFalcon,
	peripheral: BluetoothPeripheral,
	deviceIdentifier: String,
	platformAppContext: Any?,
): BleConnection
