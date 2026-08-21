package com.darkrockstudios.libs.meshcore.ble

import dev.bluefalcon.core.BlueFalcon
import dev.bluefalcon.core.BluetoothPeripheral

internal actual suspend fun platformOpenBleConnection(
	blueFalcon: BlueFalcon,
	peripheral: BluetoothPeripheral,
	deviceIdentifier: String,
): BleConnection {
	val connection =
		BlueFalconBleConnection(
			blueFalcon = blueFalcon,
			peripheral = peripheral,
			deviceIdentifier = deviceIdentifier,
		)
	connection.connectAndSetup()
	return connection
}
