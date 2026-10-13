package com.darkrockstudios.libs.meshcore.ble

import dev.bluefalcon.BlueFalcon
import dev.bluefalcon.BluetoothPeripheral

internal actual suspend fun platformOpenBleConnection(
	blueFalcon: BlueFalcon,
	peripheral: BluetoothPeripheral,
	deviceIdentifier: String,
	platformAppContext: Any?,
	autoConnect: Boolean,
): BleConnection {
	val connection =
		BlueFalconBleConnection(
			blueFalcon = blueFalcon,
			peripheral = peripheral,
			deviceIdentifier = deviceIdentifier,
			autoConnect = autoConnect,
		)
	connection.connectAndSetup()
	return connection
}
