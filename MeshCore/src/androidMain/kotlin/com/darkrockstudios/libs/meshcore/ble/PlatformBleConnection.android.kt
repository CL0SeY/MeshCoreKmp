package com.darkrockstudios.libs.meshcore.ble

import android.content.Context
import dev.bluefalcon.BlueFalcon
import dev.bluefalcon.BluetoothPeripheral

internal actual suspend fun platformOpenBleConnection(
	blueFalcon: BlueFalcon,
	peripheral: BluetoothPeripheral,
	deviceIdentifier: String,
	platformAppContext: Any?,
): BleConnection {
	val context =
		(platformAppContext as? Context)?.applicationContext
			?: throw MeshCoreBleException(
				"Android BLE connect requires an Application Context " +
					"(pass platformAppContext to BlueFalconBleAdapter)",
			)
	return AndroidGattBleConnection.open(
		context = context,
		device = peripheral.device,
		deviceIdentifier = deviceIdentifier,
	)
}
