package com.darkrockstudios.libs.meshcore.ble

import dev.bluefalcon.core.BlueFalcon
import dev.bluefalcon.core.BluetoothPeripheral

/** iOS BlueFalcon resolves peripherals without the Android registry gap. */
internal actual fun platformRegisterPeripheral(
	blueFalcon: BlueFalcon,
	peripheral: BluetoothPeripheral,
) {
	// No-op.
}
