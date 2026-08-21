package com.darkrockstudios.libs.meshcore.ble

import dev.bluefalcon.core.BlueFalcon
import dev.bluefalcon.core.BluetoothPeripheral

/**
 * Ensures [peripheral] is present in BlueFalcon's engine peripheral registry.
 *
 * AndroidEngine only emits [BlueFalcon.connectionStateUpdates] /
 * [BlueFalcon.serviceDiscoveryUpdates] for addresses found via scan (or
 * otherwise present in that registry). A [BlueFalcon.retrievePeripheral]
 * instance created from a saved MAC is invisible to those callbacks, so GATT
 * can succeed while MeshCore's connect setup hangs until timeout.
 */
internal expect fun platformRegisterPeripheral(
	blueFalcon: BlueFalcon,
	peripheral: BluetoothPeripheral,
)
