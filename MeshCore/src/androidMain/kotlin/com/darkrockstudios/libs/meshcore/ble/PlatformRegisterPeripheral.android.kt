package com.darkrockstudios.libs.meshcore.ble

import dev.bluefalcon.core.BlueFalcon
import dev.bluefalcon.core.BluetoothPeripheral
import io.github.aakira.napier.Napier
import kotlinx.coroutines.flow.MutableStateFlow

/**
 * BlueFalcon 3.x has no public API to insert a [BluetoothPeripheral] into the
 * Android engine registry. Reconnect-by-address uses [BlueFalcon.retrievePeripheral],
 * which creates an instance that never receives connection/service callbacks
 * unless we place it in `_peripherals` (kept readable via consumer ProGuard rules).
 */
internal actual fun platformRegisterPeripheral(
	blueFalcon: BlueFalcon,
	peripheral: BluetoothPeripheral,
) {
	val engine = blueFalcon.engine
	val field =
		runCatching { engine.javaClass.getDeclaredField("_peripherals") }
			.getOrElse {
				Napier.w(tag = TAG) {
					"platformRegisterPeripheral: _peripherals field missing on ${engine.javaClass.name}"
				}
				return
			}
	field.isAccessible = true
	@Suppress("UNCHECKED_CAST")
	val flow =
		field.get(engine) as? MutableStateFlow<Set<BluetoothPeripheral>> ?: run {
			Napier.w(tag = TAG) { "platformRegisterPeripheral: unexpected _peripherals type" }
			return
		}
	val uuid = peripheral.uuid
	if (flow.value.any { it.uuid.equals(uuid, ignoreCase = true) }) {
		return
	}
	flow.value = flow.value + peripheral
	Napier.d(tag = TAG) { "Registered peripheral $uuid into BlueFalcon engine registry" }
}

private const val TAG = "MeshCoreBLE"
