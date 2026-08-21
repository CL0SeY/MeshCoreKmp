package com.darkrockstudios.libs.meshcore.ble

import dev.bluefalcon.core.BlueFalcon
import dev.bluefalcon.core.BluetoothManagerState
import dev.bluefalcon.core.BluetoothPeripheral
import dev.bluefalcon.core.ServiceFilter
import dev.bluefalcon.core.toUuid
import io.github.aakira.napier.Napier
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.launch

class BlueFalconBleAdapter(
	private val blueFalcon: BlueFalcon,
) : BleAdapter {

	private val peripheralCache = mutableMapOf<String, BluetoothPeripheral>()

	override val isBluetoothEnabled: Boolean
		get() = blueFalcon.managerState.value == BluetoothManagerState.Ready

	override fun scan(filter: ScanFilter): Flow<DiscoveredDevice> = callbackFlow {
		peripheralCache.clear()
		val emitted = mutableSetOf<String>()

		Napier.d(tag = TAG) {
			"Starting scan with filter: serviceUuid='${filter.serviceUuid}', namePrefix='${filter.namePrefix}'"
		}

		val collectJob = launch {
			blueFalcon.peripherals.collect { peripherals ->
				for (bluetoothPeripheral in peripherals) {
					val name = bluetoothPeripheral.name
					Napier.d(tag = TAG) {
						"didDiscoverDevice: name='$name', uuid='${bluetoothPeripheral.uuid}', rssi=${bluetoothPeripheral.rssi}"
					}

					if (filter.namePrefix != null &&
						(name == null || !name.startsWith(filter.namePrefix))
					) {
						Napier.d(tag = TAG) { "  FILTERED OUT by namePrefix '${filter.namePrefix}'" }
						continue
					}

					peripheralCache[bluetoothPeripheral.uuid] = bluetoothPeripheral
					if (!emitted.add(bluetoothPeripheral.uuid)) continue

					val device = DiscoveredDevice(
						identifier = bluetoothPeripheral.uuid,
						name = name,
						rssi = bluetoothPeripheral.rssi?.toInt() ?: 0,
					)
					Napier.d(tag = TAG) { "  Emitting device: $device" }
					trySend(device)
				}
			}
		}

		val serviceFilters = if (filter.serviceUuid.isNotBlank()) {
			listOf(ServiceFilter(filter.serviceUuid.toUuid()))
		} else {
			emptyList()
		}
		Napier.d(tag = TAG) { "Calling blueFalcon.scan() with ${serviceFilters.size} service filter(s)" }
		launch {
			blueFalcon.scan(serviceFilters)
		}

		awaitClose {
			collectJob.cancel()
			blueFalcon.engine.scope.launch {
				blueFalcon.stopScanning()
			}
		}
	}

	override fun stopScan() {
		blueFalcon.engine.scope.launch {
			blueFalcon.stopScanning()
		}
	}

	override suspend fun connect(device: DiscoveredDevice): BleConnection {
		// Prefer a fresh peripheral from BlueFalcon's registry on reconnect —
		// the cache may hold a stale handle from before a deep-sleep wake.
		val freshPeripheral = blueFalcon.retrievePeripheral(device.identifier)
		val peripheral = freshPeripheral
			?: peripheralCache[device.identifier]
			?: throw MeshCoreBleException(
				"Peripheral not found for identifier: ${device.identifier}"
			)
		Napier.d(tag = TAG) {
			"connect(): ${device.identifier} via ${if (freshPeripheral != null) "BlueFalcon registry" else "scan cache"}"
		}

		return platformOpenBleConnection(
			blueFalcon = blueFalcon,
			peripheral = peripheral,
			deviceIdentifier = device.identifier,
		)
	}

	companion object {
		private const val TAG = "MeshCoreBLE"
	}
}
