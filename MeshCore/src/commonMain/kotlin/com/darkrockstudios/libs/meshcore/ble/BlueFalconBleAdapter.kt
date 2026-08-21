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
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull

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
		val peripheral = resolvePeripheralForConnect(device.identifier)
		Napier.d(tag = TAG) {
			"connect(): ${device.identifier} via resolved peripheral uuid=${peripheral.uuid}"
		}

		return platformOpenBleConnection(
			blueFalcon = blueFalcon,
			peripheral = peripheral,
			deviceIdentifier = device.identifier,
		)
	}

	/**
	 * BlueFalcon 3.x emits connection/service SharedFlow updates only for
	 * peripherals present in the engine registry (populated by scan). Prefer
	 * that registry, then same-process scan cache, then a brief scan, and only
	 * then [BlueFalcon.retrievePeripheral] with an explicit registry insert —
	 * never connect a bare retrievePeripheral orphan.
	 */
	private suspend fun resolvePeripheralForConnect(identifier: String): BluetoothPeripheral {
		findInRegistry(identifier)?.let { return it }

		peripheralCache.entries.firstOrNull { it.key.equals(identifier, ignoreCase = true) }
			?.value
			?.let { cached ->
				findInRegistry(cached.uuid)?.let { return it }
				platformRegisterPeripheral(blueFalcon, cached)
				peripheralCache[cached.uuid] = cached
				return cached
			}

		Napier.d(tag = TAG) {
			"connect(): $identifier not in BlueFalcon registry — brief scan to register"
		}
		val scanned = withTimeoutOrNull(REGISTER_SCAN_TIMEOUT_MS) {
			scanUntilRegistered(identifier)
		}
		if (scanned != null) return scanned

		val retrieved = blueFalcon.retrievePeripheral(identifier)
			?: throw MeshCoreBleException(
				"Peripheral not found for identifier: $identifier",
			)
		platformRegisterPeripheral(blueFalcon, retrieved)
		peripheralCache[retrieved.uuid] = retrieved
		Napier.d(tag = TAG) {
			"connect(): $identifier registered via retrievePeripheral fallback"
		}
		return retrieved
	}

	private fun findInRegistry(identifier: String): BluetoothPeripheral? =
		blueFalcon.peripherals.value.firstOrNull {
			it.uuid.equals(identifier, ignoreCase = true)
		}

	private suspend fun scanUntilRegistered(identifier: String): BluetoothPeripheral {
		val scanJob = blueFalcon.engine.scope.launch {
			runCatching { blueFalcon.scan(emptyList()) }
		}
		try {
			return blueFalcon.peripherals
				.mapNotNull { set ->
					set.firstOrNull { it.uuid.equals(identifier, ignoreCase = true) }
				}
				.first()
				.also { peripheralCache[it.uuid] = it }
		} finally {
			scanJob.cancel()
			runCatching { blueFalcon.stopScanning() }
		}
	}

	companion object {
		private const val TAG = "MeshCoreBLE"
		private const val REGISTER_SCAN_TIMEOUT_MS = 8_000L
	}
}
