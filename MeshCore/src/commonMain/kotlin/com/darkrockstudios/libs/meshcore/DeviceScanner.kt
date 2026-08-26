package com.darkrockstudios.libs.meshcore

import com.darkrockstudios.libs.meshcore.ble.BleAdapter
import com.darkrockstudios.libs.meshcore.ble.DiscoveredDevice
import com.darkrockstudios.libs.meshcore.ble.ScanFilter
import com.darkrockstudios.libs.meshcore.protocol.CommandQueue
import io.github.aakira.napier.Napier
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

class DeviceScanner(
	private val bleAdapter: BleAdapter,
) {
	private val _discoveredDevices = MutableStateFlow<List<DiscoveredDevice>>(emptyList())
	val discoveredDevices: StateFlow<List<DiscoveredDevice>> = _discoveredDevices.asStateFlow()

	val isBluetoothEnabled: Boolean get() = bleAdapter.isBluetoothEnabled

	private var scanJob: Job? = null

	fun startScan(filter: ScanFilter = ScanFilter(), scope: CoroutineScope) {
		stopScan()
		_discoveredDevices.value = emptyList()
		Napier.d(tag = TAG) { "startScan() called" }
		val scanFlow = bleAdapter.scan(filter)
		scanJob = scope.launch {
			Napier.d(tag = TAG) { "Collecting scan flow" }
			scanFlow.collect { device ->
				Napier.d(tag = TAG) { "Received device '${device.name}' (${device.identifier})" }
				val current = _discoveredDevices.value
				val existingIndex = current.indexOfFirst { it.identifier == device.identifier }
				_discoveredDevices.value = if (existingIndex >= 0) {
					current.toMutableList().apply { set(existingIndex, device) }
				} else {
					current + device
				}
				Napier.d(tag = TAG) { "Total devices = ${_discoveredDevices.value.size}" }
			}
		}
	}

	fun stopScan() {
		scanJob?.cancel()
		scanJob = null
		bleAdapter.stopScan()
	}

	suspend fun connect(
		device: DiscoveredDevice,
		scope: CoroutineScope,
		config: ConnectionConfig = ConnectionConfig(),
	): DeviceConnection {
		stopScan()
		Napier.d(tag = TAG) {
			"connect(): ${device.identifier} mtu=${config.requestedMtu} commandTimeout=${config.commandTimeout}"
		}

		val bleConnection = bleAdapter.connect(device)
		var deviceConnection: DeviceConnection? = null
		var handedOff = false
		try {
			bleConnection.requestMtu(config.requestedMtu)
			config.connectionPriority?.let { bleConnection.requestConnectionPriority(it) }

			val commandQueue = CommandQueue(
				connection = bleConnection,
				scope = scope,
				defaultTimeout = config.commandTimeout,
			)

			val established = DeviceConnection(
				bleConnection = bleConnection,
				commandQueue = commandQueue,
				scope = scope,
				config = config,
			)
			deviceConnection = established
			established.initialize()
			handedOff = true
			return established
		} finally {
			if (!handedOff) {
				// connect() already owns a GATT. If MTU, handshake, or
				// cancellation fails here the caller never gets a
				// DeviceConnection to disconnect — leftover clients wedge
				// Wear until airplane mode.
				withContext(NonCancellable) {
					val wrapped = deviceConnection
					if (wrapped != null) {
						runCatching { wrapped.disconnect() }
					} else {
						runCatching { bleConnection.disconnect() }
					}
				}
			}
		}
	}

	companion object {
		private const val TAG = "MeshCoreBLE"
	}
}
