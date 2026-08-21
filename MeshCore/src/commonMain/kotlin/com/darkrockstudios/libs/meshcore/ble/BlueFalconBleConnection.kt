package com.darkrockstudios.libs.meshcore.ble

import dev.bluefalcon.core.BlueFalcon
import dev.bluefalcon.core.BluetoothCharacteristic
import dev.bluefalcon.core.BluetoothPeripheral
import dev.bluefalcon.core.BluetoothPeripheralState
import dev.bluefalcon.core.CharacteristicWriteResult
import dev.bluefalcon.core.CharacteristicWriteType
import dev.bluefalcon.core.NotificationSubscriptionResult
import dev.bluefalcon.core.ConnectionPriority as BlueFalconConnectionPriority
import io.github.aakira.napier.Napier
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.uuid.ExperimentalUuidApi

@OptIn(ExperimentalUuidApi::class)
class BlueFalconBleConnection internal constructor(
	private val blueFalcon: BlueFalcon,
	private val peripheral: BluetoothPeripheral,
	override val deviceIdentifier: String,
) : BleConnection {

	companion object {
		/** Bounds the MTU exchange so a ghosted BLE stack fails the connect
		 *  attempt instead of hanging DeviceScanner.connect() forever. */
		private const val REQUEST_MTU_TIMEOUT_MS = 5_000L
		private const val WRITE_BACKPRESSURE_RETRY_MS = 20L
		private const val WRITE_BACKPRESSURE_TIMEOUT_MS = 5_000L
		private const val TAG = "MeshCoreBLE"
	}

	private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
	private var notificationJob: Job? = null
	private var disconnectWatchJob: Job? = null

	private val _connectionState = MutableStateFlow<ConnectionState>(ConnectionState.Disconnected)
	override val connectionState: StateFlow<ConnectionState> = _connectionState.asStateFlow()

	private val _incomingData = Channel<ByteArray>(Channel.UNLIMITED)
	override val incomingData: Flow<ByteArray> = _incomingData.receiveAsFlow()

	private var rxCharacteristic: BluetoothCharacteristic? = null
	private var txCharacteristic: BluetoothCharacteristic? = null

	internal suspend fun connectAndSetup(connectTimeoutMs: Long = 15_000L) {
		_connectionState.value = ConnectionState.Connecting
		Napier.d(tag = TAG) {
			"connectAndSetup(): $deviceIdentifier connecting (timeout=${connectTimeoutMs}ms)"
		}

		try {
			// withTimeoutOrNull (not withTimeout): TimeoutCancellationException is
			// a CancellationException, and monitorLoop rethrows CancellationException
			// — letting it propagate would kill the whole monitor loop instead of
			// entering the reconnect backoff.
			val connected = withTimeoutOrNull(connectTimeoutMs) {
				coroutineScope {
					val connectedSignal = async {
						blueFalcon.connectionStateUpdates.first {
							it.peripheral.uuid == peripheral.uuid &&
								it.state == BluetoothPeripheralState.Connected
						}
					}

					blueFalcon.connect(peripheral, autoConnect = false)
					connectedSignal.await()
					Napier.d(tag = TAG) {
						"connectAndSetup(): $deviceIdentifier connected → waiting for NUS"
					}

					// Discovery SharedFlow has no replay. Poll the peripheral after
					// connect (autoDiscover may already have finished) and request
					// an explicit discover if services are still empty.
					var nus = findNusCharacteristics(peripheral)
					if (nus == null && peripheral.services.isEmpty()) {
						blueFalcon.discoverServices(peripheral)
					}
					while (nus == null) {
						delay(50)
						nus = findNusCharacteristics(peripheral)
					}
					val (rx, tx) = nus
					rxCharacteristic = rx
					txCharacteristic = tx
					Napier.d(tag = TAG) {
						"connectAndSetup(): NUS rx/tx found → enabling TX notify"
					}

					when (
						val subscription =
							blueFalcon.setNotificationSubscription(peripheral, tx, enabled = true)
					) {
						is NotificationSubscriptionResult.Updated -> {
							if (!subscription.enabled) {
								throw MeshCoreBleException("TX notifications were not enabled")
							}
						}
						is NotificationSubscriptionResult.Disconnected ->
							throw MeshCoreBleException("Disconnected while enabling TX notify")
						is NotificationSubscriptionResult.Unsupported ->
							throw MeshCoreBleException("TX notifications unsupported")
						is NotificationSubscriptionResult.Failed ->
							throw MeshCoreBleException(
								"Failed to enable TX notify: ${subscription.cause?.message}",
							)
					}

					startNotificationCollector(tx)
					startDisconnectWatcher()
					_connectionState.value = ConnectionState.Connected
					Napier.d(tag = TAG) {
						"connectAndSetup(): $deviceIdentifier TX notify enabled → Connected"
					}
				}
			}
			if (connected == null) {
				Napier.w(tag = TAG) {
					"connectAndSetup(): timed out after ${connectTimeoutMs}ms — ghosted BLE stack?"
				}
				throw MeshCoreBleException("Connect timed out after ${connectTimeoutMs}ms")
			}
		} catch (e: Exception) {
			// BlueFalcon 3.5.1+ tracks the connectGatt handle before
			// STATE_CONNECTED, so disconnect() can release a timed-out
			// direct connect that never established.
			runCatching { blueFalcon.disconnect(peripheral) }
			stopJobs()
			_connectionState.value = ConnectionState.Disconnected
			Napier.e(tag = TAG) { "connectAndSetup(): failed: ${e.message}" }
			throw e
		}
	}

	override suspend fun write(data: ByteArray) {
		val rx = rxCharacteristic
			?: throw MeshCoreBleException("Not connected - RX characteristic unavailable")

		val started = withTimeoutOrNull(WRITE_BACKPRESSURE_TIMEOUT_MS) {
			while (true) {
				when (
					val result =
						blueFalcon.writeCharacteristic(
							peripheral,
							rx,
							data,
							CharacteristicWriteType.WithResponse,
						)
				) {
					CharacteristicWriteResult.Sent -> return@withTimeoutOrNull Unit
					CharacteristicWriteResult.Backpressured -> delay(WRITE_BACKPRESSURE_RETRY_MS)
					CharacteristicWriteResult.Disconnected ->
						throw MeshCoreBleException("Write failed: disconnected")
					CharacteristicWriteResult.Unsupported ->
						throw MeshCoreBleException("Write failed: unsupported")
					is CharacteristicWriteResult.PayloadTooLarge ->
						throw MeshCoreBleException(
							"Write failed: payload too large (max=${result.maximumLength})",
						)
					is CharacteristicWriteResult.Failed ->
						throw MeshCoreBleException(
							"Write failed: ${result.cause?.message ?: "unknown"}",
						)
				}
			}
		}
		if (started == null) {
			throw MeshCoreBleException("Write timed out after backpressure")
		}
	}

	override suspend fun requestMtu(mtu: Int): Int {
		Napier.d(tag = TAG) {
			"requestMtu(): $deviceIdentifier requesting $mtu (timeout=${REQUEST_MTU_TIMEOUT_MS}ms)"
		}
		val before = peripheral.mtuSize
		blueFalcon.changeMTU(peripheral, mtu)
		val negotiated = withTimeoutOrNull(REQUEST_MTU_TIMEOUT_MS) {
			while (true) {
				val current = peripheral.mtuSize
				if (current != null && current != before) {
					return@withTimeoutOrNull current
				}
				delay(50)
			}
			@Suppress("UNREACHABLE_CODE")
			error("unreachable")
		}
		if (negotiated == null) {
			Napier.w(tag = TAG) { "requestMtu(): timed out after ${REQUEST_MTU_TIMEOUT_MS}ms" }
			throw MeshCoreBleException("MTU request timed out after ${REQUEST_MTU_TIMEOUT_MS}ms")
		}
		Napier.d(tag = TAG) { "requestMtu(): negotiated $negotiated" }
		return negotiated
	}

	override suspend fun requestConnectionPriority(priority: ConnectionPriority): Boolean {
		return try {
			val blueFalconPriority = when (priority) {
				ConnectionPriority.LOW_POWER -> BlueFalconConnectionPriority.Low
				ConnectionPriority.BALANCED -> BlueFalconConnectionPriority.Balanced
				ConnectionPriority.HIGH -> BlueFalconConnectionPriority.High
			}
			// BlueFalcon maps this to BluetoothGatt.requestConnectionPriority(...);
			// it is fire-and-forget and reports nothing back, so any non-exception
			// outcome is treated as accepted. The node decides what it grants.
			blueFalcon.requestConnectionPriority(peripheral, blueFalconPriority)
			true
		} catch (_: Exception) {
			// Non-fatal: a rejected priority request must never fail the connect path.
			false
		}
	}

	override suspend fun disconnect() {
		stopJobs()
		blueFalcon.disconnect(peripheral)
		rxCharacteristic = null
		txCharacteristic = null
		_connectionState.value = ConnectionState.Disconnected
		_incomingData.close()
		scope.cancel()
	}

	private fun startNotificationCollector(tx: BluetoothCharacteristic) {
		notificationJob?.cancel()
		notificationJob = scope.launch {
			blueFalcon.engine.characteristicNotifications
				.filter {
					it.peripheral.uuid == peripheral.uuid &&
						it.characteristic.uuid == tx.uuid
				}
				.collect { notification ->
					_incomingData.trySend(notification.value)
				}
		}
	}

	private fun startDisconnectWatcher() {
		disconnectWatchJob?.cancel()
		disconnectWatchJob = scope.launch {
			blueFalcon.connectionStateUpdates.first {
				it.peripheral.uuid == peripheral.uuid &&
					it.state == BluetoothPeripheralState.Disconnected
			}
			Napier.d(tag = TAG) { "didDisconnect(): $deviceIdentifier" }
			_connectionState.value = ConnectionState.Disconnected
		}
	}

	private fun stopJobs() {
		notificationJob?.cancel()
		notificationJob = null
		disconnectWatchJob?.cancel()
		disconnectWatchJob = null
	}

	private fun findNusCharacteristics(
		target: BluetoothPeripheral,
	): Pair<BluetoothCharacteristic, BluetoothCharacteristic>? {
		val nusServiceUuid = BleConstants.SERVICE_UUID.lowercase()
		val rxUuid = BleConstants.RX_CHARACTERISTIC_UUID.lowercase()
		val txUuid = BleConstants.TX_CHARACTERISTIC_UUID.lowercase()

		for (service in target.services) {
			if (service.uuid.toString().lowercase() != nusServiceUuid) continue
			var foundRx: BluetoothCharacteristic? = null
			var foundTx: BluetoothCharacteristic? = null
			for (characteristic in service.characteristics) {
				val charUuid = characteristic.uuid.toString().lowercase()
				if (charUuid == rxUuid) foundRx = characteristic
				if (charUuid == txUuid) foundTx = characteristic
			}
			if (foundRx != null && foundTx != null) {
				return foundRx to foundTx
			}
		}
		return null
	}
}
