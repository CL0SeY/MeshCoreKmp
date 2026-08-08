package com.darkrockstudios.libs.meshcore.ble

import dev.bluefalcon.BlueFalcon
import dev.bluefalcon.BlueFalconDelegate
import dev.bluefalcon.BluetoothCharacteristic
import dev.bluefalcon.BluetoothPeripheral
import dev.bluefalcon.ConnectionPriority as BlueFalconConnectionPriority
import io.github.aakira.napier.Napier
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
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
		private const val TAG = "MeshCoreBLE"
	}

	private val _connectionState = MutableStateFlow<ConnectionState>(ConnectionState.Disconnected)
	override val connectionState: StateFlow<ConnectionState> = _connectionState.asStateFlow()

	private val _incomingData = Channel<ByteArray>(Channel.UNLIMITED)
	override val incomingData: Flow<ByteArray> = _incomingData.receiveAsFlow()

	private var rxCharacteristic: BluetoothCharacteristic? = null
	private var txCharacteristic: BluetoothCharacteristic? = null

	private var connectContinuation: CancellableContinuation<Unit>? = null
	private var writeContinuation: CancellableContinuation<Unit>? = null
	private var mtuContinuation: CancellableContinuation<Int>? = null

	private enum class SetupPhase {
		IDLE,
		WAITING_CONNECT,
		WAITING_CHARACTERISTICS,
		WAITING_NOTIFY_ENABLED,
		COMPLETE,
		FAILED,
	}

	private var setupPhase = SetupPhase.IDLE

	private val delegate = object : BlueFalconDelegate {
		override fun didConnect(bluetoothPeripheral: BluetoothPeripheral) {
			if (bluetoothPeripheral.uuid != peripheral.uuid) return
			// With autoDiscoverAllServicesAndCharacteristics = true,
			// Blue Falcon will trigger service + characteristic discovery automatically.
			Napier.d(tag = TAG) { "didConnect(): $deviceIdentifier → WAITING_CHARACTERISTICS" }
			setupPhase = SetupPhase.WAITING_CHARACTERISTICS
		}

		override fun didDisconnect(bluetoothPeripheral: BluetoothPeripheral) {
			if (bluetoothPeripheral.uuid != peripheral.uuid) return
			Napier.d(tag = TAG) { "didDisconnect(): $deviceIdentifier" }
			_connectionState.value = ConnectionState.Disconnected

			connectContinuation?.let {
				connectContinuation = null
				it.resumeWithException(MeshCoreBleException("Disconnected during setup"))
			}
		}

		override fun didDiscoverCharacteristics(bluetoothPeripheral: BluetoothPeripheral) {
			if (bluetoothPeripheral.uuid != peripheral.uuid) return
			if (setupPhase != SetupPhase.WAITING_CHARACTERISTICS) return

			val nusServiceUuid = BleConstants.SERVICE_UUID.lowercase()
			val rxUuid = BleConstants.RX_CHARACTERISTIC_UUID.lowercase()
			val txUuid = BleConstants.TX_CHARACTERISTIC_UUID.lowercase()

			var foundRx: BluetoothCharacteristic? = null
			var foundTx: BluetoothCharacteristic? = null

			for (service in bluetoothPeripheral.services.values) {
				if (service.uuid.toString().lowercase() == nusServiceUuid) {
					for (characteristic in service.characteristics) {
						val charUuid = characteristic.uuid.toString().lowercase()
						if (charUuid == rxUuid) foundRx = characteristic
						if (charUuid == txUuid) foundTx = characteristic
					}
					break
				}
			}

			if (foundRx == null || foundTx == null) {
				val error = "NUS service or characteristics not found"
				Napier.w(tag = TAG) { "didDiscoverCharacteristics(): $error (rx=$foundRx tx=$foundTx)" }
				_connectionState.value = ConnectionState.Error(error)
				setupPhase = SetupPhase.FAILED
				connectContinuation?.let {
					connectContinuation = null
					it.resumeWithException(MeshCoreBleException(error))
				}
				return
			}

			rxCharacteristic = foundRx
			txCharacteristic = foundTx
			Napier.d(tag = TAG) { "didDiscoverCharacteristics(): NUS rx/tx found → WAITING_NOTIFY_ENABLED" }

			setupPhase = SetupPhase.WAITING_NOTIFY_ENABLED
			blueFalcon.notifyCharacteristic(bluetoothPeripheral, foundTx, true)
		}

		override fun didUpdateNotificationStateFor(
			bluetoothPeripheral: BluetoothPeripheral,
			bluetoothCharacteristic: BluetoothCharacteristic,
		) {
			if (bluetoothPeripheral.uuid != peripheral.uuid) return
			if (setupPhase != SetupPhase.WAITING_NOTIFY_ENABLED) return
			// Only the TX characteristic's notification state completes setup;
			// a stale callback for any other characteristic must not.
			val txUuid = BleConstants.TX_CHARACTERISTIC_UUID.lowercase()
			if (bluetoothCharacteristic.uuid.toString().lowercase() != txUuid) return

			setupPhase = SetupPhase.COMPLETE
			_connectionState.value = ConnectionState.Connected
			Napier.d(tag = TAG) { "didUpdateNotificationStateFor(): TX notify enabled → Connected" }
			connectContinuation?.let {
				connectContinuation = null
				it.resume(Unit)
			}
		}

		// Note: typo in Blue Falcon's API — "didCharacteristcValueChanged" (missing 'i')
		override fun didCharacteristcValueChanged(
			bluetoothPeripheral: BluetoothPeripheral,
			bluetoothCharacteristic: BluetoothCharacteristic,
		) {
			if (bluetoothPeripheral.uuid != peripheral.uuid) return
			val txUuid = BleConstants.TX_CHARACTERISTIC_UUID.lowercase()
			if (bluetoothCharacteristic.uuid.toString().lowercase() == txUuid) {
				val value = bluetoothCharacteristic.value
				if (value != null) {
					_incomingData.trySend(value)
				}
			}
		}

		override fun didWriteCharacteristic(
			bluetoothPeripheral: BluetoothPeripheral,
			bluetoothCharacteristic: BluetoothCharacteristic,
			success: Boolean,
		) {
			if (bluetoothPeripheral.uuid != peripheral.uuid) return
			// Only writes to the RX characteristic resolve the pending write;
			// a stale callback for any other characteristic must not.
			val rxUuid = BleConstants.RX_CHARACTERISTIC_UUID.lowercase()
			if (bluetoothCharacteristic.uuid.toString().lowercase() != rxUuid) return
			writeContinuation?.let {
				writeContinuation = null
				if (success) {
					it.resume(Unit)
				} else {
					it.resumeWithException(MeshCoreBleException("Write failed"))
				}
			}
		}

		override fun didUpdateMTU(
			bluetoothPeripheral: BluetoothPeripheral,
			status: Int,
		) {
			if (bluetoothPeripheral.uuid != peripheral.uuid) return
			mtuContinuation?.let {
				mtuContinuation = null
				val newMtu = bluetoothPeripheral.mtuSize ?: 23
				Napier.d(tag = TAG) { "didUpdateMTU(): status=$status mtu=$newMtu" }
				if (status == 0) { // GATT_SUCCESS
					it.resume(newMtu)
				} else {
					it.resumeWithException(
						MeshCoreBleException("MTU request failed with status: $status")
					)
				}
			}
		}
	}

	internal suspend fun connectAndSetup(connectTimeoutMs: Long = 15_000L) {
		_connectionState.value = ConnectionState.Connecting
		blueFalcon.delegates.add(delegate)
		Napier.d(tag = TAG) { "connectAndSetup(): $deviceIdentifier connecting (timeout=${connectTimeoutMs}ms)" }

		try {
			// A ghosted BLE stack after deep-sleep wake can swallow the connect
			// callback entirely (didConnect never fires). Bound the wait so the
			// reconnect backoff in monitorLoop can run instead of hanging.
			// withTimeoutOrNull (not withTimeout): TimeoutCancellationException is
			// a CancellationException, and monitorLoop rethrows CancellationException
			// — letting it propagate would kill the whole monitor loop instead of
			// entering the reconnect backoff.
			val connected = withTimeoutOrNull(connectTimeoutMs) {
				suspendCancellableCoroutine<Unit> { cont ->
					connectContinuation = cont
					setupPhase = SetupPhase.WAITING_CONNECT
					blueFalcon.connect(peripheral, autoConnect = false)

					cont.invokeOnCancellation {
						connectContinuation = null
						setupPhase = SetupPhase.IDLE
						blueFalcon.disconnect(peripheral)
						blueFalcon.delegates.remove(delegate)
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
			blueFalcon.delegates.remove(delegate)
			_connectionState.value = ConnectionState.Disconnected
			Napier.e(tag = TAG) { "connectAndSetup(): failed: ${e.message}" }
			throw e
		}
	}

	override suspend fun write(data: ByteArray) {
		val rx = rxCharacteristic
			?: throw MeshCoreBleException("Not connected - RX characteristic unavailable")

		suspendCancellableCoroutine<Unit> { cont ->
			writeContinuation = cont
			blueFalcon.writeCharacteristicWithoutEncoding(peripheral, rx, data, writeType = null)
			cont.invokeOnCancellation { writeContinuation = null }
		}
	}

	override suspend fun requestMtu(mtu: Int): Int {
		// Same ghosted-stack class of failure as the connect: didUpdateMTU may
		// never fire. Bound the wait so a dead BLE stack fails the connect
		// attempt instead of hanging DeviceScanner.connect() forever.
		Napier.d(tag = TAG) {
			"requestMtu(): $deviceIdentifier requesting $mtu (timeout=${REQUEST_MTU_TIMEOUT_MS}ms)"
		}
		val negotiated = withTimeoutOrNull(REQUEST_MTU_TIMEOUT_MS) {
			suspendCancellableCoroutine { cont ->
				mtuContinuation = cont
				blueFalcon.changeMTU(peripheral, mtu)
				cont.invokeOnCancellation { mtuContinuation = null }
			}
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
		} catch (e: Exception) {
			// Non-fatal: a rejected priority request must never fail the connect path.
			false
		}
	}

	override suspend fun disconnect() {
		blueFalcon.disconnect(peripheral)
		blueFalcon.delegates.remove(delegate)
		rxCharacteristic = null
		txCharacteristic = null
		_connectionState.value = ConnectionState.Disconnected
		_incomingData.close()
	}
}
