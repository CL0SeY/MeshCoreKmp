package com.darkrockstudios.libs.meshcore.ble

import android.annotation.SuppressLint
import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCallback
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothGattDescriptor
import android.bluetooth.BluetoothProfile
import android.bluetooth.BluetoothStatusCodes
import android.content.Context
import android.os.Build
import io.github.aakira.napier.Napier
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import java.util.UUID
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Android BLE connection that retains the [BluetoothGatt] from `connectGatt`.
 *
 * BlueFalcon 2.5.x drops that handle and only tracks GATTs after
 * STATE_CONNECTED, so a ghosted/timed-out connect leaves the peripheral
 * locked until the adapter is reset (e.g. airplane mode). This class always
 * closes the GATT it owns — including on connect timeout and cancellation.
 */
@SuppressLint("MissingPermission")
internal class AndroidGattBleConnection private constructor(
	private val context: Context,
	private val device: BluetoothDevice,
	override val deviceIdentifier: String,
) : BleConnection {

	companion object {
		private const val TAG = "MeshCoreBLE"
		private const val CONNECT_TIMEOUT_MS = 15_000L
		private const val REQUEST_MTU_TIMEOUT_MS = 5_000L
		private const val DISCONNECT_CLOSE_TIMEOUT_MS = 2_000L
		private val CCCD_UUID: UUID = UUID.fromString("00002902-0000-1000-8000-00805f9b34fb")
		private val SERVICE_UUID: UUID = UUID.fromString(BleConstants.SERVICE_UUID)
		private val RX_UUID: UUID = UUID.fromString(BleConstants.RX_CHARACTERISTIC_UUID)
		private val TX_UUID: UUID = UUID.fromString(BleConstants.TX_CHARACTERISTIC_UUID)

		suspend fun open(
			context: Context,
			device: BluetoothDevice,
			deviceIdentifier: String,
		): AndroidGattBleConnection {
			val connection =
				AndroidGattBleConnection(
					context = context.applicationContext,
					device = device,
					deviceIdentifier = deviceIdentifier,
				)
			connection.connectAndSetup()
			return connection
		}
	}

	private val _connectionState = MutableStateFlow<ConnectionState>(ConnectionState.Disconnected)
	override val connectionState: StateFlow<ConnectionState> = _connectionState.asStateFlow()

	private val _incomingData = Channel<ByteArray>(Channel.UNLIMITED)
	override val incomingData: Flow<ByteArray> = _incomingData.receiveAsFlow()

	private val closeMutex = Mutex()
	private var gatt: BluetoothGatt? = null
	private var rxCharacteristic: BluetoothGattCharacteristic? = null
	private var txCharacteristic: BluetoothGattCharacteristic? = null

	private var connectContinuation: CancellableContinuation<Unit>? = null
	private var writeContinuation: CancellableContinuation<Unit>? = null
	private var mtuContinuation: CancellableContinuation<Int>? = null
	private var disconnectContinuation: CancellableContinuation<Unit>? = null

	private enum class SetupPhase {
		IDLE,
		WAITING_CONNECT,
		WAITING_SERVICES,
		WAITING_NOTIFY_ENABLED,
		COMPLETE,
		FAILED,
	}

	private var setupPhase = SetupPhase.IDLE

	private val callback =
		object : BluetoothGattCallback() {
			override fun onConnectionStateChange(
				gatt: BluetoothGatt,
				status: Int,
				newState: Int,
			) {
				Napier.d(tag = TAG) {
					"onConnectionStateChange(): $deviceIdentifier status=$status newState=$newState"
				}
				when (newState) {
					BluetoothProfile.STATE_CONNECTED -> {
						if (setupPhase != SetupPhase.WAITING_CONNECT) return
						setupPhase = SetupPhase.WAITING_SERVICES
						Napier.d(tag = TAG) {
							"onConnectionStateChange(): $deviceIdentifier connected → discoverServices"
						}
						if (!gatt.discoverServices()) {
							failSetup(MeshCoreBleException("Service discovery could not start"))
						}
					}

					BluetoothProfile.STATE_DISCONNECTED -> {
						_connectionState.value = ConnectionState.Disconnected
						val pendingDisconnect = disconnectContinuation
						if (pendingDisconnect != null) {
							disconnectContinuation = null
							pendingDisconnect.resume(Unit)
							return
						}
						if (setupPhase == SetupPhase.WAITING_CONNECT ||
							setupPhase == SetupPhase.WAITING_SERVICES ||
							setupPhase == SetupPhase.WAITING_NOTIFY_ENABLED
						) {
							failSetup(
								MeshCoreBleException(
									"Disconnected during setup (status=$status)",
								),
							)
						}
					}
				}
			}

			override fun onServicesDiscovered(
				gatt: BluetoothGatt,
				status: Int,
			) {
				if (setupPhase != SetupPhase.WAITING_SERVICES) return
				if (status != BluetoothGatt.GATT_SUCCESS) {
					failSetup(MeshCoreBleException("Service discovery failed with status=$status"))
					return
				}
				val service =
					gatt.getService(SERVICE_UUID)
						?: run {
							failSetup(MeshCoreBleException("NUS service not found"))
							return
						}
				val rx =
					service.getCharacteristic(RX_UUID)
						?: run {
							failSetup(MeshCoreBleException("NUS RX characteristic not found"))
							return
						}
				val tx =
					service.getCharacteristic(TX_UUID)
						?: run {
							failSetup(MeshCoreBleException("NUS TX characteristic not found"))
							return
						}
				rxCharacteristic = rx
				txCharacteristic = tx
				setupPhase = SetupPhase.WAITING_NOTIFY_ENABLED
				Napier.d(tag = TAG) {
					"onServicesDiscovered(): $deviceIdentifier NUS found → enable TX notify"
				}
				if (!enableNotifications(gatt, tx)) {
					failSetup(MeshCoreBleException("Failed to enable TX notifications"))
				}
			}

			override fun onDescriptorWrite(
				gatt: BluetoothGatt,
				descriptor: BluetoothGattDescriptor,
				status: Int,
			) {
				if (setupPhase != SetupPhase.WAITING_NOTIFY_ENABLED) return
				if (descriptor.uuid != CCCD_UUID) return
				if (descriptor.characteristic?.uuid != TX_UUID) return
				if (status != BluetoothGatt.GATT_SUCCESS) {
					failSetup(MeshCoreBleException("CCCD write failed with status=$status"))
					return
				}
				setupPhase = SetupPhase.COMPLETE
				_connectionState.value = ConnectionState.Connected
				Napier.d(tag = TAG) {
					"onDescriptorWrite(): $deviceIdentifier TX notify enabled → Connected"
				}
				connectContinuation?.let {
					connectContinuation = null
					it.resume(Unit)
				}
			}

			@Deprecated("Deprecated in Java")
			override fun onCharacteristicChanged(
				gatt: BluetoothGatt,
				characteristic: BluetoothGattCharacteristic,
			) {
				if (characteristic.uuid != TX_UUID) return
				@Suppress("DEPRECATION")
				val value = characteristic.value ?: return
				_incomingData.trySend(value)
			}

			override fun onCharacteristicChanged(
				gatt: BluetoothGatt,
				characteristic: BluetoothGattCharacteristic,
				value: ByteArray,
			) {
				if (characteristic.uuid != TX_UUID) return
				_incomingData.trySend(value)
			}

			@Deprecated("Deprecated in Java")
			override fun onCharacteristicWrite(
				gatt: BluetoothGatt,
				characteristic: BluetoothGattCharacteristic,
				status: Int,
			) {
				if (characteristic.uuid != RX_UUID) return
				completeWrite(status)
			}

			override fun onMtuChanged(
				gatt: BluetoothGatt,
				mtu: Int,
				status: Int,
			) {
				mtuContinuation?.let {
					mtuContinuation = null
					Napier.d(tag = TAG) { "onMtuChanged(): status=$status mtu=$mtu" }
					if (status == BluetoothGatt.GATT_SUCCESS) {
						it.resume(mtu)
					} else {
						it.resumeWithException(
							MeshCoreBleException("MTU request failed with status: $status"),
						)
					}
				}
			}
		}

	private fun completeWrite(status: Int) {
		writeContinuation?.let {
			writeContinuation = null
			if (status == BluetoothGatt.GATT_SUCCESS) {
				it.resume(Unit)
			} else {
				it.resumeWithException(MeshCoreBleException("Write failed with status=$status"))
			}
		}
	}

	private fun failSetup(error: MeshCoreBleException) {
		if (setupPhase == SetupPhase.FAILED || setupPhase == SetupPhase.COMPLETE) return
		setupPhase = SetupPhase.FAILED
		_connectionState.value = ConnectionState.Error(error.message ?: "BLE setup failed")
		Napier.w(tag = TAG) { "failSetup(): $deviceIdentifier ${error.message}" }
		connectContinuation?.let {
			connectContinuation = null
			it.resumeWithException(error)
		}
	}

	private fun enableNotifications(
		gatt: BluetoothGatt,
		tx: BluetoothGattCharacteristic,
	): Boolean {
		if (!gatt.setCharacteristicNotification(tx, true)) return false
		val cccd =
			tx.getDescriptor(CCCD_UUID)
				?: return false
		return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
			gatt.writeDescriptor(
				cccd,
				BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE,
			) == BluetoothStatusCodes.SUCCESS
		} else {
			@Suppress("DEPRECATION")
			cccd.value = BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
			@Suppress("DEPRECATION")
			gatt.writeDescriptor(cccd)
		}
	}

	private suspend fun connectAndSetup() {
		_connectionState.value = ConnectionState.Connecting
		Napier.d(tag = TAG) {
			"connectAndSetup(): $deviceIdentifier connecting (timeout=${CONNECT_TIMEOUT_MS}ms)"
		}
		try {
			val connected =
				withTimeoutOrNull(CONNECT_TIMEOUT_MS) {
					suspendCancellableCoroutine { cont ->
						connectContinuation = cont
						setupPhase = SetupPhase.WAITING_CONNECT
						val opened =
							device.connectGatt(
								context,
								false,
								callback,
								BluetoothDevice.TRANSPORT_LE,
							)
						if (opened == null) {
							setupPhase = SetupPhase.FAILED
							connectContinuation = null
							cont.resumeWithException(
								MeshCoreBleException("connectGatt returned null"),
							)
							return@suspendCancellableCoroutine
						}
						gatt = opened
						cont.invokeOnCancellation {
							connectContinuation = null
							setupPhase = SetupPhase.IDLE
							// Cancellation must still release the GATT we own.
							// closeOwnedGatt is suspend; kick it from a blocking
							// path via runBlocking is unsafe here, so disconnect
							// + close the handle directly.
							forceCloseGatt(opened)
							gatt = null
						}
					}
				}
			if (connected == null) {
				Napier.w(tag = TAG) {
					"connectAndSetup(): timed out after ${CONNECT_TIMEOUT_MS}ms — releasing GATT"
				}
				throw MeshCoreBleException("Connect timed out after ${CONNECT_TIMEOUT_MS}ms")
			}
		} catch (error: Exception) {
			withContext(NonCancellable) { closeOwnedGatt() }
			_connectionState.value = ConnectionState.Disconnected
			Napier.e(tag = TAG) { "connectAndSetup(): failed: ${error.message}" }
			throw error
		}
	}

	override suspend fun write(data: ByteArray) {
		val activeGatt =
			gatt ?: throw MeshCoreBleException("Not connected - GATT unavailable")
		val rx =
			rxCharacteristic
				?: throw MeshCoreBleException("Not connected - RX characteristic unavailable")
		suspendCancellableCoroutine { cont ->
			writeContinuation = cont
			val started =
				if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
					activeGatt.writeCharacteristic(
						rx,
						data,
						BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT,
					) == BluetoothStatusCodes.SUCCESS
				} else {
					@Suppress("DEPRECATION")
					rx.value = data
					rx.writeType = BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT
					@Suppress("DEPRECATION")
					activeGatt.writeCharacteristic(rx)
				}
			if (!started) {
				writeContinuation = null
				cont.resumeWithException(MeshCoreBleException("Write could not start"))
				return@suspendCancellableCoroutine
			}
			cont.invokeOnCancellation { writeContinuation = null }
		}
	}

	override suspend fun requestMtu(mtu: Int): Int {
		val activeGatt =
			gatt ?: throw MeshCoreBleException("Not connected - GATT unavailable")
		Napier.d(tag = TAG) {
			"requestMtu(): $deviceIdentifier requesting $mtu (timeout=${REQUEST_MTU_TIMEOUT_MS}ms)"
		}
		val negotiated =
			withTimeoutOrNull(REQUEST_MTU_TIMEOUT_MS) {
				suspendCancellableCoroutine { cont ->
					mtuContinuation = cont
					if (!activeGatt.requestMtu(mtu)) {
						mtuContinuation = null
						cont.resumeWithException(
							MeshCoreBleException("MTU request could not start"),
						)
						return@suspendCancellableCoroutine
					}
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
		val activeGatt = gatt ?: return false
		val native =
			when (priority) {
				ConnectionPriority.LOW_POWER -> BluetoothGatt.CONNECTION_PRIORITY_LOW_POWER
				ConnectionPriority.BALANCED -> BluetoothGatt.CONNECTION_PRIORITY_BALANCED
				ConnectionPriority.HIGH -> BluetoothGatt.CONNECTION_PRIORITY_HIGH
			}
		return try {
			activeGatt.requestConnectionPriority(native)
		} catch (_: Exception) {
			false
		}
	}

	override suspend fun disconnect() {
		withContext(NonCancellable) {
			closeOwnedGatt()
			rxCharacteristic = null
			txCharacteristic = null
			_connectionState.value = ConnectionState.Disconnected
			_incomingData.close()
		}
	}

	private suspend fun closeOwnedGatt() =
		closeMutex.withLock {
			val active = gatt ?: return@withLock
			gatt = null
			Napier.d(tag = TAG) { "closeOwnedGatt(): $deviceIdentifier disconnect+close" }
			val disconnected =
				withTimeoutOrNull(DISCONNECT_CLOSE_TIMEOUT_MS) {
					suspendCancellableCoroutine { cont ->
						disconnectContinuation = cont
						active.disconnect()
						cont.invokeOnCancellation {
							if (disconnectContinuation === cont) {
								disconnectContinuation = null
							}
						}
					}
				}
			if (disconnected == null) {
				Napier.w(tag = TAG) {
					"closeOwnedGatt(): $deviceIdentifier disconnect callback timed out — closing anyway"
				}
				disconnectContinuation = null
			}
			forceCloseGatt(active)
		}

	private fun forceCloseGatt(active: BluetoothGatt) {
		runCatching { active.disconnect() }
		runCatching { active.close() }
	}
}
