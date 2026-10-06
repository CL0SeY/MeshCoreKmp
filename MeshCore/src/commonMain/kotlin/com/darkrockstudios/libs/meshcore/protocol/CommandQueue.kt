package com.darkrockstudios.libs.meshcore.protocol

import com.darkrockstudios.libs.meshcore.MeshCoreException
import com.darkrockstudios.libs.meshcore.ble.BleConnection
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class CommandQueue(
	private val connection: BleConnection,
	scope: CoroutineScope,
	private val defaultTimeout: Duration = 5.seconds,
	private val writeTimeout: Duration = 10.seconds,
) {
	private val commandMutex = Mutex()

	private val _pushEvents = MutableSharedFlow<Response>(extraBufferCapacity = 64)
	val pushEvents: SharedFlow<Response> = _pushEvents.asSharedFlow()

	private var pendingResponseChannel: Channel<Response>? = null

	init {
		scope.launch {
			connection.incomingData.collect { data ->
				val response = ResponseParser.parse(data)
				if (response != null) {
					routeResponse(response)
				}
			}
		}
	}

	private suspend fun routeResponse(response: Response) {
		val pending = pendingResponseChannel
		if (pending != null && !isPushEvent(response)) {
			pending.send(response)
		} else {
			_pushEvents.emit(response)
		}
	}

	private fun isPushEvent(response: Response): Boolean = when (response) {
		is Response.MessagesWaiting -> true
		is Response.Ack -> true
		is Response.AdvertisementReceived -> true
		is Response.RawDataReceived -> true
		is Response.BinaryResponse -> true
		is Response.LogData -> true
		is Response.LoginSuccess -> true
		is Response.LoginFail -> true
		is Response.StatusResponse -> true
		is Response.TraceData -> true
		is Response.NewAdvert -> true
		is Response.TelemetryResponse -> true
		is Response.PathUpdated -> true
		is Response.PathDiscoveryResponse -> true
		is Response.ControlData -> true
		is Response.ContactDeleted -> true
		is Response.Unhandled -> true
		else -> false
	}

	suspend fun <T : Response> execute(
		command: ByteArray,
		timeout: Duration = defaultTimeout,
	): T {
		return executeStreaming<T>(command, timeout) { false }
	}

	suspend fun <T : Response> executeStreaming(
		command: ByteArray,
		timeout: Duration = defaultTimeout,
		onResponse: suspend (Response) -> Boolean,
	): T {
		try {
			return commandMutex.withLock {
				awaitStreaming(command, timeout, onResponse)
			}
		} catch (e: WriteStalled) {
			// The write never completed: the link is wedged and a disconnect is
			// the only cure. It must run with the mutex already released —
			// disconnecting while holding it can deadlock the collector and
			// reset() paths that also take the lock.
			connection.disconnect()
			throw MeshCoreException.CommandTimeout(
				"Link stalled: write did not complete within ${e.stalledFor}"
			)
		}
	}

	/**
	 * Runs under [commandMutex]. The write is bounded so a lost GATT completion
	 * cannot hold the queue — and every later command behind it — forever.
	 * Throws [WriteStalled] so the caller can disconnect after releasing the
	 * mutex.
	 */
	private suspend fun <T : Response> awaitStreaming(
		command: ByteArray,
		timeout: Duration,
		onResponse: suspend (Response) -> Boolean,
	): T {
		val responseChannel = Channel<Response>(1)
		pendingResponseChannel = responseChannel

		try {
			try {
				withTimeout(writeTimeout) {
					connection.write(command)
				}
			} catch (e: kotlinx.coroutines.TimeoutCancellationException) {
				// Only a genuine timeout is a stall: if the caller's coroutine
				// was cancelled instead, that cancellation must win — a plain
				// cancel is not a wedged link and must not disconnect.
				currentCoroutineContext().ensureActive()
				throw WriteStalled(writeTimeout)
			}

			var lastResponse: Response? = null
			while (true) {
				val response = withTimeout(timeout) {
					responseChannel.receive()
				}
				if (response is Response.Error) {
					throw MeshCoreException.DeviceError(response.code)
				}
				lastResponse = response
				if (!onResponse(response)) {
					break
				}
			}
			@Suppress("UNCHECKED_CAST")
			return lastResponse as T
		} catch (e: kotlinx.coroutines.TimeoutCancellationException) {
			throw MeshCoreException.CommandTimeout(
				"Command timed out after $timeout"
			)
		} finally {
			pendingResponseChannel = null
			responseChannel.close()
		}
	}

	fun reset() {
		pendingResponseChannel?.close()
		pendingResponseChannel = null
	}

	/**
	 * Internal signal: `connection.write` did not complete within [writeTimeout].
	 * [executeStreaming] converts it into a disconnect plus a public
	 * [MeshCoreException.CommandTimeout]; the disconnect happens with the queue
	 * mutex already released, and this exception never escapes as itself.
	 */
	private class WriteStalled(val stalledFor: Duration) : Exception()
}
