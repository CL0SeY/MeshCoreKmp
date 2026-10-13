@file:OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)

package com.darkrockstudios.libs.meshcore.protocol

import com.darkrockstudios.libs.meshcore.FakeBleConnection
import com.darkrockstudios.libs.meshcore.MeshCoreException
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.time.Duration.Companion.milliseconds

class CommandQueueTest {

	@Test
	fun execute_sendsCommandAndReceivesResponse() = runTest {
		val bleConnection = FakeBleConnection()
		val queue = CommandQueue(
			connection = bleConnection,
			scope = backgroundScope,
		)
		testScheduler.advanceUntilIdle()

		launch {
			while (bleConnection.writtenData.isEmpty()) {
				kotlinx.coroutines.yield()
			}
			bleConnection.simulateResponse(byteArrayOf(0x00, 0x2A, 0x00, 0x00, 0x00))
		}

		val result = queue.execute<Response.Ok>(
			command = CommandSerializer.getBattery(),
		)

		assertEquals(42, result.value)
		assertEquals(1, bleConnection.writtenData.size)
		assertEquals(0x14.toByte(), bleConnection.writtenData[0][0])
	}

	@Test
	fun execute_deviceErrorThrows() = runTest {
		val bleConnection = FakeBleConnection()
		val queue = CommandQueue(
			connection = bleConnection,
			scope = backgroundScope,
		)
		testScheduler.advanceUntilIdle()

		launch {
			while (bleConnection.writtenData.isEmpty()) {
				kotlinx.coroutines.yield()
			}
			bleConnection.simulateResponse(byteArrayOf(0x01, 0x07))
		}

		val exception = assertFailsWith<MeshCoreException.DeviceError> {
			queue.execute<Response.Ok>(
				command = CommandSerializer.getBattery(),
			)
		}
		assertEquals(0x07, exception.errorCode)
	}

	@Test
	fun execute_timeoutThrows() = runTest {
		val bleConnection = FakeBleConnection()
		val queue = CommandQueue(
			connection = bleConnection,
			scope = backgroundScope,
			defaultTimeout = 100.milliseconds,
		)
		testScheduler.advanceUntilIdle()

		assertFailsWith<MeshCoreException.CommandTimeout> {
			queue.execute<Response.Ok>(
				command = CommandSerializer.getBattery(),
				timeout = 100.milliseconds,
			)
		}
	}

	@Test
	fun pushEvents_messagesWaitingRouted() = runTest(UnconfinedTestDispatcher()) {
		val bleConnection = FakeBleConnection()
		val queue = CommandQueue(
			connection = bleConnection,
			scope = backgroundScope,
		)

		val collected = mutableListOf<Response>()
		val collectJob = backgroundScope.launch {
			queue.pushEvents.collect { collected.add(it) }
		}

		// Simulate a push event (no command pending)
		bleConnection.simulateResponse(byteArrayOf(0x83.toByte(), 0x03))

		assertEquals(1, collected.size, "Expected 1 push event, got ${collected.size}")
		assertIs<Response.MessagesWaiting>(collected[0])
		assertEquals(3, (collected[0] as Response.MessagesWaiting).count)

		collectJob.cancel()
	}

	@Test
	fun pushEvents_ackRoutedDuringCommand() = runTest {
		val bleConnection = FakeBleConnection()
		val queue = CommandQueue(
			connection = bleConnection,
			scope = backgroundScope,
		)

		val pushEvents = mutableListOf<Response>()
		val collectJob = backgroundScope.launch {
			queue.pushEvents.collect { pushEvents.add(it) }
		}
		testScheduler.advanceUntilIdle()

		launch {
			while (bleConnection.writtenData.isEmpty()) {
				kotlinx.coroutines.yield()
			}
			// Send a push event (ACK) while a command is pending
			bleConnection.simulateResponse(byteArrayOf(
				0x82.toByte(), 0x01, 0x02, 0x03, 0x04, 0x05, 0x06
			))
			kotlinx.coroutines.yield()
			// Then send the actual command response
			bleConnection.simulateResponse(byteArrayOf(0x00))
		}

		val result = queue.execute<Response.Ok>(
			command = CommandSerializer.getBattery(),
		)
		testScheduler.advanceUntilIdle()

		assertEquals(1, pushEvents.size)
		assertIs<Response.Ack>(pushEvents[0])
		assertIs<Response.Ok>(result)

		collectJob.cancel()
	}

	@Test
	fun pushEvents_rawDataRoutedAsPush() = runTest(UnconfinedTestDispatcher()) {
		val bleConnection = FakeBleConnection()
		val queue = CommandQueue(
			connection = bleConnection,
			scope = backgroundScope,
		)

		val collected = mutableListOf<Response>()
		val collectJob = backgroundScope.launch {
			queue.pushEvents.collect { collected.add(it) }
		}

		// Simulate a raw data push event (no command pending)
		bleConnection.simulateResponse(byteArrayOf(
			0x84.toByte(), 40, (-80).toByte(), 0xFF.toByte(), 0x01, 0x02,
		))

		assertEquals(1, collected.size, "Expected 1 push event, got ${collected.size}")
		assertIs<Response.RawDataReceived>(collected[0])

		collectJob.cancel()
	}

	@Test
	fun pushEvents_binaryResponseRoutedAsPush() = runTest(UnconfinedTestDispatcher()) {
		val bleConnection = FakeBleConnection()
		val queue = CommandQueue(
			connection = bleConnection,
			scope = backgroundScope,
		)

		val collected = mutableListOf<Response>()
		val collectJob = backgroundScope.launch {
			queue.pushEvents.collect { collected.add(it) }
		}

		// Simulate a binary response push event
		val data = ByteArray(10)
		data[0] = 0x8C.toByte()
		data[1] = 0x00
		// tag = 99
		data[2] = 0x63; data[3] = 0x00; data[4] = 0x00; data[5] = 0x00
		data[6] = 0xAA.toByte(); data[7] = 0xBB.toByte()
		data[8] = 0xCC.toByte(); data[9] = 0xDD.toByte()
		bleConnection.simulateResponse(data)

		assertEquals(1, collected.size, "Expected 1 push event, got ${collected.size}")
		assertIs<Response.BinaryResponse>(collected[0])
		assertEquals(99L, (collected[0] as Response.BinaryResponse).tag)

		collectJob.cancel()
	}

	@Test
	fun pushEvents_rawDataRoutedDuringCommand() = runTest {
		val bleConnection = FakeBleConnection()
		val queue = CommandQueue(
			connection = bleConnection,
			scope = backgroundScope,
		)

		val pushEvents = mutableListOf<Response>()
		val collectJob = backgroundScope.launch {
			queue.pushEvents.collect { pushEvents.add(it) }
		}
		testScheduler.advanceUntilIdle()

		launch {
			while (bleConnection.writtenData.isEmpty()) {
				kotlinx.coroutines.yield()
			}
			// Send raw data push while a command is pending
			bleConnection.simulateResponse(byteArrayOf(
				0x84.toByte(), 0x00, 0x00, 0xFF.toByte(), 0x01,
			))
			kotlinx.coroutines.yield()
			// Then send the actual command response
			bleConnection.simulateResponse(byteArrayOf(0x00))
		}

		val result = queue.execute<Response.Ok>(
			command = CommandSerializer.getBattery(),
		)
		testScheduler.advanceUntilIdle()

		assertEquals(1, pushEvents.size)
		assertIs<Response.RawDataReceived>(pushEvents[0])
		assertIs<Response.Ok>(result)

		collectJob.cancel()
	}

	@Test
	fun pushEvents_pathUpdatedRoutedDuringCommand() = runTest {
		val bleConnection = FakeBleConnection()
		val queue = CommandQueue(
			connection = bleConnection,
			scope = backgroundScope,
		)

		val pushEvents = mutableListOf<Response>()
		val collectJob = backgroundScope.launch {
			queue.pushEvents.collect { pushEvents.add(it) }
		}
		testScheduler.advanceUntilIdle()

		// PUSH_CODE_PATH_UPDATED (0x81): exactly [0x81][32-byte contact public key].
		val publicKey = ByteArray(32) { (it + 1).toByte() }

		launch {
			while (bleConnection.writtenData.isEmpty()) {
				kotlinx.coroutines.yield()
			}
			// Send a path-updated push while a command is pending
			bleConnection.simulateResponse(byteArrayOf(0x81.toByte()) + publicKey)
			kotlinx.coroutines.yield()
			// Then send the actual command response
			bleConnection.simulateResponse(byteArrayOf(0x00))
		}

		val result = queue.execute<Response.Ok>(
			command = CommandSerializer.getBattery(),
		)
		testScheduler.advanceUntilIdle()

		assertEquals(1, pushEvents.size)
		assertIs<Response.PathUpdated>(pushEvents[0])
		val pathUpdated = pushEvents[0] as Response.PathUpdated
		assertEquals(32, pathUpdated.publicKey.size)
		assertContentEquals(publicKey, pathUpdated.publicKey)
		assertIs<Response.Ok>(result)

		collectJob.cancel()
	}

	@Test
	fun execute_stalledWriteTimesOutDisconnectsAndReleasesQueue() = runTest {
		val bleConnection = FakeBleConnection()
		val queue = CommandQueue(
			connection = bleConnection,
			scope = backgroundScope,
			writeTimeout = 100.milliseconds,
		)
		bleConnection.hangWrite = true

		assertFailsWith<MeshCoreException.CommandTimeout> {
			queue.execute<Response.Ok>(command = CommandSerializer.getBattery())
		}
		assertEquals(1, bleConnection.disconnectCount)

		// The stalled write never completed, yet the queue mutex was released:
		// the next command goes through and is answered.
		bleConnection.hangWrite = false
		val stalledWrites = bleConnection.writtenData.size
		launch {
			while (bleConnection.writtenData.size <= stalledWrites) {
				kotlinx.coroutines.yield()
			}
			bleConnection.simulateResponse(byteArrayOf(0x00, 0x2A, 0x00, 0x00, 0x00))
		}

		val result = queue.execute<Response.Ok>(command = CommandSerializer.getBattery())

		assertEquals(42, result.value)
		assertEquals(stalledWrites + 1, bleConnection.writtenData.size)
	}

	@Test
	fun execute_cancelledWriteDoesNotDisconnect() = runTest {
		val bleConnection = FakeBleConnection()
		val queue = CommandQueue(
			connection = bleConnection,
			scope = backgroundScope,
		)
		bleConnection.hangWrite = true

		val caller = launch {
			queue.execute<Response.Ok>(command = CommandSerializer.getBattery())
		}
		// runCurrent, not advanceUntilIdle: advancing virtual time to idle fires
		// the write timeout and turns this into the stalled-write case.
		testScheduler.runCurrent()
		caller.cancel()
		testScheduler.advanceUntilIdle()

		kotlin.test.assertTrue(caller.isCancelled)
		assertEquals(0, bleConnection.disconnectCount, "a cancelled caller is not a wedged link")
	}
}
