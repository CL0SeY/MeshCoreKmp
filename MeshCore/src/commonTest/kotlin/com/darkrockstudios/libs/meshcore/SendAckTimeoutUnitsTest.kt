@file:OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)

package com.darkrockstudios.libs.meshcore

import com.darkrockstudios.libs.meshcore.protocol.CommandQueue
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * `PACKET_MSG_SENT` bytes 6-9 are the node's estimated ACK wait in **milliseconds**
 * (firmware `MyMesh::calcDirectTimeoutMillisFor` / `calcFloodTimeoutMillisFor`,
 * `docs/companion_protocol.md` in MeshCore). The library used to treat the field as
 * seconds and wait `value * 1000` ms, so a missed ACK parked a DM on attempt 1 for
 * the better part of an hour instead of the few seconds the node asked for.
 */
class SendAckTimeoutUnitsTest {

	private fun messageSentResponse(suggestedTimeoutMillis: Int): ByteArray {
		val resp = ByteArray(10)
		resp[0] = 0x06
		resp[1] = 0
		resp[2] = 0x0A
		resp[3] = 0x0B
		resp[4] = 0x0C
		resp[5] = 0x0D
		resp[6] = (suggestedTimeoutMillis and 0xFF).toByte()
		resp[7] = ((suggestedTimeoutMillis shr 8) and 0xFF).toByte()
		resp[8] = ((suggestedTimeoutMillis shr 16) and 0xFF).toByte()
		resp[9] = ((suggestedTimeoutMillis shr 24) and 0xFF).toByte()
		return resp
	}

	@Test
	fun ackWaitHonoursTheNodesMillisecondTimeout() = runTest {
		val bleConnection = FakeBleConnection()
		val queue = CommandQueue(connection = bleConnection, scope = backgroundScope)
		testScheduler.advanceUntilIdle()
		val connection = DeviceConnection(
			bleConnection = bleConnection,
			commandQueue = queue,
			scope = backgroundScope,
			config = ConnectionConfig(
				autoSyncTime = false,
				autoFetchContacts = false,
				autoFetchChannels = false,
				autoPollMessages = false,
			),
		)

		// Node reply: MSG_SENT, expected ack 0a0b0c0d, suggested timeout 3050 ms —
		// a direct send over a 2-hop path at ~300 ms packet airtime:
		// 500 + (300*6 + 250) * 3.
		launch {
			while (bleConnection.writtenData.isEmpty()) { yield() }
			bleConnection.simulateResponse(messageSentResponse(3_050))
		}

		val start = testScheduler.currentTime
		val result = connection.sendAndAwaitAck {
			sendDirectMessage(ByteArray(6) { it.toByte() }, "ping")
		}
		val waitedMillis = testScheduler.currentTime - start

		assertTrue(result.isFailure, "expected an ACK timeout, got $result")
		assertTrue(
			waitedMillis == 3_050L,
			"waited ${waitedMillis}ms for an ACK the node estimated at 3_050ms",
		)
	}

	@Test
	fun ackWaitIsBoundedWhenTheNodesEstimateIsGarbled() = runTest {
		val bleConnection = FakeBleConnection()
		val queue = CommandQueue(connection = bleConnection, scope = backgroundScope)
		testScheduler.advanceUntilIdle()
		val connection = DeviceConnection(
			bleConnection = bleConnection,
			commandQueue = queue,
			scope = backgroundScope,
			config = ConnectionConfig(
				autoSyncTime = false,
				autoFetchContacts = false,
				autoFetchChannels = false,
				autoPollMessages = false,
			),
		)

		// A corrupted uint32 (0x7FFFFFFF ≈ 24.8 days) must not park the caller.
		launch {
			while (bleConnection.writtenData.isEmpty()) { yield() }
			bleConnection.simulateResponse(messageSentResponse(Int.MAX_VALUE))
		}

		val start = testScheduler.currentTime
		val result = connection.sendAndAwaitAck {
			sendDirectMessage(ByteArray(6) { it.toByte() }, "ping")
		}
		val waitedMillis = testScheduler.currentTime - start

		assertTrue(result.isFailure, "expected an ACK timeout, got $result")
		assertTrue(
			waitedMillis == 120_000L,
			"garbled estimate waited ${waitedMillis}ms instead of the 120000ms bound",
		)
	}
}
