@file:OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)

package com.darkrockstudios.libs.meshcore

import com.darkrockstudios.libs.meshcore.ble.BleAdapter
import com.darkrockstudios.libs.meshcore.ble.BleConnection
import com.darkrockstudios.libs.meshcore.ble.ConnectionState
import com.darkrockstudios.libs.meshcore.ble.DiscoveredDevice
import com.darkrockstudios.libs.meshcore.ble.MeshCoreBleException
import com.darkrockstudios.libs.meshcore.ble.ScanFilter
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class DeviceScannerTest {

	@Test
	fun isBluetoothEnabled_delegatesToAdapter() {
		val adapter = FakeBleAdapter()
		val scanner = DeviceScanner(adapter)

		adapter.isBluetoothEnabled = true
		assertTrue(scanner.isBluetoothEnabled)

		adapter.isBluetoothEnabled = false
		assertFalse(scanner.isBluetoothEnabled)
	}

	@Test
	fun startScan_startsAdapter() = runTest {
		val adapter = FakeBleAdapter()
		val scanner = DeviceScanner(adapter)

		scanner.startScan(scope = backgroundScope)
		// scan() is called synchronously before launching coroutine
		assertTrue(adapter.scanStarted)
		scanner.stopScan()
	}

	@Test
	fun startScan_accumulatesDevices() = runTest(UnconfinedTestDispatcher()) {
		val adapter = FakeBleAdapter()
		val scanner = DeviceScanner(adapter)

		scanner.startScan(scope = backgroundScope)

		adapter.emitScanResult(DiscoveredDevice("dev1", "Radio A", -50))
		assertEquals(1, scanner.discoveredDevices.value.size, "Expected 1 device after first emit")
		assertEquals("Radio A", scanner.discoveredDevices.value[0].name)

		adapter.emitScanResult(DiscoveredDevice("dev2", "Radio B", -70))
		assertEquals(2, scanner.discoveredDevices.value.size, "Expected 2 devices after second emit")

		scanner.stopScan()
	}

	@Test
	fun startScan_updatesExistingDevice() = runTest(UnconfinedTestDispatcher()) {
		val adapter = FakeBleAdapter()
		val scanner = DeviceScanner(adapter)

		scanner.startScan(scope = backgroundScope)

		adapter.emitScanResult(DiscoveredDevice("dev1", "Radio A", -50))
		assertEquals(1, scanner.discoveredDevices.value.size)
		assertEquals(-50, scanner.discoveredDevices.value[0].rssi)

		adapter.emitScanResult(DiscoveredDevice("dev1", "Radio A", -30))
		assertEquals(1, scanner.discoveredDevices.value.size)
		assertEquals(-30, scanner.discoveredDevices.value[0].rssi)

		scanner.stopScan()
	}

	@Test
	fun stopScan_stopsAdapter() = runTest {
		val adapter = FakeBleAdapter()
		val scanner = DeviceScanner(adapter)

		scanner.startScan(scope = backgroundScope)
		scanner.stopScan()
		assertTrue(adapter.scanStopped)
	}

	/**
	 * INVARIANT: after `stopScan()` + `startScan()`, the new scan's collection of the
	 * adapter flow must not begin until the previous collection has completed (its
	 * `awaitClose` cleanup ran). The real adapter's cleanup is `blueFalcon.stopScanning()`,
	 * so collecting the new flow before the old one has finished cleaning up lets a stale
	 * cleanup stop the scan that just started.
	 *
	 * The two scans run on different dispatchers to make the ordering observable: the
	 * first collection is parked on a [StandardTestDispatcher], so cancelling it queues
	 * its cleanup rather than running it, and the restart's [UnconfinedTestDispatcher]
	 * would collect eagerly — a second `collect-start` before the previous `cleanup` —
	 * unless `startScan()` joins the previous job first.
	 *
	 * [LifecycleRecordingBleAdapter.stopScan] is a no-op, so this fake never models the
	 * platform stop (`blueFalcon.stopScanning()`): the assertion is an ordering proxy for
	 * that hazard, not a reproduction of it.
	 */
	@Test
	fun startScan_waitsForPreviousCollectionToCleanUp() = runTest {
		val adapter = LifecycleRecordingBleAdapter()
		val scanner = DeviceScanner(adapter)

		val firstScope = CoroutineScope(StandardTestDispatcher(testScheduler))
		scanner.startScan(scope = firstScope)
		// The first collection only begins once the standard dispatcher runs.
		testScheduler.advanceUntilIdle()
		assertEquals(listOf("collect-start"), adapter.events)

		// stopScan() cancels without joining, so the cleanup is queued, not run.
		scanner.stopScan()

		val secondScope = CoroutineScope(UnconfinedTestDispatcher(testScheduler))
		scanner.startScan(scope = secondScope)
		testScheduler.advanceUntilIdle()

		assertEquals(
			listOf("collect-start", "cleanup", "collect-start"),
			adapter.events,
			"the new collection began before the previous one's cleanup ran: ${adapter.events}",
		)

		firstScope.cancel()
		secondScope.cancel()
	}

	@Test
	fun startScan_clearsPreviousResults() = runTest(UnconfinedTestDispatcher()) {
		val adapter = FakeBleAdapter()
		val scanner = DeviceScanner(adapter)

		scanner.startScan(scope = backgroundScope)
		adapter.emitScanResult(DiscoveredDevice("dev1", "Radio A", -50))
		assertEquals(1, scanner.discoveredDevices.value.size)

		// Start new scan - should clear (discoveredDevices is set synchronously)
		scanner.startScan(scope = backgroundScope)
		assertEquals(0, scanner.discoveredDevices.value.size)
		scanner.stopScan()
	}

	@Test
	fun startScan_withFilter() = runTest {
		val adapter = FakeBleAdapter()
		val scanner = DeviceScanner(adapter)

		val filter = ScanFilter(namePrefix = "MeshCore")
		scanner.startScan(filter = filter, scope = backgroundScope)
		assertTrue(adapter.scanStarted)
		scanner.stopScan()
	}

	@Test
	fun connect_releasesGattWhenMtuFails() = runTest {
		val bleConnection = FakeBleConnection().apply {
			mtuFailure = MeshCoreBleException("MTU request failed with status: 4")
		}
		val scanner = DeviceScanner(FakeBleAdapter(bleConnection))

		assertFailsWith<MeshCoreBleException> {
			scanner.connect(sampleDevice, backgroundScope, handshakeOffConfig())
		}

		assertEquals(1, bleConnection.disconnectCount)
		assertEquals(ConnectionState.Disconnected, bleConnection.connectionState.value)
	}

	@Test
	fun connect_releasesGattWhenHandshakeTimesOut() = runTest {
		val bleConnection = FakeBleConnection()
		val scanner = DeviceScanner(FakeBleAdapter(bleConnection))

		assertFailsWith<MeshCoreException.CommandTimeout> {
			scanner.connect(
				sampleDevice,
				backgroundScope,
				handshakeOffConfig().copy(commandTimeoutSeconds = 1),
			)
		}

		assertEquals(1, bleConnection.disconnectCount)
		assertEquals(ConnectionState.Disconnected, bleConnection.connectionState.value)
	}

	@Test
	fun connect_releasesGattWhenCancelledAfterConnect() = runTest {
		val bleConnection = FakeBleConnection().apply { hangMtu = true }
		val scanner = DeviceScanner(FakeBleAdapter(bleConnection))

		val job = launch(start = CoroutineStart.UNDISPATCHED) {
			scanner.connect(sampleDevice, backgroundScope, handshakeOffConfig())
		}
		assertTrue(job.isActive)

		job.cancelAndJoin()

		assertEquals(1, bleConnection.disconnectCount)
		assertEquals(ConnectionState.Disconnected, bleConnection.connectionState.value)
	}

	private fun handshakeOffConfig() = ConnectionConfig(
		autoSyncTime = false,
		autoFetchContacts = false,
		autoFetchChannels = false,
		autoPollMessages = false,
	)

	private val sampleDevice = DiscoveredDevice("dev1", "Radio A", -50)
}

/**
 * Records the collection lifecycle of [scan]: it returns a cold `callbackFlow` that
 * appends `"collect-start"` when collection begins and `"cleanup"` when the collection
 * ends (its `awaitClose` block, the real adapter's `stopScanning()` slot), so the order
 * of [events] is the order a collector actually observed.
 */
private class LifecycleRecordingBleAdapter : BleAdapter {
	val events = mutableListOf<String>()

	override var isBluetoothEnabled: Boolean = true

	override fun scan(filter: ScanFilter): Flow<DiscoveredDevice> =
		callbackFlow {
			events += "collect-start"
			trySend(DiscoveredDevice("dev1", "Radio A", -50))
			awaitClose { events += "cleanup" }
		}

	override fun stopScan() = Unit

	override suspend fun connect(device: DiscoveredDevice): BleConnection = error("scan-only fake")
}
