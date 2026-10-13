package com.darkrockstudios.libs.meshcore.ble

import kotlinx.coroutines.flow.Flow

interface BleAdapter {
	fun scan(filter: ScanFilter = ScanFilter()): Flow<DiscoveredDevice>
	fun stopScan()

	/**
	 * Opens a connection to [device]. [autoConnect] maps to the platform's
	 * opportunistic connect mode where one exists (Android
	 * `connectGatt(autoConnect = true)`, iOS `BlueFalcon.connect(autoConnect =
	 * …)`) and is ignored by adapters without such a mode. Default false:
	 * a direct connect, unchanged from the pre-flag behaviour.
	 */
	suspend fun connect(
		device: DiscoveredDevice,
		autoConnect: Boolean = false,
	): BleConnection
	val isBluetoothEnabled: Boolean
}
