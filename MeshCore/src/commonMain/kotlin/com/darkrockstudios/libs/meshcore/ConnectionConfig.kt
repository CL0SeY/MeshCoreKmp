package com.darkrockstudios.libs.meshcore

import com.darkrockstudios.libs.meshcore.ble.ConnectionPriority
import kotlinx.serialization.Serializable
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

@Serializable
data class ConnectionConfig(
	val appName: String = "mccli",
	val commandTimeoutSeconds: Long = 10,
	val requestedMtu: Int = 512,
	val autoSyncTime: Boolean = true,
	val autoFetchContacts: Boolean = true,
	val autoFetchChannels: Boolean = true,
	val autoPollMessages: Boolean = true,
	val connectionPriority: ConnectionPriority? = null,
	/**
	 * Android: opens the GATT with `connectGatt(autoConnect = true)` — the
	 * stack keeps the connection attempt armed while the process lives and
	 * completes it when the peripheral appears, instead of a direct attempt
	 * that ends when it gives up. The connect wait still bounds the attempt
	 * and the GATT is still closed on give-up, so an opportunistic connect
	 * never outlives the attempt. iOS passes the flag to
	 * `BlueFalcon.connect(peripheral, autoConnect = …)`. Default false:
	 * direct connects are unchanged.
	 */
	val autoConnect: Boolean = false,
) {
	val commandTimeout: Duration get() = commandTimeoutSeconds.seconds
}
