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
) {
	val commandTimeout: Duration get() = commandTimeoutSeconds.seconds
}
