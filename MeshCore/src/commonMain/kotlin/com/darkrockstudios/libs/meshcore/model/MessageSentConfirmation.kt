package com.darkrockstudios.libs.meshcore.model

import kotlinx.serialization.Serializable

@Serializable
data class MessageSentConfirmation(
	val messageType: Int,
	val expectedAck: String,
	/**
	 * The node's estimate of how long an ACK can take, in **milliseconds**
	 * (`PACKET_MSG_SENT` bytes 6-9; firmware `calcDirectTimeoutMillisFor` /
	 * `calcFloodTimeoutMillisFor`). 0 when the node expects no ACK.
	 */
	val suggestedTimeoutMillis: Int,
)
