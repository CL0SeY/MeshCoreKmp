package com.darkrockstudios.libs.meshcore.model

import kotlinx.serialization.Serializable

@Serializable
data class MessageSentConfirmation(
	val messageType: Int,
	val expectedAck: String,
	/**
	 * The node's estimate of how long an ACK can take, in **milliseconds**
	 * (`PACKET_MSG_SENT` bytes 6-9; firmware `calcDirectTimeoutMillisFor` /
	 * `calcFloodTimeoutMillisFor`). 0 — or a garbled high-bit value that lands
	 * here negative — means no usable estimate: callers fall back to the
	 * connection's command timeout.
	 */
	val suggestedTimeoutMillis: Int,
)
