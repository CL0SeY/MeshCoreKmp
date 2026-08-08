package com.darkrockstudios.libs.meshcore.model

import kotlinx.serialization.Serializable

@Serializable
data class Channel(
	val index: Int,
	val name: String,
	val secret: String = "",
) {
	val isEmpty: Boolean get() = name.isBlank()
}
