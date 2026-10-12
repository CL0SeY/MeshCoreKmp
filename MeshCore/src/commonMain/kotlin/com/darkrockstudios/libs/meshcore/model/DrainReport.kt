package com.darkrockstudios.libs.meshcore.model

/**
 * Lifecycle of an internal message drain ([com.darkrockstudios.libs.meshcore.DeviceConnection.drainReports]).
 *
 * Exactly one [Started] precedes each drain and exactly one terminal report
 * ([Completed] or [Failed]) follows it — unless the drain is cancelled, in
 * which case the [Started] has no terminal — so a host can bracket a drain
 * in its own log: a missing message then reads as start-with-no-done (or an
 * explicit failure) instead of silence.
 *
 * Reports are observational — they never fetch; GET_MESSAGE stays the drain's.
 */
sealed interface DrainReport {
	/** A drain began. [waiting] is the node's push count when one triggered it. */
	data class Started(val trigger: String, val waiting: Int?) : DrainReport

	/** The drain ended normally after delivering [delivered] messages. */
	data class Completed(val trigger: String, val delivered: Int) : DrainReport

	/** The drain died before completing; the last fetch's error explains why. */
	data class Failed(val trigger: String, val delivered: Int, val reason: String) : DrainReport
}
