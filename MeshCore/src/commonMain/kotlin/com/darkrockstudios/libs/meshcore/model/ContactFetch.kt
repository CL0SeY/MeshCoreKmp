package com.darkrockstudios.libs.meshcore.model

import kotlinx.serialization.Serializable

/**
 * One contact reply, either the full list or a `since`-filtered delta.
 *
 * The firmware brackets a contact stream with `RESP_CODE_CONTACTS_START`
 * (`[0x02][count as 4-byte LE]`, MyMesh.cpp:1338-1341) and
 * `RESP_CODE_END_OF_CONTACTS` (`[0x04][lastmod as 4-byte LE]`,
 * MyMesh.cpp:2361-2365).
 */
@Serializable
data class ContactFetch(
	/** The records the node streamed, already filtered when a `since` was sent. */
	val contacts: List<Contact>,
	/**
	 * The node's `getNumContacts()` before the `since` filter — the unfiltered
	 * total, not [contacts]`.size`. Compare it against the count the caller
	 * holds to detect shrinkage (contacts removed on the node), which is the
	 * signal to fall back to a full refresh.
	 */
	val totalAtStart: Int,
	/**
	 * Max `lastmod` among the records actually streamed in this reply, and 0
	 * when none were. This is the caller's next `since`; a 0 must NOT replace a
	 * live cursor, or the next refresh would re-fetch every contact.
	 */
	val mostRecentLastmod: Long,
)
