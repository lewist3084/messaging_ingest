package com.fuli.messaging_ingest

/**
 * The document ids the Dart side mints, reproduced bit for bit.
 *
 * `MessageIngestService.docIdFor` (FuLi) keys every captured conversation and
 * message on a sanitized head plus Dart's `String.hashCode` in base 36. Since
 * 2026-09-07 the phone writes those documents itself, so it has to arrive at
 * the SAME id for the same key — otherwise a re-drain from the Dart fallback
 * path, or the migration script, files a second copy of every thread.
 *
 * Dart VM `String.hashCode` is Jenkins one-at-a-time over UTF-16 code units,
 * 32-bit unsigned throughout, masked to 30 bits at the end, with 0 mapped to 1.
 * Kotlin's `String.hashCode` is Java's and is NOT this function. Verified
 * against live Dart output on 2026-09-07 for ASCII, Latin-1, an emoji (a
 * surrogate pair) and the empty string — the same vectors the migration
 * script (`functions/scripts/migrate-device-texts.mjs`) was checked with.
 *
 * 🛑 Dart web (dart2js / wasm) hashes differently. These ids are only ever
 * minted by the VM (Android) or by this file; a web build never writes one.
 */
internal object DartDocId {

    private const val MASK32 = 0xFFFFFFFFL

    /** Dart VM `String.hashCode` of [s]. */
    fun dartHashCode(s: String): Long {
        var h = 0L
        for (ch in s) {
            h = (h + ch.code) and MASK32
            h = (h + ((h shl 10) and MASK32)) and MASK32
            h = h xor (h ushr 6)
        }
        h = (h + ((h shl 3) and MASK32)) and MASK32
        h = h xor (h ushr 11)
        h = (h + ((h shl 15) and MASK32)) and MASK32
        h = h and 0x3fffffffL
        return if (h == 0L) 1L else h
    }

    /**
     * Firestore-safe id for an arbitrary capture key: the key with everything
     * outside `[A-Za-z0-9_-]` replaced by `_`, cut to 100 characters, then
     * `_` and the Dart hash of the ORIGINAL in base 36.
     */
    fun docIdFor(raw: String): String {
        val safe = StringBuilder(raw.length)
        for (ch in raw) {
            safe.append(if (ch.isAsciiSafe()) ch else '_')
        }
        val head = if (safe.length <= 100) safe.toString() else safe.substring(0, 100)
        return head + "_" + dartHashCode(raw).toString(36)
    }

    /** The `textConversation` id for a captured (notification) thread. */
    fun deviceConversationId(conversationKey: String): String =
        "device_" + docIdFor(conversationKey)

    /** The `textConversation` id a sent text creates when nothing matches. */
    fun deviceSmsConversationId(handsetThreadId: String): String =
        "device_sms_" + docIdFor(handsetThreadId)

    private fun Char.isAsciiSafe(): Boolean =
        this in 'A'..'Z' || this in 'a'..'z' || this in '0'..'9' || this == '_' || this == '-'
}
