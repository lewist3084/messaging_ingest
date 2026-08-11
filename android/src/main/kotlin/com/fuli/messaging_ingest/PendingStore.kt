package com.fuli.messaging_ingest

import android.content.Context
import org.json.JSONArray
import org.json.JSONObject

/**
 * Durable queue + dedup ledger backed by SharedPreferences.
 *
 * Two problems make a naive notification listener produce garbage, and this
 * class exists for both:
 *
 *  1. **The Flutter engine is usually dead.** The system binds the listener
 *     service on boot and keeps it bound; our Dart isolate is not running for
 *     most of the messages we see. Anything not persisted here is lost.
 *
 *  2. **Notifications re-post their whole history.** Google Messages updates a
 *     conversation's notification by re-posting it with every recent message
 *     in the MessagingStyle, not just the new one. Treating each
 *     `onNotificationPosted` as "one new message" duplicates the entire thread
 *     on every incoming text. [seen] is what stops that.
 */
internal class PendingStore(context: Context) {

    private val prefs =
        context.getSharedPreferences("messaging_ingest", Context.MODE_PRIVATE)

    companion object {
        private const val KEY_QUEUE = "queue"
        private const val KEY_SEEN = "seen"

        /** Cap on undrained messages. Past this the oldest are dropped: a queue
         *  that grew without bound would eventually stall the service on a
         *  multi-megabyte JSON parse inside onNotificationPosted. */
        private const val MAX_QUEUE = 1000

        /** Dedup window. Each notification re-post carries at most ~25 messages,
         *  so this covers many conversations' worth of overlap. */
        private const val MAX_SEEN = 2000
    }

    @Synchronized
    fun hasSeen(key: String): Boolean = readSeen().contains(key)

    /** Records [key] as seen, trimming oldest-first. Insertion order is the
     *  ledger's order, so a JSONArray is the right shape — not a Set. */
    @Synchronized
    fun markSeen(key: String) {
        val seen = readSeen()
        if (seen.contains(key)) return
        seen.add(key)
        while (seen.size > MAX_SEEN) seen.removeAt(0)
        val arr = JSONArray()
        seen.forEach { arr.put(it) }
        prefs.edit().putString(KEY_SEEN, arr.toString()).apply()
    }

    @Synchronized
    fun enqueue(message: JSONObject) {
        val queue = readQueue()
        queue.put(message)
        val trimmed = if (queue.length() > MAX_QUEUE) {
            val out = JSONArray()
            for (i in (queue.length() - MAX_QUEUE) until queue.length()) {
                out.put(queue.get(i))
            }
            out
        } else {
            queue
        }
        prefs.edit().putString(KEY_QUEUE, trimmed.toString()).apply()
    }

    /** Returns everything queued and clears it. The caller owns the messages
     *  after this returns — if the Dart side drops them they are gone. */
    @Synchronized
    fun drain(): List<Map<String, Any?>> {
        val queue = readQueue()
        prefs.edit().remove(KEY_QUEUE).apply()
        val out = ArrayList<Map<String, Any?>>(queue.length())
        for (i in 0 until queue.length()) {
            val obj = queue.optJSONObject(i) ?: continue
            out.add(obj.toMap())
        }
        return out
    }

    @Synchronized
    fun queueDepth(): Int = readQueue().length()

    private fun readQueue(): JSONArray =
        runCatching { JSONArray(prefs.getString(KEY_QUEUE, "[]")) }.getOrElse { JSONArray() }

    private fun readSeen(): MutableList<String> {
        val arr = runCatching { JSONArray(prefs.getString(KEY_SEEN, "[]")) }
            .getOrElse { JSONArray() }
        val out = ArrayList<String>(arr.length())
        for (i in 0 until arr.length()) out.add(arr.optString(i))
        return out
    }
}

internal fun JSONObject.toMap(): Map<String, Any?> {
    val out = HashMap<String, Any?>()
    val it = keys()
    while (it.hasNext()) {
        val k = it.next()
        val v = get(k)
        out[k] = if (v === JSONObject.NULL) null else v
    }
    return out
}
