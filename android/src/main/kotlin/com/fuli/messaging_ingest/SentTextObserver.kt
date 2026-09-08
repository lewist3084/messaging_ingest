package com.fuli.messaging_ingest

import android.content.Context
import android.database.ContentObserver
import android.net.Uri
import android.os.Handler
import android.os.Looper
import android.provider.Telephony
import android.util.Log

/**
 * Watches the handset's SMS/MMS store and files what the member SENDS, with
 * no Flutter engine — the sent-side twin of [MessageNotificationListener].
 *
 * A text typed in the messaging app posts no notification; it lands in the
 * provider. Until 2026-09-07 only the Dart side read that store, on boot or
 * when the Texts screen opened, so the member's own half of a conversation
 * reached their computer whenever the app next ran. Now the provider's
 * change notification triggers a native sync, debounced, on the writer's
 * thread. The same watermark and the same ids as the Dart sync, so the two
 * can run in either order and neither duplicates the other.
 *
 * Registration needs READ_SMS, which is asked on the phone's channel page,
 * never at launch — so [ensureRegistered] is cheap and is called from every
 * place that might be the first moment the grant exists: the listener's
 * `onCreate`, every notification it sees, and the Dart side's `configure`.
 */
internal object SentTextObserver {

    private const val TAG = "SentTextObserver"

    /** How long after the last provider change before reading. A sent MMS
     *  writes several rows; one read after the burst is enough. */
    private const val DEBOUNCE_MS = 3000L

    private val main = Handler(Looper.getMainLooper())

    @Volatile private var registered = false
    private var observer: ContentObserver? = null
    private var pending: Runnable? = null

    private val URIS: List<Uri> = listOf(
        Telephony.Sms.CONTENT_URI,
        Telephony.Mms.CONTENT_URI,
    )

    /** Registers once, when READ_SMS is held; a no-op otherwise or after. */
    @Synchronized
    fun ensureRegistered(context: Context) {
        if (registered) return
        val app = context.applicationContext
        if (!SentMessageReader(app).isGranted()) return
        val obs = object : ContentObserver(main) {
            override fun onChange(selfChange: Boolean) = kick(app)
            override fun onChange(selfChange: Boolean, uri: Uri?) = kick(app)
        }
        runCatching {
            for (uri in URIS) app.contentResolver.registerContentObserver(uri, true, obs)
            observer = obs
            registered = true
            Log.i(TAG, "watching the SMS/MMS store")
        }.onFailure { Log.w(TAG, "could not register: ${it.message}") }
    }

    /** Schedules one sync after the burst settles. Safe from any thread. */
    fun kick(context: Context) {
        val app = context.applicationContext
        synchronized(this) {
            pending?.let { main.removeCallbacks(it) }
            val r = Runnable {
                synchronized(this) { pending = null }
                DeviceTextWriter.executor.execute {
                    val n = runCatching { DeviceTextWriter(app).syncSent() }
                        .onFailure { Log.w(TAG, "sent sync failed: ${it.message}") }
                        .getOrDefault(0)
                    if (n > 0) Log.i(TAG, "filed $n sent text(s)")
                }
            }
            pending = r
            main.postDelayed(r, DEBOUNCE_MS)
        }
    }
}
