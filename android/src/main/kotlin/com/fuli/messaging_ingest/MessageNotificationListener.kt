package com.fuli.messaging_ingest

import android.app.Notification
import android.os.Build
import android.service.notification.NotificationListenerService
import android.service.notification.StatusBarNotification
import androidx.core.app.NotificationCompat
import org.json.JSONObject

/**
 * Captures messages by reading MessagingStyle notifications.
 *
 * This is the only route to RCS content on Android. The framework RCS APIs are
 * annotated `@hide` and allowlisted to Google Messages, so no app — including
 * one running on your own phone — can read RCS threads directly. What Google
 * Messages *does* do is post a MessagingStyle notification per conversation,
 * and that carries per-message sender attribution, which is enough to
 * reconstruct a thread.
 *
 * Deliberately strict: a notification without a MessagingStyle is ignored
 * outright rather than being scraped from EXTRA_TITLE/EXTRA_TEXT. Those fields
 * hold summaries ("3 new messages"), sender-less previews, and app chrome, and
 * folding them in is what turns an ingest feed into noise.
 */
class MessageNotificationListener : NotificationListenerService() {

    private lateinit var store: PendingStore

    companion object {
        /** Set by the plugin while a Dart isolate is alive. Null most of the
         *  time — the service outlives the engine by design. */
        @Volatile
        internal var sink: ((Map<String, Any?>) -> Unit)? = null

        @Volatile
        internal var connected: Boolean = false

        /** What Android substitutes for a redacted sensitive notification,
         *  as measured on Android 16 in English. A handset in another locale
         *  posts the same resource translated; add it here once seen. */
        private val REDACTED_PLACEHOLDERS = setOf(
            "Sensitive notification content hidden",
        )

        /**
         * The live reply box of each conversation whose notification is still
         * posted: the RemoteInput action Google Messages attached, keyed on
         * the same conversation key the capture uses. Firing it with text is
         * how a smartwatch or Android Auto answers a thread, and it is the
         * ONLY route into an RCS chat any third-party app has. It exists only
         * while the notification does — reading the thread on the phone
         * dismisses it, and [onNotificationRemoved] drops the entry.
         */
        private val replyTargets =
            java.util.concurrent.ConcurrentHashMap<String, ReplyTarget>()

        internal fun canReplyInline(conversationKey: String): Boolean =
            replyTargets.containsKey(conversationKey)

        /**
         * Answers [conversationKey] through its posted notification. False
         * when there is no live reply box; throws if the system refused to
         * fire the action (the intent was cancelled under us).
         */
        internal fun replyInline(context: android.content.Context, conversationKey: String, text: String): Boolean {
            val target = replyTargets[conversationKey] ?: return false
            val intent = android.content.Intent()
            val results = android.os.Bundle().apply { putCharSequence(target.remoteInput.resultKey, text) }
            androidx.core.app.RemoteInput.addResultsToIntent(arrayOf(target.remoteInput), intent, results)
            val pending = target.action.actionIntent
                ?: throw IllegalStateException("reply action carries no intent")
            pending.send(context, 0, intent)
            return true
        }
    }

    internal class ReplyTarget(
        val sbnKey: String,
        val action: androidx.core.app.NotificationCompat.Action,
        val remoteInput: androidx.core.app.RemoteInput,
    )

    override fun onNotificationRemoved(sbn: StatusBarNotification) {
        // Keyed on conversation, removed by notification: the two are 1:1 for
        // Google Messages, and a stale entry would fire a cancelled intent.
        val gone = replyTargets.entries.filter { it.value.sbnKey == sbn.key }.map { it.key }
        for (k in gone) replyTargets.remove(k)
    }

    override fun onCreate() {
        super.onCreate()
        store = PendingStore(applicationContext)
        writer = DeviceTextWriter(applicationContext)
        // The sent side rides on the same always-alive process. Registered
        // here when READ_SMS is already held, and re-tried on every
        // notification for the day the grant arrives.
        SentTextObserver.ensureRegistered(applicationContext)
    }

    private lateinit var writer: DeviceTextWriter

    override fun onListenerConnected() {
        super.onListenerConnected()
        connected = true
    }

    override fun onListenerDisconnected() {
        super.onListenerDisconnected()
        connected = false
        // Android drops the binding on app update and occasionally under memory
        // pressure. Without this the service stays dead until the user toggles
        // the permission off and on again, which they will never think to do.
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
            requestRebind(componentNameFor())
        }
    }

    private fun componentNameFor() =
        android.content.ComponentName(this, MessageNotificationListener::class.java)

    override fun onNotificationPosted(sbn: StatusBarNotification) {
        runCatching { ingest(sbn) }
        runCatching { SentTextObserver.ensureRegistered(applicationContext) }
    }

    /**
     * Where a fresh capture goes. Since 2026-09-07 the phone writes it to
     * Firestore itself, on the writer's thread, so the text reaches the
     * member's computer whether or not the app is running. The pending queue
     * is the fallback: anything the writer could not place — no config yet,
     * signed out, offline past the timeout, a switch that says hold — is
     * queued exactly as before, for the Dart drain the next time the app runs.
     *
     * The live Dart sink is invoked ONLY for a queued payload. A payload the
     * phone already wrote would otherwise be written a second time by the
     * Dart side — identical documents, so harmless, but a wasted round trip
     * per text and a second thread transaction racing the first.
     */
    private fun deliver(payloads: List<JSONObject>) {
        if (payloads.isEmpty()) return
        if (!writer.isConfigured()) {
            queue(payloads)
            return
        }
        DeviceTextWriter.executor.execute {
            val outcome = runCatching { writer.writeInbound(payloads) }
                .getOrElse { DeviceTextWriter.Outcome.DEFER }
            if (outcome == DeviceTextWriter.Outcome.DEFER) queue(payloads)
        }
    }

    private fun queue(payloads: List<JSONObject>) {
        for (payload in payloads) {
            store.enqueue(payload)
            sink?.invoke(payload.toMap())
        }
    }

    private fun ingest(sbn: StatusBarNotification) {
        val notification = sbn.notification ?: return
        val style = NotificationCompat.MessagingStyle
            .extractMessagingStyleFromNotification(notification) ?: return

        val messages = style.messages
        if (messages.isEmpty()) return

        // Only the phone's texting app. Anything posts a MessagingStyle —
        // Slack, a Claude prompt and a Life360 alert were all filed as "my
        // phone" threads before the Dart drain learned to drop them. Dropping
        // them HERE keeps them out of the dedup ledger and the queue too. The
        // list is Dart's `kDeviceTextPackages`, pushed down with the config.
        if (sbn.packageName !in allowedPackages()) return

        val conversationKey = conversationKeyFor(sbn, notification, style)
        val isGroup = style.isGroupConversation
        val conversationTitle = style.conversationTitle?.toString()
        val selfName = style.user?.name?.toString()
        val reply = replyActionFor(notification)
        val canReply = reply != null
        // Keep the live reply box whether or not any message below is new:
        // a re-post of an already-captured thread still refreshes the intent.
        if (reply != null) {
            replyTargets[conversationKey] = ReplyTarget(sbn.key, reply.first, reply.second)
        }

        val fresh = ArrayList<JSONObject>()
        for (message in messages) {
            val text = message.text?.toString() ?: continue
            if (text.isEmpty()) continue
            // 🛑 Android 15 redacts a one-time-code text for a listener it does
            // not trust: the Person is stripped and the text replaced with this
            // placeholder. Stripped Person + "from me" convention below meant
            // the OS's own placeholder was filed as a text the MEMBER SENT —
            // three of them live, in two blank-titled threads, on 2026-09-06.
            // It is not a message; drop it here.
            if (text in REDACTED_PLACEHOLDERS) continue

            val senderName = message.person?.name?.toString()
            // MessagingStyle convention: a message the user sent carries a null
            // Person. Capturing our own outgoing messages matters — a thread
            // read as inbound-only reads as a monologue and the agent drafting
            // a reply has no idea what we already said. In practice this only
            // ever sees a reply typed into the notification shade: a reply
            // typed in the messaging app posts no notification at all, which
            // is why sent texts are read from the SMS store instead
            // (SentMessageReader).
            val isFromMe = message.person == null ||
                (selfName != null && senderName == selfName)

            val dedupKey = listOf(
                sbn.packageName,
                conversationKey,
                message.timestamp.toString(),
                senderName ?: "self",
                text.hashCode().toString(),
            ).joinToString("|")

            if (store.hasSeen(dedupKey)) continue
            store.markSeen(dedupKey)

            val payload = JSONObject().apply {
                put("dedupKey", dedupKey)
                put("packageName", sbn.packageName)
                put("conversationKey", conversationKey)
                put("conversationTitle", conversationTitle ?: JSONObject.NULL)
                put("isGroup", isGroup)
                put("senderName", senderName ?: JSONObject.NULL)
                put("isFromMe", isFromMe)
                put("text", text)
                put("timestamp", message.timestamp)
                put("postedAt", sbn.postTime)
                // Whether a RemoteInput reply action was still attached when we
                // saw this. It goes stale the moment the notification is
                // dismissed, so recording it now is the only way to measure how
                // often the direct-reply path would actually have been open.
                put("canReply", canReply)
            }

            fresh.add(payload)
        }
        deliver(fresh)
    }

    private fun allowedPackages(): Set<String> =
        IngestConfig(applicationContext).read()?.packages ?: IngestConfig.DEFAULT_PACKAGES

    /**
     * A stable identity for the thread. `shortcutId` is what Google Messages
     * sets for conversation shortcuts and is the most durable; the notification
     * tag and key both change across re-posts on some OEM skins, so they are
     * last resorts.
     */
    private fun conversationKeyFor(
        sbn: StatusBarNotification,
        notification: Notification,
        style: NotificationCompat.MessagingStyle,
    ): String {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            notification.shortcutId?.takeIf { it.isNotEmpty() }?.let { return it }
        }
        style.conversationTitle?.toString()?.takeIf { it.isNotEmpty() }?.let { return it }
        sbn.tag?.takeIf { it.isNotEmpty() }?.let { return it }
        return sbn.key
    }

    /** The free-form reply action and its input, read through the compat
     *  layer so the same object can be fired back later. */
    private fun replyActionFor(
        notification: Notification,
    ): Pair<NotificationCompat.Action, androidx.core.app.RemoteInput>? {
        val count = NotificationCompat.getActionCount(notification)
        for (i in 0 until count) {
            val action = NotificationCompat.getAction(notification, i) ?: continue
            val input = action.remoteInputs?.firstOrNull { it.allowFreeFormInput } ?: continue
            if (action.actionIntent == null) continue
            return action to input
        }
        return null
    }
}
