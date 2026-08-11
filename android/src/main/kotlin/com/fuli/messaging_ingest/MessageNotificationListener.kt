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
    }

    override fun onCreate() {
        super.onCreate()
        store = PendingStore(applicationContext)
    }

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
    }

    private fun ingest(sbn: StatusBarNotification) {
        val notification = sbn.notification ?: return
        val style = NotificationCompat.MessagingStyle
            .extractMessagingStyleFromNotification(notification) ?: return

        val messages = style.messages
        if (messages.isEmpty()) return

        val conversationKey = conversationKeyFor(sbn, notification, style)
        val isGroup = style.isGroupConversation
        val conversationTitle = style.conversationTitle?.toString()
        val selfName = style.user?.name?.toString()
        val canReply = hasReplyAction(notification)

        for (message in messages) {
            val text = message.text?.toString() ?: continue
            if (text.isEmpty()) continue

            val senderName = message.person?.name?.toString()
            // MessagingStyle convention: a message the user sent carries a null
            // Person. Capturing our own outgoing messages matters — a thread
            // read as inbound-only reads as a monologue and the agent drafting
            // a reply has no idea what we already said.
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

            store.enqueue(payload)
            sink?.invoke(payload.toMap())
        }
    }

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

    private fun hasReplyAction(notification: Notification): Boolean {
        val actions = notification.actions ?: return false
        return actions.any { action ->
            action.remoteInputs?.any { it.allowFreeFormInput } == true
        }
    }
}
