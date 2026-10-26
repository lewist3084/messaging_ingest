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
         * The reply box of each conversation this process has seen a
         * notification for: the RemoteInput action Google Messages attached,
         * keyed on the same conversation key the capture uses. Firing it with
         * text is how a smartwatch or Android Auto answers a thread, and it is
         * the ONLY route into an RCS chat — or a GROUP chat of any kind — that
         * a third-party app has.
         *
         * 🛑 KEPT AFTER THE NOTIFICATION IS DISMISSED. Until 2026-10-14 the
         * entry was dropped in [onNotificationRemoved], on the theory that a
         * dismissed notification's intent is dead. It is not: a PendingIntent
         * is a record in system_server that lives as long as anyone holds it,
         * and Google Messages neither cancels its reply intents on dismissal
         * nor checks for a posted notification when one fires. Dropping the
         * entry meant every group reply typed on the computer more than a few
         * hours after the last text failed with "reply from the phone" — the
         * member had simply read the thread. Now the box is kept, marked
         * [ReplyTarget.dismissedAt], and only the SYSTEM retires it: a
         * `CanceledException` on send is the one proof that it is gone.
         *
         * The map is in memory because a PendingIntent cannot be written to
         * disk (its binder does not marshal). A process restart — app update,
         * a force-stop, memory pressure — therefore empties it, and
         * [rememberReplyTargetsOnShade] refills it from whatever is still
         * posted, both on connect and the first time a key is missed.
         */
        private val replyTargets =
            java.util.concurrent.ConcurrentHashMap<String, ReplyTarget>()

        /** The bound service, while it is connected — what [replyTargetFor]
         *  reads the shade through. */
        @Volatile
        private var instance: MessageNotificationListener? = null

        /** A box last seen posted or dismissed longer ago than this is
         *  forgotten: the thread has had nothing to say for a month. */
        private const val REPLY_TARGET_TTL_MS = 30L * 24 * 60 * 60 * 1000

        internal fun canReplyInline(conversationKey: String): Boolean =
            replyTargetFor(conversationKey) != null

        /** How the box for [conversationKey] stands — for the outbox row's
         *  diagnosis, never for a decision. */
        internal fun replyBoxState(conversationKey: String): String {
            val t = replyTargetFor(conversationKey) ?: return "none"
            return if (t.dismissedAt == 0L) "posted" else "kept"
        }

        private fun replyTargetFor(conversationKey: String): ReplyTarget? {
            if (conversationKey.isEmpty()) return null
            replyTargets[conversationKey]?.let { return it }
            // Not remembered: this process may have started after the
            // notification was posted. The shade still has it if the member
            // has not read the thread.
            instance?.rememberReplyTargetsOnShade()
            return replyTargets[conversationKey]
        }

        /**
         * Answers [conversationKey] through its notification's reply box —
         * posted or kept. False when this phone has no box for it, or the
         * system reports the box dead (the one case the box is dropped);
         * throws if the system refused to fire it for any other reason.
         */
        internal fun replyInline(context: android.content.Context, conversationKey: String, text: String): Boolean {
            val target = replyTargetFor(conversationKey) ?: return false
            val intent = android.content.Intent()
            val results = android.os.Bundle().apply { putCharSequence(target.remoteInput.resultKey, text) }
            androidx.core.app.RemoteInput.addResultsToIntent(arrayOf(target.remoteInput), intent, results)
            // What a keyboard reply says about itself; Messages reads it to
            // tell typed text from a canned choice.
            androidx.core.app.RemoteInput.setResultsSource(
                intent,
                androidx.core.app.RemoteInput.SOURCE_FREE_FORM_INPUT,
            )
            val pending = target.action.actionIntent
                ?: throw IllegalStateException("reply action carries no intent")
            try {
                pending.send(context, 0, intent)
            } catch (e: android.app.PendingIntent.CanceledException) {
                // The messaging app cancelled this intent (it was updated, or
                // it rebuilt its notification with a new one). The box is
                // dead; forget it so the next look reads the shade afresh.
                replyTargets.remove(conversationKey, target)
                return false
            }
            return true
        }
    }

    internal class ReplyTarget(
        val sbnKey: String,
        val action: androidx.core.app.NotificationCompat.Action,
        val remoteInput: androidx.core.app.RemoteInput,
        /** When the notification was last posted or re-posted. */
        val seenAt: Long = System.currentTimeMillis(),
    ) {
        /** 0 while the notification is on the shade; the time it left it
         *  otherwise. The box is kept either way. */
        @Volatile
        var dismissedAt: Long = 0L
    }

    override fun onNotificationRemoved(sbn: StatusBarNotification) {
        // Keyed on conversation, removed by notification: the two are 1:1 for
        // Google Messages. The box stays — see [replyTargets] — and is only
        // marked, so a later send can say which kind of box carried it.
        val now = System.currentTimeMillis()
        for (t in replyTargets.values) {
            if (t.sbnKey == sbn.key && t.dismissedAt == 0L) t.dismissedAt = now
        }
    }

    /**
     * Refills [replyTargets] from every notification still on the shade from
     * the phone's texting app. Called on connect (a process restart has just
     * emptied the map) and when a key is asked for that the map lacks. It
     * records boxes only; it files no message — every text on the shade is
     * either in the seen-ledger already or about to arrive through
     * [onNotificationPosted].
     */
    internal fun rememberReplyTargetsOnShade() {
        // Throws when the listener is not connected; nothing to read then.
        val posted = runCatching { activeNotifications }.getOrNull() ?: return
        val allowed = allowedPackages()
        for (sbn in posted) {
            if (sbn.packageName !in allowed) continue
            runCatching { rememberReplyTarget(sbn) }
        }
        // `Collection.removeIf` is API 24; minSdk is 23.
        val cutoff = System.currentTimeMillis() - REPLY_TARGET_TTL_MS
        val iter = replyTargets.entries.iterator()
        while (iter.hasNext()) {
            val t = iter.next().value
            if (maxOf(t.seenAt, t.dismissedAt) < cutoff) iter.remove()
        }
    }

    /**
     * Records [sbn]'s reply box under its conversation key and returns that
     * key, or null when the notification is not a conversation with a
     * free-form reply. A re-post of a known conversation REPLACES its box:
     * the newest intent is the one the messaging app expects to be fired.
     */
    private fun rememberReplyTarget(sbn: StatusBarNotification): String? {
        val notification = sbn.notification ?: return null
        val style = NotificationCompat.MessagingStyle
            .extractMessagingStyleFromNotification(notification) ?: return null
        val reply = replyActionFor(notification) ?: return null
        val key = conversationKeyFor(sbn, notification, style)
        replyTargets[key] = ReplyTarget(sbn.key, reply.first, reply.second)
        return key
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

    override fun onDestroy() {
        if (instance === this) instance = null
        super.onDestroy()
    }

    override fun onListenerConnected() {
        super.onListenerConnected()
        connected = true
        instance = this
        // A connect follows a process start (or a rebind after an update):
        // the reply boxes are gone from memory and the shade still has them.
        runCatching { rememberReplyTargetsOnShade() }
    }

    override fun onListenerDisconnected() {
        super.onListenerDisconnected()
        connected = false
        instance = null
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
        val partyNumbers = partyNumbersOf(notification, style)
        val reply = replyActionFor(notification)
        val canReply = reply != null
        // Keep the reply box whether or not any message below is new: a
        // re-post of an already-captured thread still refreshes the intent.
        if (reply != null) {
            replyTargets[conversationKey] = ReplyTarget(sbn.key, reply.first, reply.second)
        }

        val fresh = ArrayList<JSONObject>()
        try {
            collectFresh(
                sbn, conversationKey, isGroup, conversationTitle, selfName, canReply,
                partyNumbers, messages, fresh,
            )
        } finally {
            // A conversation that has just posted can be answered again. Any
            // reply the member typed while this phone had no box for it is
            // waiting in the outbox; send it now, AFTER the text that woke
            // the box is filed (same single-thread executor, in order).
            if (reply != null && writer.isConfigured()) {
                DeviceTextWriter.executor.execute {
                    val n = runCatching {
                        writer.sendWaitingReplies(conversationKey) { text ->
                            replyInline(applicationContext, conversationKey, text)
                        }
                    }.onFailure {
                        android.util.Log.w("MessageNotificationListener", "waiting replies failed: ${it.message}")
                    }.getOrDefault(0)
                    if (n > 0) android.util.Log.i("MessageNotificationListener", "sent $n waiting repl(y/ies)")
                }
            }
        }
    }

    /** The messages on [sbn] this process has not filed yet, delivered. */
    private fun collectFresh(
        sbn: StatusBarNotification,
        conversationKey: String,
        isGroup: Boolean,
        conversationTitle: String?,
        selfName: String?,
        canReply: Boolean,
        partyNumbers: List<String>,
        messages: List<NotificationCompat.MessagingStyle.Message>,
        fresh: ArrayList<JSONObject>,
    ) {
        // The sender's number by their CONTACT, looked up once per name per
        // notification — see [contactNumberFor].
        val contactNumbers = HashMap<String, String?>()
        for (message in messages) {
            val text = message.text?.toString().orEmpty()
            // A picture message posts a notification with an EMPTY text and a
            // data URI beside it — which the `?: continue` here used to drop
            // whole, so an incoming photo simply never arrived. Images and
            // contact cards only: anything else the shade can carry has no
            // bubble to render it in. A vCard (RCS from Google Messages may
            // carry one as the data URI) is kept since 2026-10-15 — the
            // writer parses it into a `contact` attachment.
            val dataUri = message.dataUri
            val dataMime = message.dataMimeType?.trim().orEmpty().lowercase()
            val dataAttachmentUri = if (dataMime.startsWith("image/") || VCardParser.isVCardMime(dataMime)) {
                dataUri
            } else {
                null
            }
            if (text.isEmpty() && dataAttachmentUri == null) continue
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
            // The sender's number, when the handset put a `tel:` URI on the
            // Person (2026-10-23). Google Messages mostly does not — the
            // writer's thread index joins a name-only capture to its
            // provider copy by content instead — but when it does, the join
            // is a lookup and the thread learns the number at once.
            // Else (2026-10-26) the number the phone's CONTACTS hold under
            // the sender's name, on a one-to-one thread: an RCS chat with a
            // saved contact never reaches the provider store, so without
            // this a thread titled "Fred Lewis" never learned Fred's number
            // and its card could offer no ⓘ — nothing to file, nothing to
            // junk — while the card beside it, headed by a bare number, could.
            val senderNumber = if (isFromMe) {
                null
            } else {
                telNumber(message.person?.uri)
                    ?: if (isGroup) null else contactNumberFor(senderName, contactNumbers)
            }

            val dedupKey = buildList {
                add(sbn.packageName)
                add(conversationKey)
                add(message.timestamp.toString())
                add(senderName ?: "self")
                add(text.hashCode().toString())
                // 🛑 APPENDED ONLY WHEN THERE IS AN ATTACHMENT (a picture
                // or, since 2026-10-15, a contact card). This key is the
                // Firestore document id (`DartDocId.docIdFor`) and the local
                // seen-ledger's key, so adding a segment unconditionally would
                // re-key every text already captured — every one of them would
                // come back as a NEW document on the next re-post, and the
                // member's threads would double. Two pictures sent in the same
                // second under one caption do need separating, so the segment
                // exists; it just cannot exist for a plain text.
                if (dataAttachmentUri != null) add(dataAttachmentUri.toString().hashCode().toString())
            }.joinToString("|")

            if (store.hasSeen(dedupKey)) continue
            store.markSeen(dedupKey)

            val payload = JSONObject().apply {
                put("dedupKey", dedupKey)
                put("packageName", sbn.packageName)
                put("conversationKey", conversationKey)
                put("conversationTitle", conversationTitle ?: JSONObject.NULL)
                put("isGroup", isGroup)
                put("senderName", senderName ?: JSONObject.NULL)
                put("senderNumber", senderNumber ?: JSONObject.NULL)
                // Every `tel:` the notification named for the thread — the
                // sender's, and the people list's. Empty on most RCS threads.
                put("partyNumbers", org.json.JSONArray(buildList {
                    senderNumber?.let { add(it) }
                    for (n in partyNumbers) if (n !in this) add(n)
                }))
                put("isFromMe", isFromMe)
                put("text", text)
                // The picture or contact card, when there is one. A content URI the shade
                // granted US read on — it is not readable by any other
                // process and it does not survive the notification, so the
                // writer uploads it on the way past rather than storing it.
                put("attachmentUri", dataAttachmentUri?.toString() ?: JSONObject.NULL)
                put("attachmentMime", if (dataAttachmentUri != null) dataMime else JSONObject.NULL)
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
     * The numbers the notification named for its people, in the order the
     * handset listed them: the people list (API 28+) and the older
     * `EXTRA_PEOPLE` strings, `tel:` URIs only. The member's own Person, when
     * the style names one with a number, is left out.
     */
    private fun partyNumbersOf(
        notification: Notification,
        style: NotificationCompat.MessagingStyle,
    ): List<String> {
        val out = ArrayList<String>()
        val self = telNumber(style.user?.uri)
        fun add(uri: String?) {
            val n = telNumber(uri) ?: return
            if (n != self && n !in out) out.add(n)
        }
        val extras = notification.extras ?: return out
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
            runCatching {
                @Suppress("DEPRECATION")
                val people = extras.getParcelableArrayList<android.app.Person>(Notification.EXTRA_PEOPLE_LIST)
                people?.forEach { add(it.uri) }
            }
        }
        runCatching {
            @Suppress("DEPRECATION")
            extras.getStringArray(Notification.EXTRA_PEOPLE)?.forEach { add(it) }
        }
        return out
    }

    /**
     * The one number the phone's contacts hold under [name], or null: no such
     * contact, more than one (never guess which "Chris"), no contacts
     * permission, or a name that is itself a number. [SmsSender.resolveNumber]
     * is the rule — the same one a reply typed in the app uses to find the
     * number behind a name-only thread. Cached per notification in [cache].
     */
    private fun contactNumberFor(name: String?, cache: HashMap<String, String?>): String? {
        val n = name?.trim().orEmpty()
        if (n.isEmpty() || n.startsWith("~") || SmsSender.looksLikeNumber(n)) return null
        return cache.getOrPut(n) {
            runCatching { SmsSender(applicationContext).resolveNumber(n) }.getOrNull()
        }
    }

    /**
     * The number in a `tel:` URI, as the handset would dial it (`+` and
     * digits; separators dropped), or null for anything else — a
     * `mailto:`, a contacts URI, a short code. Mirrors nothing on the
     * Dart side: Dart only ever reads the number this produced.
     */
    private fun telNumber(uri: String?): String? {
        val raw = uri?.trim().orEmpty()
        if (!raw.startsWith("tel:", ignoreCase = true)) return null
        val decoded = runCatching { android.net.Uri.decode(raw.substring(4)) }.getOrDefault(raw.substring(4))
        val sb = StringBuilder()
        for ((i, ch) in decoded.withIndex()) {
            if (ch.isDigit()) sb.append(ch)
            else if (ch == '+' && i == 0) sb.append(ch)
            else if (ch == ' ' || ch == '-' || ch == '(' || ch == ')' || ch == '.') continue
            else return null
        }
        val digits = sb.count { it.isDigit() }
        return if (digits >= 7) sb.toString() else null
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
