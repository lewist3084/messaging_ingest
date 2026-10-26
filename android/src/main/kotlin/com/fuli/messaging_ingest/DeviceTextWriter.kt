package com.fuli.messaging_ingest

import android.content.Context
import android.content.pm.ApplicationInfo
import android.util.Log
import com.google.android.gms.tasks.Task
import com.google.android.gms.tasks.Tasks
import com.google.firebase.FirebaseApp
import com.google.firebase.Timestamp
import com.google.firebase.appcheck.FirebaseAppCheck
import com.google.firebase.appcheck.playintegrity.PlayIntegrityAppCheckProviderFactory
import com.google.firebase.auth.FirebaseAuth
import com.google.firebase.firestore.DocumentReference
import com.google.firebase.firestore.DocumentSnapshot
import com.google.firebase.firestore.FieldValue
import com.google.firebase.firestore.FirebaseFirestore
import com.google.firebase.firestore.FirebaseFirestoreSettings
import com.google.firebase.firestore.PersistentCacheSettings
import com.google.firebase.firestore.Query
import com.google.firebase.firestore.SetOptions
import org.json.JSONObject
import java.util.Date
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * Writes captured texts to Firestore FROM THE PHONE, with no Flutter engine.
 *
 * Until 2026-09-07 the notification listener could only queue a captured text
 * on the handset; the Dart side wrote it to Firestore the next time the app
 * ran — at boot, or when the Texts screen opened. So with the app dead a text
 * that arrived on the phone reached the member's computer hours later, or
 * never. Nothing wakes a dead Flutter engine for an incoming text.
 *
 * The listener service, on the other hand, is bound by the system and alive
 * for days. It runs in the app's main process, where `google-services.json`
 * has already initialised the default `FirebaseApp`, native `FirebaseAuth`
 * has restored the signed-in user from its own store, and the Firestore SDK
 * is in the APK. So the phone writes the documents itself, in exactly the
 * shape `MessageIngestService` (Dart) writes them — same ids, same fields,
 * same advance-only thread transaction — and the Dart drain becomes the
 * fallback for anything this could not write.
 *
 * ── What it needs, and where it comes from ──
 *
 * - WHO and WHERE: [IngestConfig], pushed down by Dart once the membership is
 *   resolved. Without it, nothing is written.
 * - The settings (`deviceTextCaptureOff` and the Limit list): read FRESH off
 *   the member document on every write. Incoming and sent texts have no
 *   switches — both are always written once the phone is set up, and the
 *   old `deviceCaptureIncoming` / `deviceCaptureSent` fields are ignored —
 *   server first, cache when offline — so a switch flipped on the web takes
 *   effect on the phone without the app ever opening.
 * - App Check: the Dart side installs the provider from `BootService`; with
 *   no engine nobody has, so a release build installs Play Integrity here
 *   before the first request. Harmless when enforcement is off; the difference
 *   between PERMISSION_DENIED and a write when it is on.
 *
 * ── Two SDK traps, both handled ──
 *
 * 🛑 `FirebaseFirestore.setFirestoreSettings` THROWS if the instance has been
 * used with DIFFERENT settings. The Dart side sets a 50 MB persistent cache on
 * its first access; if this process had already started Firestore with the
 * SDK default, opening the app afterwards would crash cloud_firestore's
 * plugin. So the phone starts Firestore with the SAME settings the Dart side
 * will apply — the size travels in [IngestConfig.Values.cacheSizeBytes] — and
 * equal settings are accepted in either order.
 *
 * 🛑 A transaction needs the network; a batch does not. A batched message
 * write is durable in the local SQLite queue the moment `commit()` returns its
 * Task, and drains when the phone is next online. The thread-preview
 * transaction fails offline. So on ANY failure past the batch, the payload is
 * handed back to the pending queue and the Dart drain redoes both — every
 * write here is idempotent, so a redo costs nothing but a read.
 */
internal class DeviceTextWriter(private val context: Context) {

    companion object {
        /** [learnContactNumbers] has run in this process. */
        @Volatile private var contactsSwept = false

        private const val TAG = "DeviceTextWriter"

        /** One thread: writes are serialised, and Firestore's own worker does
         *  the I/O. `Tasks.await` must never run on the main thread. */
        val executor = Executors.newSingleThreadExecutor { r ->
            Thread(r, "messaging-ingest-writer").apply { isDaemon = true }
        }

        private const val COL_COMPANY = "company"
        private const val COL_MEMBER = "member"
        private const val COL_CONVERSATION = "textConversation"
        private const val COL_MESSAGE = "message"

        /** A reply typed elsewhere for this phone to send — `DeviceOutboxService`. */
        private const val COL_OUTBOX = "deviceOutbox"

        /** A group reply the Dart side could not send because this phone had
         *  no reply box for the thread; [sendWaitingReplies] sends it when
         *  the thread next posts. */
        private const val STATUS_WAITING = "waiting"

        /** How long a waiting reply is still the member's words. */
        private const val WAITING_MAX_AGE_MS = 24L * 60 * 60 * 1000

        private const val FIELD_CAPTURE_OFF = "deviceTextCaptureOff"

        /** A batch commit is durable locally as soon as it is enqueued; this
         *  is how long we wait for the SERVER before moving on. */
        private const val WRITE_TIMEOUT_S = 25L

        /** Sent-text paging, the same constants as the Dart side. */
        private const val SENT_PAGE_SIZE = 300

        /** Set on a thread once its phone history has been pulled. */
        private const val FIELD_HISTORY_PULLED_AT = "deviceHistoryPulledAt"

        /** The most of ONE conversation's past a pull files. */
        private const val HISTORY_LIMIT = 300

        /** A history row must be at least this much older than the text that
         *  woke the pull, so the notification's own copy of that text is not
         *  filed a second time from the provider (whose clock differs). */
        private const val HISTORY_MARGIN_MS = 1000L
        private const val SENT_PAGES_PER_SYNC = 4

        @Volatile private var appCheckInstalled = false
        @Volatile private var settingsApplied = false
    }

    private val config = IngestConfig(context)

    /** What one write runs against. Null when this process cannot write. */
    private class Session(
        val db: FirebaseFirestore,
        val company: DocumentReference,
        val memberRef: DocumentReference,
        val memberName: String,
        val packages: Set<String>,
        val captureOff: Boolean,
        /** [DirectoryCapture.EVERYONE] or [DirectoryCapture.DIRECTORY]. */
        val incomingScope: String,
    ) {
        /** The directory, asked once per party — [DirectoryCapture] caches
         *  across sessions, so this is only the handle. */
        val directory: DirectoryCapture get() = DirectoryCapture(company)
    }

    /** The outcome of handing a capture to the phone's own writer. */
    enum class Outcome {
        /** Written (or dropped by a switch, which is also final). */
        HANDLED,

        /** Could not write from here; the caller keeps it for the Dart drain. */
        DEFER,
    }

    fun isConfigured(): Boolean = config.isConfigured()

    // ── Session ─────────────────────────────────────────────────────────────

    private fun session(): Session? {
        val cfg = config.read() ?: return null
        if (FirebaseApp.getApps(context).isEmpty()) {
            Log.w(TAG, "no FirebaseApp in this process; deferring")
            return null
        }
        val user = FirebaseAuth.getInstance().currentUser
        if (user == null || user.uid != cfg.uid) {
            // Signed out, or a different sign-in than the one configured. A
            // stale config must never write as somebody else.
            return null
        }
        installAppCheck()
        val db = firestore(cfg.cacheSizeBytes)
        val company = db.collection(COL_COMPANY).document(cfg.companyId)
        val memberRef = company.collection(COL_MEMBER).document(cfg.memberId)

        // The switches, and the name, off the live member document. `get()`
        // goes to the server and falls back to the cache offline; either way
        // it is the newest this phone can know.
        val member: DocumentSnapshot? = runCatching { await(memberRef.get()) }
            .onFailure { Log.w(TAG, "member read failed: ${it.message}") }
            .getOrNull()
        val data = member?.data ?: emptyMap<String, Any?>()
        val name = memberNameFrom(data).ifBlank { cfg.memberName }.ifBlank { "Me" }
        return Session(
            db = db,
            company = company,
            memberRef = memberRef,
            memberName = name,
            packages = cfg.packages,
            captureOff = data[FIELD_CAPTURE_OFF] == true,
            incomingScope = DirectoryCapture.scopeOf(data),
        )
    }

    /** `name`, else `firstName lastName` — the same rule as the Dart side. */
    private fun memberNameFrom(data: Map<String, Any?>): String {
        val name = (data["name"] as? String)?.trim().orEmpty()
        if (name.isNotEmpty()) return name
        val first = (data["firstName"] as? String)?.trim().orEmpty()
        val last = (data["lastName"] as? String)?.trim().orEmpty()
        return "$first $last".trim()
    }

    private fun firestore(cacheSizeBytes: Long): FirebaseFirestore {
        val db = FirebaseFirestore.getInstance()
        if (!settingsApplied) {
            settingsApplied = true
            // Must EQUAL what cloud_firestore will set from Dart, or whichever
            // side comes second throws — see the class comment. When the Dart
            // side got here first the instance already carries these and the
            // assignment is a no-op; when it has diverged (a Dart-side change
            // not mirrored here) the throw is swallowed and the running
            // settings stand.
            runCatching {
                db.firestoreSettings = FirebaseFirestoreSettings.Builder()
                    .setLocalCacheSettings(
                        PersistentCacheSettings.newBuilder()
                            .setSizeBytes(cacheSizeBytes)
                            .build(),
                    )
                    .build()
            }.onFailure { Log.w(TAG, "firestore settings not applied: ${it.message}") }
        }
        return db
    }

    private fun installAppCheck() {
        if (appCheckInstalled) return
        appCheckInstalled = true
        val debuggable =
            (context.applicationInfo.flags and ApplicationInfo.FLAG_DEBUGGABLE) != 0
        // Release only, matching BootService's `ENABLE_APP_CHECK` default of
        // kReleaseMode. A debug build's debug provider needs a token registered
        // in the console and is the Dart side's call.
        if (debuggable) return
        runCatching {
            FirebaseAppCheck.getInstance().installAppCheckProviderFactory(
                PlayIntegrityAppCheckProviderFactory.getInstance(),
            )
        }.onFailure { Log.w(TAG, "App Check install failed: ${it.message}") }
    }

    // ── Inbound (notification) captures ─────────────────────────────────────

    /**
     * Writes a batch of captured notification messages — the payloads the
     * listener builds — as the member. Runs on [executor]; blocking.
     *
     * Mirrors `MessageIngestService._persistAll`: one merge-set per message
     * keyed on the dedup key, then the newest message per thread advances the
     * thread document.
     */
    fun writeInbound(payloads: List<JSONObject>): Outcome {
        if (payloads.isEmpty()) return Outcome.HANDLED
        val s = session() ?: return Outcome.DEFER
        // Master off: the Dart side leaves these queued, and so do we.
        if (s.captureOff) return Outcome.DEFER

        val batch = s.db.batch()
        val newestPerThread = HashMap<String, JSONObject>()
        // What each thread already holds — see [ThreadDedupe].
        val dedupers = HashMap<String, ThreadDedupe>()
        var merged = 0
        // Preview line per message, keyed on dedup key, so a picture with no
        // caption advances the thread as "📷 Photo" rather than as blank.
        val previews = HashMap<String, String>()
        val learning = ThreadLearning()
        // 🛑 Loaded lazily, and the message is NOT filed without it. Until
        // 2026-09-17 this path minted `device_<conversationKey>` without
        // looking, so a reply to a text the member had SENT (filed by
        // syncSent as `device_sms_<threadId>`) opened a second thread for
        // the same people, and every message after it landed in both.
        var index: DeviceThreadIndex? = null
        var count = 0
        for (m in payloads) {
            val packageName = m.optString("packageName")
            if (packageName !in s.packages) continue
            // Outside the member's scope (not in the directory): dropped,
            // final. Undecidable (the directory could not be asked): the
            // whole payload is deferred to the Dart drain, nothing guessed.
            when (inScope(s, notificationParties(m))) {
                null -> return Outcome.DEFER
                false -> continue
                true -> {}
            }
            val conversationKey = m.optString("conversationKey")
            val dedupKey = m.optString("dedupKey")
            if (conversationKey.isEmpty() || dedupKey.isEmpty()) continue
            val idx = index ?: loadThreadIndex(s)?.also { index = it }
                ?: return Outcome.DEFER
            val isGroup = m.optBoolean("isGroup", false)
            val numbers = notificationNumbers(m)
            var threadId = idx.resolveInbound(conversationKey, threadTitle(m), isGroup, numbers)
            val fromMe = m.optBoolean("isFromMe", false)
            val senderName = m.optStringOrNull("senderName")
            val timestamp = m.optLong("timestamp", 0L)
            val postedAt = m.optLong("postedAt", timestamp)

            val messageDocId = DartDocId.docIdFor(dedupKey)
            val text = m.optString("text")
            // A picture (or a contact card) arrives as a notification data
            // URI beside an empty text. The grant on it dies with the
            // notification, so it is copied to Storage below — there is no
            // later. Named BEFORE the twin check, because a card may be the
            // one thing the twin is missing.
            val attachmentUri = m.optStringOrNull("attachmentUri").orEmpty()
            val attachmentMime = m.optStringOrNull("attachmentMime").orEmpty()
            val stem = if (VCardParser.isVCardMime(attachmentMime)) "contact" else "photo"
            val parts: List<Map<String, Any?>> = if (attachmentUri.isEmpty()) emptyList() else listOf(
                mapOf(
                    "uri" to attachmentUri,
                    "mimeType" to attachmentMime,
                    "fileName" to "$stem-$timestamp" +
                        SentMessageReader.extensionFor(attachmentMime),
                )
            )
            val hasCard = partsCarryCard(parts)
            // 🛑 THE SAME PERSON, FILED TWICE (2026-10-23). A sender who is not
            // in the phone's contacts reaches this path as a NAME alone
            // (`~Carrie Folsom`) and the provider path as a NUMBER alone, so
            // no key could join them and each minted its own thread. When no
            // key matched and this would be a new 1:1 thread, ask whether
            // another thread active in the window already holds this very
            // text from the other path — see DeviceThreadIndex.threadHoldingTwin.
            if (idx.isNew(threadId) && !isGroup) {
                idx.threadHoldingTwin(threadId, timestamp, numbers) { candidate ->
                    dedupers.getOrPut(candidate) { ThreadDedupe.recent(conversationRef(s, candidate)) }
                        .hasTwin(dedupKey, fromMe, text, timestamp, hasCard)
                }?.let { into ->
                    idx.adopt(threadId, into)
                    threadId = into
                }
            }
            val threadRef = conversationRef(s, threadId)
            // What this capture knew about the other party, for the thread
            // to keep: the numbers the handset named, the name it wore.
            learning.note(
                threadId,
                idx.learn(
                    threadId,
                    numbers + listOfNotNull(threadTitle(m), senderName.takeIf { !fromMe }),
                    isGroup,
                ),
            )
            // 🛑 THE SAME TEXT, CAPTURED TWICE. This path keys on the
            // notification (`<package>|<conv>|<ts>|<sender>|<hash>`) and the
            // provider path keys on the SMS/MMS row id (`mms|37026`), so
            // neither could ever see the other's copy — and both capture an
            // outgoing text. Ask the thread what it already holds.
            val dedupe = dedupers.getOrPut(threadId) {
                ThreadDedupe.recent(threadRef)
            }
            val twin = dedupe.duplicateOf(dedupKey, fromMe, text, timestamp, hasCard)
            if (twin != null) {
                val fold = HashMap<String, Any?>()
                // So the next sweep matches on the KEY and never has to
                // guess from text and time again.
                fold["deviceDedupKeyAlt"] = FieldValue.arrayUnion(dedupKey)
                fold["devicePackageName"] = packageName
                // The one thing only this path knows: whether the
                // notification can still be replied to.
                fold["canReply"] = m.optBoolean("canReply", false)
                fold.putAll(cardUpgrade(dedupe, twin, parts, s.company.id))
                batch.set(
                    threadRef.collection(COL_MESSAGE).document(twin),
                    fold,
                    SetOptions.merge(),
                )
                merged++
                continue
            }
            val attachments = uploadedAttachments(parts, s.company.id, messageDocId)
            // "Contact card" beside the card itself is the sender's stand-in,
            // not a caption (2026-10-15): the bubble is the card alone.
            val filedText = textBeside(text, attachments)

            val doc = HashMap<String, Any?>()
            doc["text"] = filedText
            doc["senderRef"] = if (fromMe) s.memberRef else null
            doc["senderName"] = if (fromMe) s.memberName else (senderName ?: "Unknown")
            // The time it was SENT, not the time it reached Firestore.
            doc["createdAt"] = Timestamp(Date(timestamp))
            // Unread until the member says otherwise — the rail node on the
            // timeline, opening the thread, or replying. A notification on
            // the phone is not proof it was read. Their own text is read.
            doc["readByMemberIds"] = if (fromMe) listOf(s.memberRef.id) else emptyList()
            doc["visibleToMemberIds"] = listOf(s.memberRef.id)
            doc["attachments"] = attachments
            doc["isDeleted"] = false
            doc["source"] = "device"
            doc["capturedAt"] = Timestamp(Date(postedAt))
            doc["devicePackageName"] = packageName
            doc["deviceDedupKey"] = dedupKey
            doc["canReply"] = m.optBoolean("canReply", false)
            doc["ingestedAt"] = FieldValue.serverTimestamp()
            batch.set(
                threadRef.collection(COL_MESSAGE).document(messageDocId),
                doc,
                SetOptions.merge(),
            )
            count++
            dedupe.record(messageDocId, dedupKey, fromMe, filedText, timestamp, attachmentsCarryCard(attachments))
            previews[dedupKey] = previewText(filedText, attachments)

            val incumbent = newestPerThread[threadId]
            if (incumbent == null || timestamp > incumbent.optLong("timestamp", 0L)) {
                newestPerThread[threadId] = m
            }
        }
        // Nothing new, but a merge may still be waiting to be written.
        if (count == 0 && merged == 0) return Outcome.HANDLED

        try {
            await(batch.commit())
        } catch (e: TimeoutException) {
            // Enqueued locally; the server will see it when the phone is
            // online. The thread advance below cannot run offline, so defer
            // the whole payload and let the Dart drain finish the job.
            Log.w(TAG, "batch commit still pending after ${WRITE_TIMEOUT_S}s; deferring")
            return Outcome.DEFER
        } catch (e: Exception) {
            Log.w(TAG, "batch commit failed: ${e.message}")
            return Outcome.DEFER
        }

        var allAdvanced = true
        for ((threadId, m) in newestPerThread) {
            val fromMe = m.optBoolean("isFromMe", false)
            val senderName = m.optStringOrNull("senderName")
            val ok = advanceThread(
                s,
                threadId,
                title = threadTitle(m),
                isGroup = m.optBoolean("isGroup", false),
                text = previews[m.optString("dedupKey")] ?: m.optString("text"),
                senderName = if (fromMe) s.memberName else (senderName ?: "Unknown"),
                atMillis = m.optLong("timestamp", 0L),
                extra = withLearned(
                    mapOf(
                        "devicePackageName" to m.optString("packageName"),
                        "deviceConversationKey" to m.optString("conversationKey"),
                    ),
                    learning.take(threadId),
                ),
            )
            if (!ok) allAdvanced = false
        }
        learning.flush(s)
        // A conversation that has just come alive brings its past with it —
        // once. A group notification names no number to find it by.
        for ((threadId, m) in newestPerThread) {
            if (m.optBoolean("isGroup", false)) continue
            runCatching {
                pullHistory(
                    s,
                    threadId,
                    handsetThreadId = null,
                    beforeMillis = m.optLong("timestamp", 0L) - HISTORY_MARGIN_MS,
                    // The number when the handset named one — an unsaved
                    // sender's past was unreachable by name alone.
                    whoForLookup = notificationNumbers(m).firstOrNull() ?: threadTitle(m),
                )
            }.onFailure { Log.w(TAG, "history pull failed for $threadId: ${it.message}") }
        }
        return if (allAdvanced) Outcome.HANDLED else Outcome.DEFER
    }

    /**
     * The name a captured thread wears: the handset's conversation title when
     * it has one, else the other party. Blank counts as absent, and so does
     * our own name — an outgoing message names nobody.
     */
    /**
     * Whether a capture with these PARTIES comes in under the member's scope
     * (`deviceIncomingScope`): true / false, or NULL when the directory could
     * not be asked — the caller then leaves the capture where it is (defers
     * the payload, or stops the page before its watermark) rather than
     * dropping or admitting it on a guess. Everyone ⇒ true, no read.
     *
     * Mirrors `MessageIngestService._inScope` — keep them equal.
     */
    private fun inScope(s: Session, parties: List<String?>): Boolean? {
        if (s.incomingScope != DirectoryCapture.DIRECTORY) return true
        return try {
            s.directory.knowsAny(parties) { name -> SmsSender(context).resolveNumber(name) }
        } catch (e: Exception) {
            Log.w(TAG, "directory lookup failed: ${e.message}")
            null
        }
    }

    /** The parties to a captured NOTIFICATION: the sender, and whoever the
     *  handset titled the conversation with (a group's names, comma-separated).
     *  Mirrors `_notificationParties` in Dart. */
    private fun notificationParties(m: JSONObject): List<String?> {
        val out = ArrayList<String?>()
        out.add(m.optStringOrNull("senderName"))
        m.optStringOrNull("conversationTitle")?.split(',')?.let { out.addAll(it) }
        // The numbers the handset named (2026-10-23): a directory that holds
        // the number and not the RCS name still admits the text.
        out.addAll(notificationNumbers(m))
        return out
    }

    private fun threadTitle(m: JSONObject): String? {
        val title = m.optStringOrNull("conversationTitle")?.trim().orEmpty()
        if (title.isNotEmpty()) return title
        val sender = m.optStringOrNull("senderName")?.trim().orEmpty()
        if (sender.isNotEmpty() && !m.optBoolean("isFromMe", false)) return sender
        return null
    }

    // ── Sent texts (the handset's SMS/MMS store) ────────────────────────────

    /**
     * Texts the member SENT, read from the provider under READ_SMS and filed
     * into the thread they answered. Mirrors `MessageIngestService._syncSent`
     * page for page: the native watermark advances only after the batch has
     * committed, so an interrupted sync re-reads rather than loses.
     *
     * Returns how many were written; 0 without the grant, the config, the
     * switch, or anything new. Runs on [executor]; blocking.
     */
    fun syncSent(): Int {
        val reader = SentMessageReader(context)
        if (!reader.isGranted()) return 0
        val s = session() ?: return 0
        if (s.captureOff) return 0

        var index: DeviceThreadIndex? = null
        var written = 0
        for (page in 0 until SENT_PAGES_PER_SYNC) {
            val watermark = reader.watermark()
            // 🛑 NO history import: the first sync on a phone starts at NOW, the
            // same as the Dart side. Only texts sent from here on are filed.
            if (watermark <= 0L) {
                reader.ack(System.currentTimeMillis())
                break
            }
            val since = watermark
            val rows = runCatching { reader.read(since, SENT_PAGE_SIZE) }.getOrElse { emptyList() }
            if (rows.isEmpty()) break

            // Watermark untouched on failure: the next sync re-reads.
            val idx = index ?: loadThreadIndex(s)?.also { index = it } ?: break
            val batch = s.db.batch()
            val newestPerThread = HashMap<String, SentRow>()
            val oldestPerThread = HashMap<String, SentRow>()
            val dedupers = HashMap<String, ThreadDedupe>()
            var merged = 0
            // Preview line per message, so the thread advance below can show
            // "📷 Photo" for a picture that carried no caption.
            val previews = HashMap<String, String>()
            val learning = ThreadLearning()
            var newest = 0L
            var count = 0
            // The directory could not be asked about a row: this page is
            // neither committed nor acked, and the next sync re-reads it.
            var undecided = false
            for (raw in rows) {
                val row = SentRow.from(raw) ?: continue
                // Past a text the scope keeps out, so it is not re-read forever.
                if (row.timestamp > newest) newest = row.timestamp
                when (inScope(s, row.names + row.addresses)) {
                    null -> { undecided = true; break }
                    false -> continue
                    true -> {}
                }
                var threadId = idx.resolveSent(row.threadId, row.addresses, row.names)
                val hasCard = partsCarryCard(row.attachments)
                // A number with no thread of its own yet: the member's text
                // may already sit in the thread the notification NAMED for
                // the same person (an unsaved sender) — see the inbound path.
                if (idx.isNew(threadId) && row.addresses.size == 1) {
                    idx.threadHoldingTwin(threadId, row.timestamp, row.addresses) { candidate ->
                        dedupers.getOrPut(candidate) { ThreadDedupe.recent(conversationRef(s, candidate)) }
                            .hasTwin(row.dedupKey, true, row.text, row.timestamp, hasCard)
                    }?.let { into ->
                        idx.adopt(threadId, into)
                        threadId = into
                    }
                }
                val threadRef = conversationRef(s, threadId)
                learning.note(
                    threadId,
                    idx.learn(threadId, row.addresses + row.names, row.addresses.size > 1),
                    handsetThreadId = row.threadId,
                )
                val messageDocId = DartDocId.docIdFor(row.dedupKey)
                // The notification listener has usually already filed this
                // very text — see the note on the inbound path.
                val dedupe = dedupers.getOrPut(threadId) {
                    ThreadDedupe.recent(threadRef)
                }
                val twin = dedupe.duplicateOf(row.dedupKey, true, row.text, row.timestamp, hasCard)
                if (twin != null) {
                    val fold = HashMap<String, Any?>()
                    fold["deviceDedupKeyAlt"] = FieldValue.arrayUnion(row.dedupKey)
                    // What only the provider knows: which handset thread
                    // this is, the numbers it went to — and the contact card
                    // a notification copy could not carry.
                    fold["deviceKind"] = row.kind
                    fold["deviceSmsThreadId"] = row.threadId
                    fold["deviceRecipients"] = row.addresses
                    fold.putAll(cardUpgrade(dedupe, twin, row.attachments, s.company.id))
                    batch.set(
                        threadRef.collection(COL_MESSAGE).document(twin),
                        fold,
                        SetOptions.merge(),
                    )
                    merged++
                    continue
                }
                val attachments =
                    uploadedAttachments(row.attachments, s.company.id, messageDocId)
                val filedText = textBeside(row.text, attachments)
                val doc = HashMap<String, Any?>()
                doc["text"] = filedText
                doc["senderRef"] = s.memberRef
                doc["senderName"] = s.memberName
                doc["createdAt"] = Timestamp(Date(row.timestamp))
                doc["readByMemberIds"] = listOf(s.memberRef.id)
                doc["visibleToMemberIds"] = listOf(s.memberRef.id)
                doc["attachments"] = attachments
                doc["isDeleted"] = false
                doc["source"] = "device"
                doc["deviceKind"] = row.kind
                doc["deviceSmsThreadId"] = row.threadId
                doc["deviceRecipients"] = row.addresses
                doc["capturedAt"] = Timestamp.now()
                doc["deviceDedupKey"] = row.dedupKey
                doc["ingestedAt"] = FieldValue.serverTimestamp()
                batch.set(
                    threadRef.collection(COL_MESSAGE).document(messageDocId),
                    doc,
                    SetOptions.merge(),
                )
                count++
                dedupe.record(
                    messageDocId, row.dedupKey, true, filedText, row.timestamp,
                    attachmentsCarryCard(attachments),
                )
                previews[row.dedupKey] = previewText(filedText, attachments)
                val incumbent = newestPerThread[threadId]
                if (incumbent == null || row.timestamp > incumbent.timestamp) {
                    newestPerThread[threadId] = row
                }
                val earliest = oldestPerThread[threadId]
                if (earliest == null || row.timestamp < earliest.timestamp) {
                    oldestPerThread[threadId] = row
                }
                if (row.timestamp > newest) newest = row.timestamp
            }
            if (undecided) break
            if (count == 0 && merged == 0) {
                // Nothing usable on this page (blank texts, no recipients);
                // move the watermark past it or the same page returns forever.
                val last = rows.maxOfOrNull { (it["timestamp"] as? Long) ?: 0L } ?: 0L
                if (last > 0L) reader.ack(last)
                if (rows.size < SENT_PAGE_SIZE) break else continue
            }

            try {
                await(batch.commit())
            } catch (e: Exception) {
                Log.w(TAG, "sent batch commit failed: ${e.message}")
                // Watermark untouched: the next sync re-reads this page.
                break
            }
            written += count

            for ((threadId, row) in newestPerThread) {
                advanceThread(
                    s,
                    threadId,
                    // A sent text never renames a thread the inbound side
                    // named; it only names one it had to create.
                    title = if (idx.isNew(threadId)) sentTitle(row) else null,
                    isGroup = row.addresses.size > 1,
                    text = previews[row.dedupKey] ?: row.text,
                    senderName = s.memberName,
                    atMillis = row.timestamp,
                    extra = withLearned(
                        mapOf(
                            "deviceSmsThreadId" to row.threadId,
                            "deviceRecipients" to row.addresses,
                        ),
                        learning.take(threadId),
                    ),
                )
            }
            learning.flush(s)

            // Only now: the batch is durable.
            if (newest > 0L) reader.ack(newest)
            // Each conversation written to brings its past with it — once.
            for ((threadId, row) in oldestPerThread) {
                runCatching {
                    pullHistory(
                        s,
                        threadId,
                        handsetThreadId = row.threadId,
                        beforeMillis = row.timestamp,
                        whoForLookup = null,
                    )
                }.onFailure { Log.w(TAG, "history pull failed for $threadId: ${it.message}") }
            }
            if (rows.size < SENT_PAGE_SIZE) break
        }
        return written
    }

    /**
     * One pass over the handset's store in BOTH directions — what the
     * provider's change notification and the Dart side's sync both call.
     * Returns how many documents were written.
     */
    fun syncStore(): Int {
        val sent = runCatching { syncSent() }
            .onFailure { Log.w(TAG, "sent sync failed: ${it.message}") }
            .getOrDefault(0)
        val inbox = runCatching { syncInbox() }
            .onFailure { Log.w(TAG, "inbox sync failed: ${it.message}") }
            .getOrDefault(0)
        runCatching { learnContactNumbers() }
            .onFailure { Log.w(TAG, "contact-number sweep failed: ${it.message}") }
        return sent + inbox
    }

    /**
     * Teaches every one-to-one thread titled with a CONTACT's name the
     * number the phone's contacts hold under it (2026-10-26), once per
     * thread. The listener now learns it on each new text; this is for the
     * threads that were filed before it did — an RCS chat with a saved
     * contact never reaches the provider store, so nothing else would ever
     * teach them. A thread with a number already, a group, a thread headed
     * by digits, and a thread already looked at (`deviceNumberLookupAt`) are
     * left alone; a name the contacts hold twice, or not at all, is stamped
     * looked-at with no number — never guessed. Once per process, so a
     * phone that syncs every few minutes reads its threads for this once.
     */
    private fun learnContactNumbers() {
        if (contactsSwept) return
        val reader = SentMessageReader(context)
        if (!reader.contactsGranted()) return
        val s = session() ?: return
        if (s.captureOff) return
        // list-cap: 500. One member's own phone; same cap as the thread index.
        val snap = await(
            s.company.collection(COL_CONVERSATION)
                .whereEqualTo("channel", "device")
                .whereArrayContains("participantRefs", s.memberRef)
                .limit(500)
                .get(),
        )
        contactsSwept = true
        val sender = SmsSender(context)
        var batch = s.db.batch()
        var pending = 0
        for (doc in snap.documents) {
            if (doc.get("isGroup") == true) continue
            if (doc.get("deviceNumberLookupAt") != null) continue
            if (stringsOf(doc.get("deviceRecipients")).isNotEmpty()) continue
            val title = (doc.get("title") as? String)?.trim().orEmpty()
            if (title.isEmpty() || title.startsWith("~") || SmsSender.looksLikeNumber(title)) continue
            val number = runCatching { sender.resolveNumber(title) }.getOrNull()
            val fields = HashMap<String, Any?>()
            fields["deviceNumberLookupAt"] = FieldValue.serverTimestamp()
            if (number != null) fields["deviceRecipients"] = FieldValue.arrayUnion(number)
            batch.set(doc.reference, fields, SetOptions.merge())
            if (++pending >= 400) {
                await(batch.commit())
                batch = s.db.batch()
                pending = 0
            }
        }
        if (pending > 0) await(batch.commit())
    }

    /**
     * Texts that ARRIVED, read from the same store — see
     * [SentMessageReader.readInbox] for why the notification listener alone
     * is not enough: Google Messages posts nothing for the conversation that
     * is OPEN on the phone, so a reply that came in while the member was
     * reading that thread on the handset was lost (2026-10-13). An SMS or
     * MMS lands in the provider whatever is on screen.
     *
     * Same shape as [syncSent]: its own watermark, acked only after the batch
     * is durable; NO history on the first run; the member's Limit list,
     * matched on the SENDER's name and number; and [ThreadDedupe], so the
     * copy the listener filed a second earlier (when it did) is the one that
     * survives, with this row's key folded in.
     *
     * Unread (`readByMemberIds` empty), like every captured inbound text
     * since 2026-10-10: a phone notification is not a read, and neither is
     * the provider row. A text from a SHORT CODE or an alphanumeric sender
     * (a one-time code, a carrier notice) is never filed — not a
     * conversation, and the listener drops the OS's redacted placeholder for
     * the same texts on purpose.
     *
     * 🛑 An RCS reply to an open thread is still invisible: no store holds it.
     */
    fun syncInbox(): Int {
        val reader = SentMessageReader(context)
        if (!reader.isGranted()) return 0
        val s = session() ?: return 0
        if (s.captureOff) return 0

        var index: DeviceThreadIndex? = null
        var written = 0
        for (page in 0 until SENT_PAGES_PER_SYNC) {
            val watermark = reader.inboxWatermark()
            // 🛑 NO history import, and the sent watermark is not borrowed —
            // see KEY_INBOX_WATERMARK. Only texts arriving from here on.
            if (watermark <= 0L) {
                reader.ackInbox(System.currentTimeMillis())
                break
            }
            val rows = runCatching { reader.readInbox(watermark, SENT_PAGE_SIZE) }
                .getOrElse { emptyList() }
            if (rows.isEmpty()) break

            val idx = index ?: loadThreadIndex(s)?.also { index = it } ?: break
            val batch = s.db.batch()
            val newestPerThread = HashMap<String, InboxRow>()
            val dedupers = HashMap<String, ThreadDedupe>()
            val previews = HashMap<String, String>()
            val learning = ThreadLearning()
            var merged = 0
            var newest = 0L
            var count = 0
            // See syncSent: an undecidable row stops the page before its ack.
            var undecided = false
            for (raw in rows) {
                // Past a row we will never file (blank, a short code, kept out
                // by the scope, a group we cannot place), so it is not re-read.
                (raw["timestamp"] as? Long)?.let { if (it > newest) newest = it }
                val row = InboxRow.from(raw) ?: continue
                // The sender, by contact name and by number — the store path's
                // half of what the listener matches on the notification.
                when (inScope(s, listOf(row.senderName, row.sender))) {
                    null -> { undecided = true; break }
                    false -> continue
                    true -> {}
                }
                var threadId = idx.resolveInbox(row.threadId, row.sender, row.displayName, row.others)
                    ?: continue
                val hasCard = partsCarryCard(row.attachments)
                // An unsaved sender: the listener may have filed this very text
                // a second ago under the sender's RCS NAME, with no number to
                // meet this row on — see the inbound path.
                if (idx.isNew(threadId)) {
                    idx.threadHoldingTwin(threadId, row.timestamp, listOf(row.sender)) { candidate ->
                        dedupers.getOrPut(candidate) { ThreadDedupe.recent(conversationRef(s, candidate)) }
                            .hasTwin(row.dedupKey, false, row.text, row.timestamp, hasCard)
                    }?.let { into ->
                        idx.adopt(threadId, into)
                        threadId = into
                    }
                }
                val threadRef = conversationRef(s, threadId)
                learning.note(
                    threadId,
                    idx.learn(threadId, listOf(row.sender, row.senderName), false),
                    handsetThreadId = row.threadId,
                )
                val messageDocId = DartDocId.docIdFor(row.dedupKey)
                // The listener usually filed this very text a second ago —
                // the same twin rule as the sent side, the other direction.
                val dedupe = dedupers.getOrPut(threadId) { ThreadDedupe.recent(threadRef) }
                val twin = dedupe.duplicateOf(row.dedupKey, false, row.text, row.timestamp, hasCard)
                if (twin != null) {
                    val fold = HashMap<String, Any?>()
                    fold["deviceDedupKeyAlt"] = FieldValue.arrayUnion(row.dedupKey)
                    fold["deviceKind"] = row.kind
                    fold["deviceSmsThreadId"] = row.threadId
                    fold["deviceSender"] = row.sender
                    // The 2026-10-14 case: the notification filed "Contact
                    // card" with no data URI, and this MMS row IS the card.
                    fold.putAll(cardUpgrade(dedupe, twin, row.attachments, s.company.id))
                    batch.set(
                        threadRef.collection(COL_MESSAGE).document(twin),
                        fold,
                        SetOptions.merge(),
                    )
                    merged++
                    continue
                }
                val attachments =
                    uploadedAttachments(row.attachments, s.company.id, messageDocId)
                val filedText = textBeside(row.text, attachments)
                val doc = HashMap<String, Any?>()
                doc["text"] = filedText
                doc["senderRef"] = null
                doc["senderName"] = row.displayName
                doc["createdAt"] = Timestamp(Date(row.timestamp))
                doc["readByMemberIds"] = emptyList<String>()
                doc["visibleToMemberIds"] = listOf(s.memberRef.id)
                doc["attachments"] = attachments
                doc["isDeleted"] = false
                doc["source"] = "device"
                doc["deviceKind"] = row.kind
                doc["deviceSmsThreadId"] = row.threadId
                doc["deviceSender"] = row.sender
                doc["capturedAt"] = Timestamp.now()
                doc["deviceDedupKey"] = row.dedupKey
                doc["ingestedAt"] = FieldValue.serverTimestamp()
                batch.set(
                    threadRef.collection(COL_MESSAGE).document(messageDocId),
                    doc,
                    SetOptions.merge(),
                )
                count++
                dedupe.record(
                    messageDocId, row.dedupKey, false, filedText, row.timestamp,
                    attachmentsCarryCard(attachments),
                )
                previews[row.dedupKey] = previewText(filedText, attachments)
                val incumbent = newestPerThread[threadId]
                if (incumbent == null || row.timestamp > incumbent.timestamp) {
                    newestPerThread[threadId] = row
                }
            }
            if (undecided) break
            if (count == 0 && merged == 0) {
                if (newest > 0L) reader.ackInbox(newest)
                if (rows.size < SENT_PAGE_SIZE) break else continue
            }

            try {
                await(batch.commit())
            } catch (e: Exception) {
                Log.w(TAG, "inbox batch commit failed: ${e.message}")
                // Watermark untouched: the next sync re-reads this page.
                break
            }
            written += count

            for ((threadId, row) in newestPerThread) {
                advanceThread(
                    s,
                    threadId,
                    // Names only a thread it had to create, like a sent text.
                    title = if (idx.isNew(threadId)) row.displayName else null,
                    isGroup = false,
                    text = previews[row.dedupKey] ?: row.text,
                    senderName = row.displayName,
                    atMillis = row.timestamp,
                    extra = withLearned(
                        mapOf("deviceSmsThreadId" to row.threadId),
                        learning.take(threadId),
                    ),
                )
            }
            learning.flush(s)

            // Only now: the batch is durable.
            if (newest > 0L) reader.ackInbox(newest)
            // A conversation this path had to create brings its past with it.
            for ((threadId, row) in newestPerThread) {
                if (!idx.isNew(threadId)) continue
                runCatching {
                    pullHistory(
                        s,
                        threadId,
                        handsetThreadId = row.threadId,
                        beforeMillis = row.timestamp - HISTORY_MARGIN_MS,
                        whoForLookup = null,
                    )
                }.onFailure { Log.w(TAG, "history pull failed for $threadId: ${it.message}") }
            }
            if (rows.size < SENT_PAGE_SIZE) break
        }
        return written
    }

    // ── One conversation's history ──────────────────────────────────────────

    /**
     * Pulls the SMS/MMS past of ONE conversation, the first time a text is
     * filed into it after setup — in either direction — and never the whole
     * phone. Setting a phone up files nothing old (user, 2026-09-15): a person
     * who texts, or is texted, brings their history with them. Once per thread
     * (`deviceHistoryPulledAt`), the newest [HISTORY_LIMIT] messages older than
     * [beforeMillis]. Only a thread that passed the Limit list gets here.
     *
     * The handset conversation comes from, in order: [handsetThreadId] (a sent
     * text carries it), the thread's `deviceSmsThreadId`, or [whoForLookup] —
     * the notification's name resolved to a number through the contacts, or
     * the title itself when it IS a number. No match (an RCS-only chat, an
     * unsaved sender) pulls nothing and marks nothing, so a later text that
     * teaches the thread its number still can.
     *
     * 🛑 RCS has no readable store: an RCS chat's past cannot come in.
     *
     * Stamped `backfill: true`: indexed for search like any message
     * (commSearchIndex), but no push, no secretary, no timeline card —
     * messageDispatch skips backfills. Idempotent: ids are the provider row
     * ids the sent-text sync uses, so a repeated pull rewrites the same docs.
     */
    private fun pullHistory(
        s: Session,
        threadId: String,
        handsetThreadId: String?,
        beforeMillis: Long,
        whoForLookup: String?,
    ) {
        val reader = SentMessageReader(context)
        if (!reader.isGranted() || beforeMillis <= 0L) return
        val threadRef = s.company.collection(COL_CONVERSATION).document(threadId)
        val snap = runCatching { await(threadRef.get()) }.getOrNull() ?: return
        if (snap.get(FIELD_HISTORY_PULLED_AT) != null) return
        val handset = handsetThreadId?.takeIf { it.isNotBlank() }
            ?: (snap.get("deviceSmsThreadId") as? String)?.takeIf { it.isNotBlank() }
            ?: whoForLookup?.trim()?.takeIf { it.isNotEmpty() }?.let { who ->
                val number = if (SmsSender.looksLikeNumber(who)) who
                else SmsSender(context).resolveNumber(who)
                number?.let { reader.threadIdFor(it) }
            }
        val handsetId = handset?.toLongOrNull() ?: return

        val rows = runCatching { reader.readThread(handsetId, beforeMillis, HISTORY_LIMIT) }
            .getOrElse { return }
        // 🛑 The history sweep re-reads MONTHS of the handset's own database,
        // and the notification listener has already filed everything it saw
        // live. On 2026-09-17 that put a second copy of every message from
        // 2026-09-09 onward into the threads it swept. The window here spans
        // the whole pull, not the newest few.
        val oldest = rows.mapNotNull { it["timestamp"] as? Long }.minOrNull() ?: 0L
        val dedupe = ThreadDedupe.since(threadRef, oldest - ThreadDedupe.WINDOW_MS)
        var batch = s.db.batch()
        var inBatch = 0
        for (raw in rows) {
            val dedupKey = (raw["dedupKey"] as? String)?.takeIf { it.isNotEmpty() } ?: continue
            val text = (raw["text"] as? String).orEmpty()
            val parts = (raw["attachments"] as? List<*>)
                ?.mapNotNull { p -> (p as? Map<*, *>)?.entries?.associate { it.key.toString() to it.value } }
                ?: emptyList()
            if (text.isBlank() && parts.isEmpty()) continue
            val outgoing = raw["outgoing"] == true
            val messageDocId = DartDocId.docIdFor(dedupKey)
            val historyMillis = (raw["timestamp"] as? Long) ?: 0L
            val twin = dedupe.duplicateOf(dedupKey, outgoing, text, historyMillis, partsCarryCard(parts))
            if (twin != null) {
                // Built rather than literal: a merge-set writes an explicit
                // null, so handing it `raw["kind"]` when the sweep did not
                // report one would BLANK the field on the survivor.
                val fold = HashMap<String, Any?>()
                fold["deviceDedupKeyAlt"] = FieldValue.arrayUnion(dedupKey)
                (raw["kind"] as? String)?.takeIf { it.isNotEmpty() }
                    ?.let { fold["deviceKind"] = it }
                fold["deviceSmsThreadId"] = handsetId.toString()
                fold.putAll(cardUpgrade(dedupe, twin, parts, s.company.id))
                batch.set(
                    threadRef.collection(COL_MESSAGE).document(twin),
                    fold,
                    SetOptions.merge(),
                )
                if (++inBatch >= 400) {
                    if (!commitHistory(batch)) return
                    batch = s.db.batch()
                    inBatch = 0
                }
                continue
            }
            val sender = raw["sender"] as? String
            val attachments = uploadedAttachments(parts, s.company.id, messageDocId)
            val filedText = textBeside(text, attachments)
            val doc = HashMap<String, Any?>()
            doc["text"] = filedText
            doc["senderRef"] = if (outgoing) s.memberRef else null
            doc["senderName"] = if (outgoing) s.memberName
            else (raw["senderName"] as? String) ?: sender?.let { nationalNumber(it) } ?: "Unknown"
            doc["createdAt"] = Timestamp(Date((raw["timestamp"] as? Long) ?: 0L))
            doc["readByMemberIds"] = listOf(s.memberRef.id)
            doc["visibleToMemberIds"] = listOf(s.memberRef.id)
            doc["attachments"] = attachments
            doc["isDeleted"] = false
            doc["source"] = "device"
            doc["deviceKind"] = raw["kind"]
            doc["deviceSmsThreadId"] = handsetId.toString()
            doc["deviceDedupKey"] = dedupKey
            doc["deviceHistory"] = true
            doc["backfill"] = true
            doc["capturedAt"] = Timestamp.now()
            doc["ingestedAt"] = FieldValue.serverTimestamp()
            batch.set(
                threadRef.collection(COL_MESSAGE).document(messageDocId),
                doc,
                SetOptions.merge(),
            )
            dedupe.record(
                messageDocId,
                dedupKey,
                outgoing,
                filedText,
                historyMillis,
                attachmentsCarryCard(attachments),
            )
            // Under Firestore's 500-write batch ceiling.
            if (++inBatch >= 400) {
                if (!commitHistory(batch)) return
                batch = s.db.batch()
                inBatch = 0
            }
        }
        if (inBatch > 0 && !commitHistory(batch)) return
        // Marked only after the messages are durable; the handset id also lets
        // later sent texts file into this same thread.
        runCatching {
            await(
                threadRef.set(
                    mapOf(
                        FIELD_HISTORY_PULLED_AT to FieldValue.serverTimestamp(),
                        "deviceSmsThreadId" to handsetId.toString(),
                    ),
                    SetOptions.merge(),
                ),
            )
        }.onFailure { Log.w(TAG, "history mark failed for $threadId: ${it.message}") }
    }

    private fun commitHistory(batch: com.google.firebase.firestore.WriteBatch): Boolean =
        try {
            await(batch.commit())
            true
        } catch (e: Exception) {
            Log.w(TAG, "history batch failed: ${e.message}")
            false
        }

    /** The name a thread gets when a SENT text has to create it: its
     *  recipients, by contact name where the phone has one, else the number
     *  as the phone would print it. */
    private fun sentTitle(row: SentRow): String {
        val parts = ArrayList<String>(row.addresses.size)
        for (i in row.addresses.indices) {
            val name = row.names.getOrNull(i)?.trim().orEmpty()
            parts.add(if (name.isNotEmpty()) name else nationalNumber(row.addresses[i]))
        }
        return parts.joinToString(", ")
    }

    /** One row from [SentMessageReader.read], typed. */
    private class SentRow(
        val dedupKey: String,
        val kind: String,
        val threadId: String,
        val addresses: List<String>,
        val names: List<String?>,
        val text: String,
        val timestamp: Long,
        val attachments: List<Map<String, Any?>>,
    ) {
        companion object {
            @Suppress("UNCHECKED_CAST")
            fun from(m: Map<String, Any?>): SentRow? {
                val text = (m["text"] as? String).orEmpty()
                val attachments = (m["attachments"] as? List<*>)
                    ?.mapNotNull { it as? Map<String, Any?> }
                    ?: emptyList()
                // A picture message has no text. Requiring one here was the
                // second of the three places a photo went missing — see
                // SentMessageReader.readMms for the first.
                if (text.isEmpty() && attachments.isEmpty()) return null
                val dedupKey = (m["dedupKey"] as? String)?.takeIf { it.isNotEmpty() } ?: return null
                val addresses = (m["addresses"] as? List<*>)?.mapNotNull { it as? String } ?: emptyList()
                if (addresses.isEmpty()) return null
                val rawNames = (m["names"] as? List<*>) ?: emptyList<Any?>()
                val names = ArrayList<String?>(addresses.size)
                for (i in addresses.indices) names.add(rawNames.getOrNull(i) as? String)
                return SentRow(
                    dedupKey = dedupKey,
                    kind = (m["kind"] as? String) ?: "sms",
                    threadId = (m["threadId"] as? String) ?: "",
                    addresses = addresses,
                    names = names,
                    text = text,
                    timestamp = (m["timestamp"] as? Long) ?: 0L,
                    attachments = attachments,
                )
            }
        }
    }

    /** One row from [SentMessageReader.readInbox], typed. Null for a row
     *  that is not a text from a person: blank, no sender, or a short code /
     *  alphanumeric sender id. */
    private class InboxRow(
        val dedupKey: String,
        val kind: String,
        val threadId: String,
        val sender: String,
        val senderName: String?,
        /** TO addresses the MMS named beside the handset's own placeholder. */
        val others: List<String>,
        val text: String,
        val timestamp: Long,
        val attachments: List<Map<String, Any?>>,
    ) {
        /** The sender as the thread shows them: the contact, else the number
         *  as the phone would print it. */
        val displayName: String
            get() = senderName?.trim()?.takeIf { it.isNotEmpty() } ?: nationalNumber(sender)

        companion object {
            @Suppress("UNCHECKED_CAST")
            fun from(m: Map<String, Any?>): InboxRow? {
                val text = (m["text"] as? String).orEmpty()
                val attachments = (m["attachments"] as? List<*>)
                    ?.mapNotNull { it as? Map<String, Any?> }
                    ?: emptyList()
                if (text.isEmpty() && attachments.isEmpty()) return null
                val dedupKey = (m["dedupKey"] as? String)?.takeIf { it.isNotEmpty() } ?: return null
                val sender = (m["sender"] as? String)?.trim().orEmpty()
                if (sender.isEmpty() || looksLikeServiceSender(sender)) return null
                return InboxRow(
                    dedupKey = dedupKey,
                    kind = (m["kind"] as? String) ?: "sms",
                    threadId = (m["threadId"] as? String) ?: "",
                    sender = sender,
                    senderName = m["senderName"] as? String,
                    others = (m["others"] as? List<*>)?.mapNotNull { it as? String } ?: emptyList(),
                    text = text,
                    timestamp = (m["timestamp"] as? Long) ?: 0L,
                    attachments = attachments,
                )
            }
        }
    }

    /**
     * Copies each captured picture to Storage and returns the `attachments`
     * value for the message document — the shape `TextMessageAttachment`
     * reads, so a captured photo renders in the bubble exactly like one sent
     * from the app. A vCard part goes through [uploadedCard] instead
     * (2026-10-15) and may yield several attachments, one per card.
     *
     * A picture that cannot be read or uploaded is DROPPED, not deferred. Both
     * sources are perishable — a notification's URI grant dies with the
     * notification, and an MMS part can be reclaimed — so retrying forever
     * would hold the whole message hostage to bytes that are already gone. The
     * message still lands with its text; [previewText] is what stops a
     * picture-only one from arriving as a blank bubble.
     */
    private fun uploadedAttachments(
        parts: List<Map<String, Any?>>,
        companyId: String,
        messageDocId: String,
    ): List<Map<String, Any?>> {
        if (parts.isEmpty()) return emptyList()
        val uploader = AttachmentUploader(context)
        val out = ArrayList<Map<String, Any?>>(parts.size)
        for (part in parts) {
            val uri = (part["uri"] as? String)?.takeIf { it.isNotEmpty() } ?: continue
            if (VCardParser.isVCardMime(part["mimeType"] as? String, part["fileName"] as? String)) {
                out.addAll(uploadedCard(uploader, part, uri, companyId, messageDocId))
                continue
            }
            val mimeType = (part["mimeType"] as? String).orEmpty().ifBlank { "image/jpeg" }
            val fileName = (part["fileName"] as? String).orEmpty().ifBlank { "photo" }
            val url = uploader.upload(
                uri = uri,
                mimeType = mimeType,
                fileName = fileName,
                companyId = companyId,
                messageDocId = messageDocId,
            ) ?: continue
            out.add(
                hashMapOf(
                    "url" to url,
                    "fileName" to fileName,
                    "mimeType" to mimeType,
                    // Only images reach here — SentMessageReader and the
                    // listener filter on `image/`, and a vCard took the
                    // branch above — so the bubble has something to render.
                    "type" to "image",
                )
            )
        }
        return out
    }

    /**
     * A texted CONTACT CARD (2026-10-15): read, parsed with [VCardParser],
     * and the raw `.vcf` uploaded as-is. One `type: 'contact'` attachment
     * per card in the file, all pointing at the same upload — the shape
     * `TextMessageAttachment.contact` reads. A file that parses to nothing
     * is still filed, as a `document`, so the member can at least open it.
     * Unreadable or past [AttachmentUploader.VCARD_MAX_BYTES]: dropped, like
     * a picture, and for the same reason (the source is perishable).
     */
    private fun uploadedCard(
        uploader: AttachmentUploader,
        part: Map<String, Any?>,
        uri: String,
        companyId: String,
        messageDocId: String,
    ): List<Map<String, Any?>> {
        // A part held in the provider's `text` column has no file behind its
        // URI — SentMessageReader hands its text over as `inlineText`.
        val bytes = uploader.readBytes(uri, AttachmentUploader.VCARD_MAX_BYTES)
            ?: (part["inlineText"] as? String)?.takeIf { it.isNotBlank() }?.toByteArray(Charsets.UTF_8)
            ?: return emptyList()
        val partMime = (part["mimeType"] as? String).orEmpty().trim()
        val mimeType = if (VCardParser.isVCardMime(partMime)) partMime else "text/vcard"
        val named = (part["fileName"] as? String).orEmpty().trim().ifBlank { "contact.vcf" }
        val fileName = if (named.lowercase().endsWith(".vcf")) named else "$named.vcf"
        val cards = runCatching { VCardParser.parse(VCardParser.decode(bytes)) }
            .onFailure { Log.w(TAG, "vCard $fileName did not parse: ${it.message}") }
            .getOrDefault(emptyList())
        val url = uploader.uploadBytes(
            bytes = bytes,
            mimeType = "text/vcard",
            fileName = fileName,
            companyId = companyId,
            messageDocId = messageDocId,
        ) ?: return emptyList()
        if (cards.isEmpty()) {
            return listOf(
                hashMapOf(
                    "url" to url,
                    "fileName" to fileName,
                    "mimeType" to mimeType,
                    "type" to "document",
                )
            )
        }
        return cards.map { card ->
            hashMapOf<String, Any?>(
                "url" to url,
                "fileName" to fileName,
                "mimeType" to mimeType,
                "type" to "contact",
                "contact" to card,
            )
        }
    }

    /** Whether capture handles (before upload) include a vCard part. */
    private fun partsCarryCard(parts: List<Map<String, Any?>>): Boolean = parts.any {
        VCardParser.isVCardMime(it["mimeType"] as? String, it["fileName"] as? String)
    }

    /** Whether uploaded attachments include a parsed card. */
    private fun attachmentsCarryCard(attachments: List<Map<String, Any?>>): Boolean =
        attachments.any { it["type"] == "contact" }

    /**
     * The text to file beside [attachments]. A sender's "Contact card"
     * placeholder next to the parsed card is dropped (2026-10-15), so the
     * bubble is the card alone; next to a card that did NOT parse it stays,
     * because then it is the only thing saying what the file is.
     */
    private fun textBeside(text: String, attachments: List<Map<String, Any?>>): String =
        if (attachmentsCarryCard(attachments) && VCardParser.isPlaceholderText(text)) "" else text

    /**
     * Merge fields that hand a twin captured WITHOUT its card the card this
     * copy carries (2026-10-15). The notification can post "Contact card"
     * with no data URI; the provider reads the same MMS seconds later WITH
     * the vCard. [ThreadDedupe] pairs them, and folding only the alt key
     * would keep the copy that has nothing behind its words. So the card is
     * uploaded under the TWIN's id and written onto it, and its placeholder
     * text cleared. Empty when there is nothing to add.
     */
    private fun cardUpgrade(
        dedupe: ThreadDedupe,
        twin: String,
        parts: List<Map<String, Any?>>,
        companyId: String,
    ): Map<String, Any?> {
        if (!partsCarryCard(parts) || dedupe.carriesCard(twin)) return emptyMap()
        val attachments = uploadedAttachments(parts, companyId, twin)
        if (!attachmentsCarryCard(attachments)) return emptyMap()
        val out = HashMap<String, Any?>()
        out["attachments"] = attachments
        if (dedupe.placeholderOnly(twin)) out["text"] = ""
        dedupe.markCard(twin)
        return out
    }

    /**
     * What a thread's preview line says for this message. A picture message
     * has no text, and a thread whose newest row is blank reads as an empty
     * conversation in the list — the same silence the picture itself used to
     * arrive in. A contact card reads `👤 <name>` (2026-10-15).
     */
    private fun previewText(text: String, attachments: List<Map<String, Any?>>): String {
        if (text.isNotBlank() || attachments.isEmpty()) return text
        val cards = attachments.filter { it["type"] == "contact" }
        if (cards.isNotEmpty()) {
            if (cards.size > 1) return "👤 ${cards.size} contacts"
            @Suppress("UNCHECKED_CAST")
            val name = (cards.first()["contact"] as? Map<String, Any>)
                ?.let { VCardParser.nameOf(it).trim() }
                .orEmpty()
            return if (name.isEmpty()) "👤 Contact" else "👤 $name"
        }
        return if (attachments.size == 1) "📷 Photo" else "📷 ${attachments.size} photos"
    }

    /** The member's device threads, indexed for filing texts in EITHER
     *  direction. Mirrors `_DeviceThreadIndex`. */
    /**
     * What a thread ALREADY holds, so two capture paths cannot file one text
     * twice.
     *
     * Every path keys its documents on a dedup key of its own making — the
     * notification listener on `<package>|<conversation>|<ts>|<sender>|<hash>`,
     * the SMS/MMS provider on `mms|37026`, the history sweep on the same
     * provider ids. The document id is derived from that key, so each path is
     * perfectly idempotent WITH ITSELF and blind to every other. An outgoing
     * text is seen by two of them and a swept conversation by all three, which
     * is why "sure" arrived in the thread twice, 1.8 seconds apart.
     *
     * There is no id to join on: the provider truncates the timestamp to whole
     * seconds and reports it about a second off the notification's. So the
     * match is on CONTENT — same direction, same words, within [WINDOW_MS] —
     * and only ACROSS namespaces: two rows from one path with the same words
     * are two real messages (somebody typed "ok" twice), and dropping one of
     * those would be losing a message to hide a duplicate.
     *
     * A match is consumed, so a genuine pair of identical texts pairs off
     * one-to-one against the other path's pair instead of collapsing to one.
     * The survivor takes the newcomer's key in `deviceDedupKeyAlt`, and the
     * next sweep matches on that key exactly, with no guessing.
     *
     * 🛑 A CONTACT CARD's two copies do not share words (2026-10-15). The
     * notification may file the sender's stand-in "Contact card" with no
     * data URI, and the provider files the same MMS with the vCard and NO
     * text (the placeholder is dropped beside a parsed card). So a
     * placeholder-only text and an empty/placeholder one also pair — but
     * only when EXACTLY ONE side carries the card. Two card-less blanks are
     * two pictures; two cards are two cards.
     */
    private class ThreadDedupe private constructor(private val rows: MutableList<Row>) {

        private class Row(
            val id: String,
            val namespace: String,
            val alts: MutableSet<String>,
            val outgoing: Boolean,
            val text: String,
            val millis: Long,
            /** Holds a contact card (or the raw `.vcf` of one). */
            var hasCard: Boolean = false,
            var claimed: Boolean = false,
        )

        /**
         * The id of the message already holding this text, or null when it is
         * new to the thread. [hasCard]: the newcomer carries a vCard part.
         */
        fun duplicateOf(
            dedupKey: String,
            outgoing: Boolean,
            text: String,
            millis: Long,
            hasCard: Boolean = false,
        ): String? {
            // Written by an earlier sweep that already folded this key in.
            rows.firstOrNull { it.alts.contains(dedupKey) }?.let { return it.id }
            return twinOf(dedupKey, outgoing, text, millis, hasCard)?.also {
                it.claimed = true
                it.alts.add(dedupKey)
            }?.id
        }

        /**
         * Whether the thread holds a twin of this text — [duplicateOf]
         * without claiming it. The cross-thread join asks this of OTHER
         * threads before a path mints a new one (2026-10-23); the path that
         * then files into the thread it names claims the twin itself.
         */
        fun hasTwin(
            dedupKey: String,
            outgoing: Boolean,
            text: String,
            millis: Long,
            hasCard: Boolean = false,
        ): Boolean =
            rows.any { it.alts.contains(dedupKey) } ||
                twinOf(dedupKey, outgoing, text, millis, hasCard) != null

        private fun twinOf(
            dedupKey: String,
            outgoing: Boolean,
            text: String,
            millis: Long,
            hasCard: Boolean,
        ): Row? {
            val ns = namespaceOf(dedupKey)
            val body = normalize(text)
            if (body.isEmpty() && !hasCard) return null
            return rows.firstOrNull { row ->
                !row.claimed &&
                    row.namespace != ns &&
                    // A row that already folded a key from THIS path has met
                    // its twin; a second same-words row from this path is a
                    // second message, not a third copy.
                    row.alts.none { namespaceOf(it) == ns } &&
                    row.outgoing == outgoing &&
                    kotlin.math.abs(row.millis - millis) <= WINDOW_MS &&
                    ((body.isNotEmpty() && row.text == body) ||
                        cardTwins(row.text, row.hasCard, body, hasCard))
            }
        }

        /** Adds a message this batch is about to write. */
        fun record(
            docId: String,
            dedupKey: String,
            outgoing: Boolean,
            text: String,
            millis: Long,
            hasCard: Boolean = false,
        ) {
            rows.add(
                Row(
                    id = docId,
                    namespace = namespaceOf(dedupKey),
                    alts = mutableSetOf(dedupKey),
                    outgoing = outgoing,
                    text = normalize(text),
                    millis = millis,
                    hasCard = hasCard,
                )
            )
        }

        /** Whether the message [docId] already holds a contact card. */
        fun carriesCard(docId: String): Boolean = rows.any { it.id == docId && it.hasCard }

        /** Whether the message [docId]'s words are only a card placeholder. */
        fun placeholderOnly(docId: String): Boolean =
            rows.firstOrNull { it.id == docId }?.let { VCardParser.isPlaceholderText(it.text) } == true

        /** The card was just written onto [docId]; a third copy adds nothing. */
        fun markCard(docId: String) {
            rows.filter { it.id == docId }.forEach { it.hasCard = true }
        }

        companion object {
            /** Observed gap between the two paths is ~2s; this is slack, not a guess. */
            const val WINDOW_MS = 120_000L
            private const val FIELD_CREATED = "createdAt"
            private const val RECENT = 80L
            private const val SWEEP = 500L

            /** The newest messages — enough for a live capture or a sent page. */
            fun recent(threadRef: DocumentReference): ThreadDedupe = load(
                threadRef.collection(COL_MESSAGE)
                    .orderBy(FIELD_CREATED, Query.Direction.DESCENDING)
                    .limit(RECENT)
            )

            /** Everything since [sinceMillis] — for a sweep that reaches back months. */
            fun since(threadRef: DocumentReference, sinceMillis: Long): ThreadDedupe = load(
                threadRef.collection(COL_MESSAGE)
                    .whereGreaterThanOrEqualTo(
                        FIELD_CREATED,
                        Timestamp(Date(maxOf(0L, sinceMillis))),
                    )
                    .orderBy(FIELD_CREATED, Query.Direction.DESCENDING)
                    .limit(SWEEP)
            )

            private fun load(query: Query): ThreadDedupe {
                val rows = ArrayList<Row>()
                // A thread we cannot read is a thread we cannot dedupe
                // against. Writing the message anyway is the safe failure:
                // a duplicate can be merged later, a dropped text cannot be
                // recovered from anywhere.
                val snap = runCatching { await(query.get()) }.getOrNull()
                for (doc in snap?.documents.orEmpty()) {
                    val key = (doc.get("deviceDedupKey") as? String).orEmpty()
                    val alts = HashSet<String>()
                    if (key.isNotEmpty()) alts.add(key)
                    // Star projection: no unchecked cast, so no suppression.
                    (doc.get("deviceDedupKeyAlt") as? Iterable<*>)?.forEach { alt ->
                        (alt as? String)?.takeIf { it.isNotEmpty() }?.let { alts.add(it) }
                    }
                    val at = (doc.get(FIELD_CREATED) as? Timestamp)?.toDate()?.time ?: continue
                    val hasCard = (doc.get("attachments") as? Iterable<*>)?.any { a ->
                        val m = a as? Map<*, *>
                        m != null && (
                            m["type"] == "contact" ||
                                VCardParser.isVCardMime(m["mimeType"] as? String, m["fileName"] as? String)
                            )
                    } == true
                    rows.add(
                        Row(
                            id = doc.id,
                            namespace = namespaceOf(key),
                            alts = alts.toMutableSet(),
                            outgoing = doc.get("senderRef") != null,
                            text = normalize((doc.get("text") as? String).orEmpty()),
                            millis = at,
                            hasCard = hasCard,
                        )
                    )
                }
                return ThreadDedupe(rows)
            }

            /** The capture path a key came from: `mms`, `sms`, the app id. */
            private fun namespaceOf(key: String): String = key.substringBefore('|')

            private fun normalize(text: String): String = text.trim().lowercase()

            /**
             * Two copies of one shared contact: exactly one side holds the
             * card, its words are empty or a placeholder, and the card-less
             * side's words ARE a placeholder (an empty card-less row is a
             * picture, not this).
             */
            private fun cardTwins(aText: String, aCard: Boolean, bText: String, bCard: Boolean): Boolean {
                if (!aCard && !bCard) return false
                // Both copies carry the card (a vCard data URI on the
                // notification AND the MMS part): the same message when
                // neither has words of its own. Claiming is one-to-one and
                // across capture paths only, so two cards sent back to back
                // still land as two (2026-10-15).
                if (aCard && bCard) {
                    return (aText.isEmpty() || VCardParser.isPlaceholderText(aText)) &&
                        (bText.isEmpty() || VCardParser.isPlaceholderText(bText))
                }
                val cardText = if (aCard) aText else bText
                val plainText = if (aCard) bText else aText
                return (cardText.isEmpty() || VCardParser.isPlaceholderText(cardText)) &&
                    VCardParser.isPlaceholderText(plainText)
            }
        }
    }

    /**
     * The member's device threads as [DeviceThreadIndex] rows — the index
     * itself is pure and lives in DeviceThreadIndex.kt. Null when the read
     * failed: a caller that cannot see the existing threads must not file,
     * or it re-creates the split the index exists to prevent.
     */
    private fun loadThreadIndex(s: Session): DeviceThreadIndex? {
        val snap = runCatching {
            // list-cap: 500. One member's own phone; the live member has 38
            // device threads (measured 2026-09-06). Same cap as the Dart index.
            await(
                s.company.collection(COL_CONVERSATION)
                    .whereEqualTo("channel", "device")
                    .whereArrayContains("participantRefs", s.memberRef)
                    .limit(500)
                    .get(),
            )
        }.onFailure { Log.w(TAG, "thread index load failed: ${it.message}") }
            .getOrNull() ?: return null
        val rows = snap.documents.map { doc ->
            DeviceThreadIndex.ThreadRow(
                id = doc.id,
                handsetThreadId = doc.get("deviceSmsThreadId") as? String,
                conversationKey = doc.get("deviceConversationKey") as? String,
                title = (doc.get("title") as? String).orEmpty(),
                numbers = stringsOf(doc.get("deviceRecipients")),
                aliases = stringsOf(doc.get("deviceAliases")),
                isGroup = doc.get("isGroup") == true,
                lastMessageMillis = (doc.get("lastMessageAt") as? Timestamp)?.toDate()?.time,
            )
        }
        return DeviceThreadIndex(rows)
    }

    private fun stringsOf(v: Any?): List<String> =
        (v as? Iterable<*>)?.mapNotNull { (it as? String)?.trim()?.takeIf { t -> t.isNotEmpty() } }
            ?: emptyList()

    private fun conversationRef(s: Session, threadId: String): DocumentReference =
        s.company.collection(COL_CONVERSATION).document(threadId)

    /**
     * The numbers a notification named for the other party (2026-10-23):
     * the sender's `tel:` URI when Google Messages set one on the Person,
     * and the notification's people list. Empty for most RCS threads — the
     * listener records what the handset gave, and the cross-thread twin join
     * is what covers a name-only capture.
     */
    private fun notificationNumbers(m: JSONObject): List<String> {
        val out = LinkedHashSet<String>()
        m.optStringOrNull("senderNumber")?.trim()?.takeIf { it.isNotEmpty() }?.let { out.add(it) }
        val arr = m.optJSONArray("partyNumbers")
        if (arr != null) {
            for (i in 0 until arr.length()) {
                arr.optString(i)?.trim()?.takeIf { it.isNotEmpty() }?.let { out.add(it) }
            }
        }
        return out.toList()
    }

    /** [base] with what the thread learned, where the base does not already
     *  say it (the sent path writes `deviceRecipients` whole; a union must
     *  not fight it in one set). */
    private fun withLearned(base: Map<String, Any?>, learned: Map<String, Any?>): Map<String, Any?> =
        if (learned.isEmpty()) base else base + learned.filterKeys { it !in base }

    /**
     * What each thread learned about its other party in one pass — numbers,
     * names, its handset thread id — written ONCE per thread after the batch:
     * on the thread advance for a thread that got a new message, and as a
     * merge of its own for a thread that only FOLDED a twin (the case the
     * cross-thread join creates, and the one that must learn the alias).
     */
    private class ThreadLearning {
        private class Facts {
            val numbers = LinkedHashSet<String>()
            val names = LinkedHashSet<String>()
            var handsetThreadId: String? = null
        }

        private val facts = HashMap<String, Facts>()

        fun note(threadId: String, learned: DeviceThreadIndex.Learned, handsetThreadId: String? = null) {
            if (learned.isEmpty && handsetThreadId == null) return
            val f = facts.getOrPut(threadId) { Facts() }
            f.numbers.addAll(learned.numbers)
            f.names.addAll(learned.names)
            handsetThreadId?.takeIf { it.isNotBlank() }?.let { f.handsetThreadId = it }
        }

        /** The merge fields for [threadId], taken off the ledger. */
        fun take(threadId: String): Map<String, Any?> {
            val f = facts.remove(threadId) ?: return emptyMap()
            val out = HashMap<String, Any?>()
            if (f.numbers.isNotEmpty()) out["deviceRecipients"] = FieldValue.arrayUnion(*f.numbers.toTypedArray())
            if (f.names.isNotEmpty()) out["deviceAliases"] = FieldValue.arrayUnion(*f.names.toTypedArray())
            f.handsetThreadId?.let { out["deviceSmsThreadId"] = it }
            return out
        }

        /** Whatever no thread advance carried: the fold-only threads. */
        fun flush(s: Session) {
            for (id in facts.keys.toList()) {
                val fields = take(id)
                if (fields.isEmpty()) continue
                runCatching {
                    await(s.company.collection(COL_CONVERSATION).document(id).set(fields, SetOptions.merge()))
                }.onFailure { Log.w(TAG, "thread learn failed for $id: ${it.message}") }
            }
        }
    }

    // ── Thread preview ──────────────────────────────────────────────────────

    // ── Replies that waited for a reply box ─────────────────────────────────

    /**
     * Sends every outbox row of this member that is `waiting` on
     * [conversationKey] — a group reply typed elsewhere while this phone had
     * no reply box for the thread — now that the conversation has posted
     * again. [fire] is the listener's inline reply; it answers false when the
     * box is not there after all.
     *
     * Runs on [executor] (Firestore is awaited); blocking. Returns how many
     * left the phone. Mirrors the Dart `DeviceOutboxService._sendOne` for the
     * one route the Dart side cannot take on its own: the Dart engine is not
     * running when the notification lands, and the box lives in THIS process.
     *
     * - Claimed in a transaction (`waiting` → `sending`) so a Dart pass that
     *   happens to be awake never sends the same words twice.
     * - A row that has waited longer than [WAITING_MAX_AGE_MS] is NOT sent:
     *   the conversation has moved on without those words, and a reply
     *   that arrives a day late reads as a reply to whatever came since. It
     *   fails, with that reason, and the member decides from the strip.
     * - On `sent` the text is FILED into the thread here, keyed
     *   `outbox|<id>` exactly as `MessageIngestService.recordOutboxSent`
     *   files it, so the feed moves at once and the re-posted notification
     *   folds into this document through the thread dedupe.
     */
    fun sendWaitingReplies(conversationKey: String, fire: (text: String) -> Boolean): Int {
        if (conversationKey.isEmpty()) return 0
        val s = session() ?: return 0
        val rows = runCatching {
            await(
                s.company.collection(COL_OUTBOX)
                    .whereEqualTo("memberRef", s.memberRef)
                    .whereEqualTo("status", STATUS_WAITING)
                    .whereEqualTo("conversationKey", conversationKey)
                    // list-cap: 20. ONE member's replies waiting on ONE
                    // conversation — typed between two texts of a thread;
                    // 0 rows existed when the status was created
                    // (2026-10-14). The next post sends the rest.
                    .limit(20)
                    .get(),
            )
        }.onFailure { Log.w(TAG, "waiting outbox read failed: ${it.message}") }
            .getOrNull() ?: return 0
        if (rows.isEmpty) return 0

        val docs = rows.documents.sortedBy { d ->
            (d.get("createdAt") as? Timestamp)?.toDate()?.time ?: 0L
        }
        var sent = 0
        val now = System.currentTimeMillis()
        for (d in docs) {
            val ref = d.reference
            val text = (d.get("text") as? String)?.trim().orEmpty()
            val threadId = (d.get("threadId") as? String)?.trim().orEmpty()
            val isGroup = d.get("isGroup") == true
            val since = ((d.get("retriedAt") as? Timestamp) ?: (d.get("createdAt") as? Timestamp))
                ?.toDate()?.time ?: now
            // Claim it, or leave it to whoever did.
            val claimed = runCatching {
                await(
                    s.db.runTransaction { tx ->
                        val fresh = tx.get(ref)
                        if (fresh.get("status") != STATUS_WAITING) return@runTransaction false
                        tx.set(
                            ref,
                            mapOf(
                                "status" to "sending",
                                "claimedAt" to FieldValue.serverTimestamp(),
                                "claimedBy" to "phone",
                            ),
                            SetOptions.merge(),
                        )
                        true
                    },
                )
            }.getOrDefault(false)
            if (!claimed) continue

            fun settle(fields: Map<String, Any?>) {
                runCatching { await(ref.set(fields, SetOptions.merge())) }
                    .onFailure { Log.w(TAG, "outbox ${ref.id} settle failed: ${it.message}") }
            }

            if (text.isEmpty()) {
                settle(mapOf("status" to "failed", "done" to false, "error" to "Nothing to send.",
                    "failedAt" to FieldValue.serverTimestamp()))
                continue
            }
            if (now - since > WAITING_MAX_AGE_MS) {
                settle(
                    mapOf(
                        "status" to "failed",
                        "done" to false,
                        "error" to "The conversation came back to your phone more than a day " +
                            "after you wrote this, so it was not sent. Retry sends it now.",
                        "failedAt" to FieldValue.serverTimestamp(),
                    ),
                )
                continue
            }

            val ok = try {
                fire(text)
            } catch (e: Exception) {
                settle(
                    mapOf(
                        "status" to "failed",
                        "done" to false,
                        "error" to "Your phone could not send it: ${e.message ?: e.javaClass.simpleName}",
                        "failedAt" to FieldValue.serverTimestamp(),
                    ),
                )
                continue
            }
            if (!ok) {
                // The box the post promised is not usable after all. Back to
                // waiting: the next post brings a new one.
                settle(mapOf("status" to STATUS_WAITING, "done" to false, "replyBox" to "gone"))
                continue
            }

            val sentAt = Date()
            settle(
                mapOf(
                    "status" to "sent",
                    "done" to true,
                    "route" to "inline",
                    "replyBox" to "reposted",
                    "sentBy" to "phone",
                    "error" to FieldValue.delete(),
                    "sentAt" to FieldValue.serverTimestamp(),
                ),
            )
            sent++
            if (threadId.isEmpty()) continue
            // File it — the mirror of `recordOutboxSent`.
            val dedupKey = "outbox|${ref.id}"
            val threadRef = s.company.collection(COL_CONVERSATION).document(threadId)
            val doc = HashMap<String, Any?>()
            doc["text"] = text
            doc["senderRef"] = s.memberRef
            doc["senderName"] = s.memberName
            doc["createdAt"] = Timestamp(sentAt)
            doc["readByMemberIds"] = listOf(s.memberRef.id)
            doc["visibleToMemberIds"] = listOf(s.memberRef.id)
            doc["attachments"] = emptyList<Map<String, Any?>>()
            doc["isDeleted"] = false
            doc["source"] = "device"
            doc["deviceOutboxId"] = ref.id
            doc["deviceRoute"] = "inline"
            doc["deviceDedupKey"] = dedupKey
            doc["capturedAt"] = Timestamp(sentAt)
            doc["ingestedAt"] = FieldValue.serverTimestamp()
            runCatching {
                await(
                    threadRef.collection(COL_MESSAGE)
                        .document(DartDocId.docIdFor(dedupKey))
                        .set(doc, SetOptions.merge()),
                )
            }.onFailure { Log.w(TAG, "could not file waiting reply ${ref.id}: ${it.message}") }
            advanceThread(
                s,
                threadId,
                // A sent text never renames the thread it answers.
                title = null,
                isGroup = isGroup,
                text = text,
                senderName = s.memberName,
                atMillis = sentAt.time,
                extra = emptyMap(),
            )
        }
        return sent
    }

    /**
     * Moves a thread's preview forward to [atMillis] — never backwards —
     * inside a transaction, exactly as `_advanceThread` does. Returns false
     * when the transaction could not run (offline, or a rules refusal).
     */
    private fun advanceThread(
        s: Session,
        threadId: String,
        title: String?,
        isGroup: Boolean,
        text: String,
        senderName: String,
        atMillis: Long,
        extra: Map<String, Any?>,
    ): Boolean {
        val threadRef = s.company.collection(COL_CONVERSATION).document(threadId)
        val at = Timestamp(Date(atMillis))
        return try {
            await(
                s.db.runTransaction { tx ->
                    val snap = tx.get(threadRef)
                    val existing = snap.get("lastMessageAt") as? Timestamp
                    val priorTitle = (snap.get("title") as? String)?.takeIf { it.isNotBlank() }
                    if (snap.exists() && existing != null && at.compareTo(existing) <= 0) {
                        // An equal or newer message already landed. Keep the
                        // preview; still keep what this one taught us.
                        if (extra.isNotEmpty()) tx.set(threadRef, extra, SetOptions.merge())
                        return@runTransaction null
                    }
                    val doc = HashMap<String, Any?>()
                    doc["title"] = title ?: priorTitle ?: "Unknown"
                    doc["channel"] = "device"
                    doc["participantRefs"] = listOf(s.memberRef)
                    doc["participantNames"] = listOf(s.memberName)
                    // arrayUnion, never a list: the server files the thread
                    // under the matching directory contact in this same field
                    // (deviceThreadContacts.js), and a merge-set of a list
                    // REPLACES it on every message.
                    doc["commContextRefs"] = FieldValue.arrayUnion(s.memberRef.path)
                    doc["isGroup"] = isGroup
                    doc["lastMessageText"] = text
                    doc["lastMessageSenderName"] = senderName
                    doc["lastMessageAt"] = at
                    doc["updatedAt"] = FieldValue.serverTimestamp()
                    if (!snap.exists()) {
                        doc["createdAt"] = FieldValue.serverTimestamp()
                        doc["createdBy"] = s.memberRef
                    }
                    doc.putAll(extra)
                    tx.set(threadRef, doc, SetOptions.merge())
                    null
                },
            )
            true
        } catch (e: Exception) {
            Log.w(TAG, "thread advance failed for $threadId: ${e.message}")
            false
        }
    }
}

// ── Helpers shared with the Dart side's text normalisation ──────────────────

/** `Tasks.await` with the writer's timeout; unwraps the cause. */
private fun <T> await(task: Task<T>): T {
    try {
        return Tasks.await(task, 25L, TimeUnit.SECONDS)
    } catch (e: ExecutionException) {
        throw (e.cause as? Exception) ?: e
    }
}

private fun JSONObject.optStringOrNull(key: String): String? {
    if (!has(key) || isNull(key)) return null
    return optString(key)
}

/**
 * A sender that is not a person: a short code (fewer than seven digits) or an
 * alphanumeric sender id ("VERIZON", "Google"). What arrives from one is a
 * one-time code or a service notice — nothing a member answers, and the
 * listener declines the OS's redacted placeholder for the same texts.
 */
internal fun looksLikeServiceSender(address: String): Boolean {
    val bare = address.filter { !it.isWhitespace() && it != '+' && it != '-' && it != '(' && it != ')' && it != '.' }
    if (bare.isEmpty()) return true
    if (!bare.all { it.isDigit() }) return true
    return bare.length < 7
}

/**
 * `+18016949606` → `(801) 694-9606`, the way Google Messages titles a thread
 * with no contact. Anything that is not ten US digits comes back as typed.
 */
internal fun nationalNumber(raw: String): String {
    val digits = raw.filter { it.isDigit() }
    val local = if (digits.length == 11 && digits.startsWith("1")) digits.substring(1) else digits
    if (local.length != 10) return raw
    return "(" + local.substring(0, 3) + ") " + local.substring(3, 6) + "-" + local.substring(6)
}

// `partiesKey` lives beside the index that keys on it: DeviceThreadIndex.kt.

