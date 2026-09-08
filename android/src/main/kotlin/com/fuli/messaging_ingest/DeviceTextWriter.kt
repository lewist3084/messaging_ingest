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
 * - The switches (`deviceTextCaptureOff`, `deviceCaptureIncoming`,
 *   `deviceCaptureSent`): read FRESH off the member document on every write,
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

        private const val FIELD_CAPTURE_OFF = "deviceTextCaptureOff"
        private const val FIELD_CAPTURE_INCOMING = "deviceCaptureIncoming"
        private const val FIELD_CAPTURE_SENT = "deviceCaptureSent"

        /** A batch commit is durable locally as soon as it is enqueued; this
         *  is how long we wait for the SERVER before moving on. */
        private const val WRITE_TIMEOUT_S = 25L

        /** Sent-text paging, the same constants as the Dart side. */
        private const val SENT_BACKFILL_MS = 30L * 24 * 60 * 60 * 1000
        private const val SENT_PAGE_SIZE = 300
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
        val captureIncoming: Boolean,
        val captureSent: Boolean,
    )

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
            captureIncoming = data[FIELD_CAPTURE_INCOMING] != false,
            captureSent = data[FIELD_CAPTURE_SENT] != false,
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
        // "Bring in texts that arrive" off: dropped, exactly as the drain
        // drops them — nothing announced while it is off is filed.
        if (!s.captureIncoming) return Outcome.HANDLED

        val batch = s.db.batch()
        val newestPerThread = HashMap<String, JSONObject>()
        var count = 0
        for (m in payloads) {
            val packageName = m.optString("packageName")
            if (packageName !in s.packages) continue
            val conversationKey = m.optString("conversationKey")
            val dedupKey = m.optString("dedupKey")
            if (conversationKey.isEmpty() || dedupKey.isEmpty()) continue
            val threadId = DartDocId.deviceConversationId(conversationKey)
            val threadRef = s.company.collection(COL_CONVERSATION).document(threadId)
            val fromMe = m.optBoolean("isFromMe", false)
            val senderName = m.optStringOrNull("senderName")
            val timestamp = m.optLong("timestamp", 0L)
            val postedAt = m.optLong("postedAt", timestamp)

            val doc = HashMap<String, Any?>()
            doc["text"] = m.optString("text")
            doc["senderRef"] = if (fromMe) s.memberRef else null
            doc["senderName"] = if (fromMe) s.memberName else (senderName ?: "Unknown")
            // The time it was SENT, not the time it reached Firestore.
            doc["createdAt"] = Timestamp(Date(timestamp))
            doc["readByMemberIds"] = listOf(s.memberRef.id)
            doc["visibleToMemberIds"] = listOf(s.memberRef.id)
            doc["attachments"] = emptyList<Any>()
            doc["isDeleted"] = false
            doc["source"] = "device"
            doc["capturedAt"] = Timestamp(Date(postedAt))
            doc["devicePackageName"] = packageName
            doc["deviceDedupKey"] = dedupKey
            doc["canReply"] = m.optBoolean("canReply", false)
            doc["ingestedAt"] = FieldValue.serverTimestamp()
            batch.set(
                threadRef.collection(COL_MESSAGE).document(DartDocId.docIdFor(dedupKey)),
                doc,
                SetOptions.merge(),
            )
            count++

            val incumbent = newestPerThread[threadId]
            if (incumbent == null || timestamp > incumbent.optLong("timestamp", 0L)) {
                newestPerThread[threadId] = m
            }
        }
        if (count == 0) return Outcome.HANDLED

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
                text = m.optString("text"),
                senderName = if (fromMe) s.memberName else (senderName ?: "Unknown"),
                atMillis = m.optLong("timestamp", 0L),
                extra = mapOf(
                    "devicePackageName" to m.optString("packageName"),
                    "deviceConversationKey" to m.optString("conversationKey"),
                ),
            )
            if (!ok) allAdvanced = false
        }
        return if (allAdvanced) Outcome.HANDLED else Outcome.DEFER
    }

    /**
     * The name a captured thread wears: the handset's conversation title when
     * it has one, else the other party. Blank counts as absent, and so does
     * our own name — an outgoing message names nobody.
     */
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
        if (s.captureOff || !s.captureSent) return 0

        var index: ThreadIndex? = null
        var written = 0
        for (page in 0 until SENT_PAGES_PER_SYNC) {
            val watermark = reader.watermark()
            val since = if (watermark > 0L) watermark else System.currentTimeMillis() - SENT_BACKFILL_MS
            val rows = runCatching { reader.read(since, SENT_PAGE_SIZE) }.getOrElse { emptyList() }
            if (rows.isEmpty()) break

            val idx = index ?: ThreadIndex.load(s).also { index = it }
            val batch = s.db.batch()
            val newestPerThread = HashMap<String, SentRow>()
            var newest = 0L
            var count = 0
            for (raw in rows) {
                val row = SentRow.from(raw) ?: continue
                val threadId = idx.resolve(row)
                val threadRef = s.company.collection(COL_CONVERSATION).document(threadId)
                val doc = HashMap<String, Any?>()
                doc["text"] = row.text
                doc["senderRef"] = s.memberRef
                doc["senderName"] = s.memberName
                doc["createdAt"] = Timestamp(Date(row.timestamp))
                doc["readByMemberIds"] = listOf(s.memberRef.id)
                doc["visibleToMemberIds"] = listOf(s.memberRef.id)
                doc["attachments"] = emptyList<Any>()
                doc["isDeleted"] = false
                doc["source"] = "device"
                doc["deviceKind"] = row.kind
                doc["deviceSmsThreadId"] = row.threadId
                doc["deviceRecipients"] = row.addresses
                doc["capturedAt"] = Timestamp.now()
                doc["deviceDedupKey"] = row.dedupKey
                doc["ingestedAt"] = FieldValue.serverTimestamp()
                batch.set(
                    threadRef.collection(COL_MESSAGE).document(DartDocId.docIdFor(row.dedupKey)),
                    doc,
                    SetOptions.merge(),
                )
                count++
                val incumbent = newestPerThread[threadId]
                if (incumbent == null || row.timestamp > incumbent.timestamp) {
                    newestPerThread[threadId] = row
                }
                if (row.timestamp > newest) newest = row.timestamp
            }
            if (count == 0) {
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
                    text = row.text,
                    senderName = s.memberName,
                    atMillis = row.timestamp,
                    extra = mapOf(
                        "deviceSmsThreadId" to row.threadId,
                        "deviceRecipients" to row.addresses,
                    ),
                )
            }

            // Only now: the batch is durable.
            if (newest > 0L) reader.ack(newest)
            if (rows.size < SENT_PAGE_SIZE) break
        }
        return written
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
    ) {
        companion object {
            @Suppress("UNCHECKED_CAST")
            fun from(m: Map<String, Any?>): SentRow? {
                val text = (m["text"] as? String)?.takeIf { it.isNotEmpty() } ?: return null
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
                )
            }
        }
    }

    /** The member's device threads, indexed for filing sent texts. Mirrors
     *  `_DeviceThreadIndex`. */
    private class ThreadIndex(
        private val byHandsetThread: HashMap<String, String>,
        private val byParties: HashMap<String, String>,
    ) {
        private val created = HashSet<String>()

        companion object {
            fun load(s: Session): ThreadIndex {
                val byHandset = HashMap<String, String>()
                val byParties = HashMap<String, String>()
                val snap = runCatching {
                    // list-cap: 500. One member's own phone; the live member
                    // has 38 device threads (measured 2026-09-06). Same cap
                    // as the Dart index.
                    await(
                        s.company.collection(COL_CONVERSATION)
                            .whereEqualTo("channel", "device")
                            .whereArrayContains("participantRefs", s.memberRef)
                            .limit(500)
                            .get(),
                    )
                }.getOrNull()
                if (snap != null) {
                    for (doc in snap.documents) {
                        val handset = (doc.get("deviceSmsThreadId") as? String)?.trim().orEmpty()
                        if (handset.isNotEmpty()) byHandset[handset] = doc.id
                        val title = (doc.get("title") as? String).orEmpty()
                        val key = partiesKey(title.split(','))
                        // First one wins: two threads with the same people
                        // keep filing into the same one.
                        if (key.isNotEmpty() && !byParties.containsKey(key)) byParties[key] = doc.id
                    }
                }
                return ThreadIndex(byHandset, byParties)
            }
        }

        fun resolve(row: SentRow): String {
            byHandsetThread[row.threadId]?.let { return it }
            val parties = ArrayList<String>(row.addresses.size)
            for (i in row.addresses.indices) {
                val name = row.names.getOrNull(i)?.trim().orEmpty()
                parties.add(if (name.isNotEmpty()) name else row.addresses[i])
            }
            val key = partiesKey(parties)
            val byTitle = byParties[key]
            val id = byTitle ?: DartDocId.deviceSmsConversationId(row.threadId)
            if (byTitle == null) {
                created.add(id)
                byParties[key] = id
            }
            byHandsetThread[row.threadId] = id
            return id
        }

        fun isNew(threadId: String): Boolean = threadId in created
    }

    // ── Thread preview ──────────────────────────────────────────────────────

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
                    doc["commContextRefs"] = listOf(s.memberRef.path)
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
 * `+18016949606` → `(801) 694-9606`, the way Google Messages titles a thread
 * with no contact. Anything that is not ten US digits comes back as typed.
 */
internal fun nationalNumber(raw: String): String {
    val digits = raw.filter { it.isDigit() }
    val local = if (digits.length == 11 && digits.startsWith("1")) digits.substring(1) else digits
    if (local.length != 10) return raw
    return "(" + local.substring(0, 3) + ") " + local.substring(3, 6) + "-" + local.substring(6)
}

/**
 * One key for "these people", however they are spelled — the Dart
 * `partiesKey`, character for character: names lower-cased, a number reduced
 * to its last ten digits, Google Messages' `~` on a non-contact RCS name
 * dropped, the result sorted and joined with `|`.
 */
internal fun partiesKey(parts: Iterable<String>): String {
    val out = ArrayList<String>()
    for (raw in parts) {
        var p = raw.trim()
        if (p.startsWith("~")) p = p.substring(1).trim()
        if (p.isEmpty()) continue
        val digits = p.filter { it.isDigit() }
        val bare = p.filter { it != ' ' && it != '(' && it != ')' && it != '+' && it != '-' && it != '.' && !it.isWhitespace() }
        if (digits.length >= 5 && digits.length == bare.length) {
            out.add(if (digits.length > 10) digits.substring(digits.length - 10) else digits)
        } else {
            out.add(p.lowercase())
        }
    }
    out.sort()
    return out.joinToString("|")
}

