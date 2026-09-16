package com.fuli.messaging_ingest

import android.Manifest
import android.content.Context
import android.content.pm.PackageManager
import android.net.Uri
import android.provider.ContactsContract
import android.provider.Telephony
import androidx.core.content.ContextCompat

/**
 * Reads texts the member SENT, out of the handset's own message store.
 *
 * The notification listener cannot see these: Google Messages posts a
 * MessagingStyle notification only for a message that ARRIVES, and a reply
 * typed in the app posts nothing and dismisses what was there. Measured on
 * 2026-09-06 across twelve days of live capture: 190 inbound, 0 outbound.
 *
 * So the outgoing side comes from the telephony providers instead —
 * `content://sms/sent` and `content://mms` with `msg_box = 2` — under
 * `READ_SMS`. That store holds SMS and MMS only. An RCS chat lives in Google
 * Messages' private database with no API to it, so a thread that has gone RCS
 * (any group with an iPhone on iOS 18+, most Android-to-Android chats) still
 * shows only the other side here. That is a platform ceiling, not a bug to
 * find.
 *
 * Names: the provider stores phone numbers; the captured inbound threads are
 * titled with contact display names, because that is what the notification
 * carried. Resolving a number back to the same name is what lets a sent
 * message land in the thread its reply belongs to, and needs `READ_CONTACTS`.
 * Without it the number is returned raw and the Dart side matches on digits.
 *
 * Reads are non-destructive; the caller advances [watermark] via [ack] only
 * after its own write has committed, so an interrupted sync re-reads rather
 * than loses.
 *
 * Also reads ONE conversation's past — both directions — for the history
 * pull ([readThread], [threadIdFor]). Nothing here reads the whole store.
 */
internal class SentMessageReader(private val context: Context) {

    private val prefs =
        context.getSharedPreferences("messaging_ingest", Context.MODE_PRIVATE)

    /** Number → contact name for one read. Group MMS repeats the same few
     *  recipients on every message; one PhoneLookup each is plenty. */
    private val names = HashMap<String, String?>()

    companion object {
        private const val KEY_WATERMARK = "sentWatermarkMs"

        /** PduHeaders.TO — the recipients of an outgoing MMS. */
        private const val MMS_ADDR_TO = 151

        /** PduHeaders.FROM — the sender of an incoming MMS. */
        private const val MMS_ADDR_FROM = 137

        /** The provider's placeholder for "this handset" in an MMS addr row. */
        private const val SELF_TOKEN = "insert-address-token"

        private val MMS_PART_URI: Uri = Uri.parse("content://mms/part")

        /**
         * A file extension for a captured image whose part carried no name.
         * The chip's label is the only thing the member sees, and "photo-42"
         * with no extension reads as a file nothing can open.
         */
        fun extensionFor(mimeType: String): String = when (mimeType.lowercase()) {
            "image/jpeg", "image/jpg" -> ".jpg"
            "image/png" -> ".png"
            "image/gif" -> ".gif"
            "image/webp" -> ".webp"
            "image/heic", "image/heif" -> ".heic"
            "image/bmp" -> ".bmp"
            else -> ""
        }
    }

    fun isGranted(): Boolean = held(Manifest.permission.READ_SMS)

    fun contactsGranted(): Boolean = held(Manifest.permission.READ_CONTACTS)

    private fun held(permission: String) =
        ContextCompat.checkSelfPermission(context, permission) ==
            PackageManager.PERMISSION_GRANTED

    /** Epoch millis of the newest sent message already handed over, or 0. */
    fun watermark(): Long = prefs.getLong(KEY_WATERMARK, 0L)

    /** Advances the watermark. Never moves it backwards. */
    fun ack(untilMillis: Long) {
        if (untilMillis > watermark()) {
            prefs.edit().putLong(KEY_WATERMARK, untilMillis).apply()
        }
    }

    /**
     * Sent SMS and MMS newer than [sinceMillis], oldest first, at most
     * [limit]. Oldest-first on purpose: when there are more than [limit] the
     * caller acks what it got and the next read continues from there, so a
     * backlog is walked rather than skipped.
     */
    fun read(sinceMillis: Long, limit: Int): List<Map<String, Any?>> {
        if (!isGranted()) return emptyList()
        val out = ArrayList<Map<String, Any?>>()
        runCatching { readSms(sinceMillis, limit, out) }
        runCatching { readMms(sinceMillis, limit, out) }
        out.sortBy { it["timestamp"] as Long }
        return if (out.size > limit) ArrayList(out.subList(0, limit)) else out
    }

    private fun readSms(sinceMillis: Long, limit: Int, out: MutableList<Map<String, Any?>>) {
        val projection = arrayOf(
            Telephony.Sms._ID,
            Telephony.Sms.THREAD_ID,
            Telephony.Sms.ADDRESS,
            Telephony.Sms.BODY,
            Telephony.Sms.DATE,
        )
        context.contentResolver.query(
            Telephony.Sms.Sent.CONTENT_URI,
            projection,
            "${Telephony.Sms.DATE} > ?",
            arrayOf(sinceMillis.toString()),
            "${Telephony.Sms.DATE} ASC LIMIT $limit",
        )?.use { c ->
            val iId = c.getColumnIndexOrThrow(Telephony.Sms._ID)
            val iThread = c.getColumnIndexOrThrow(Telephony.Sms.THREAD_ID)
            val iAddr = c.getColumnIndexOrThrow(Telephony.Sms.ADDRESS)
            val iBody = c.getColumnIndexOrThrow(Telephony.Sms.BODY)
            val iDate = c.getColumnIndexOrThrow(Telephony.Sms.DATE)
            while (c.moveToNext()) {
                val body = c.getString(iBody) ?: continue
                if (body.isBlank()) continue
                val address = c.getString(iAddr)?.trim().orEmpty()
                if (address.isEmpty()) continue
                out.add(
                    payload(
                        kind = "sms",
                        id = c.getLong(iId),
                        threadId = c.getLong(iThread),
                        addresses = listOf(address),
                        text = body,
                        timestamp = c.getLong(iDate),
                    )
                )
            }
        }
    }

    private fun readMms(sinceMillis: Long, limit: Int, out: MutableList<Map<String, Any?>>) {
        // MMS dates are stored in SECONDS, unlike SMS. Every other column that
        // looks the same is the same; this one is the trap.
        val sinceSeconds = sinceMillis / 1000
        val projection = arrayOf(
            Telephony.Mms._ID,
            Telephony.Mms.THREAD_ID,
            Telephony.Mms.DATE,
        )
        context.contentResolver.query(
            Telephony.Mms.CONTENT_URI,
            projection,
            "${Telephony.Mms.MESSAGE_BOX} = ? AND ${Telephony.Mms.DATE} > ?",
            arrayOf(Telephony.Mms.MESSAGE_BOX_SENT.toString(), sinceSeconds.toString()),
            "${Telephony.Mms.DATE} ASC LIMIT $limit",
        )?.use { c ->
            val iId = c.getColumnIndexOrThrow(Telephony.Mms._ID)
            val iThread = c.getColumnIndexOrThrow(Telephony.Mms.THREAD_ID)
            val iDate = c.getColumnIndexOrThrow(Telephony.Mms.DATE)
            while (c.moveToNext()) {
                val id = c.getLong(iId)
                // 🛑 A picture message carries no text/plain part at all.
                // Until 2026-09-08 the `?: continue` here dropped it on the
                // floor: a photo texted from the handset's own Messages app
                // reached the thread as NOTHING, and a captioned one as the
                // caption alone with no sign a picture had gone with it.
                // Keep the row when either half is present.
                val text = mmsText(id).orEmpty()
                val attachments = mmsAttachments(id)
                if (text.isBlank() && attachments.isEmpty()) continue
                val recipients = mmsRecipients(id)
                if (recipients.isEmpty()) continue
                out.add(
                    payload(
                        kind = "mms",
                        id = id,
                        threadId = c.getLong(iThread),
                        addresses = recipients,
                        text = text,
                        timestamp = c.getLong(iDate) * 1000,
                        attachments = attachments,
                    )
                )
            }
        }
    }

    /** The text/plain parts of one MMS, joined. Null when it carried none —
     *  a picture-only message has nothing to file as a text. */
    private fun mmsText(id: Long): String? {
        val parts = ArrayList<String>()
        context.contentResolver.query(
            MMS_PART_URI,
            arrayOf(
                Telephony.Mms.Part._ID,
                Telephony.Mms.Part.CONTENT_TYPE,
                Telephony.Mms.Part.TEXT,
                Telephony.Mms.Part._DATA,
            ),
            "${Telephony.Mms.Part.MSG_ID} = ?",
            arrayOf(id.toString()),
            null,
        )?.use { c ->
            val iId = c.getColumnIndexOrThrow(Telephony.Mms.Part._ID)
            val iCt = c.getColumnIndexOrThrow(Telephony.Mms.Part.CONTENT_TYPE)
            val iText = c.getColumnIndexOrThrow(Telephony.Mms.Part.TEXT)
            val iData = c.getColumnIndexOrThrow(Telephony.Mms.Part._DATA)
            while (c.moveToNext()) {
                if (c.getString(iCt) != "text/plain") continue
                val inline = c.getString(iText)
                val text = if (!c.isNull(iData) && inline.isNullOrEmpty()) {
                    // A long text part is spilled to a file the provider serves
                    // back by part id. Read it; never touch _data's path.
                    val partUri = Uri.withAppendedPath(MMS_PART_URI, c.getLong(iId).toString())
                    runCatching {
                        context.contentResolver.openInputStream(partUri)?.use {
                            it.readBytes().toString(Charsets.UTF_8)
                        }
                    }.getOrNull()
                } else {
                    inline
                }
                if (!text.isNullOrBlank()) parts.add(text)
            }
        }
        return if (parts.isEmpty()) null else parts.joinToString("\n")
    }

    /**
     * The IMAGE parts of one MMS, as handles the writer can fetch bytes for.
     *
     * Images only, deliberately. A picture is what a text message actually
     * carries; the SMIL layout part is markup, and a vCard or an audio clip
     * would each need their own rendering before uploading one was worth the
     * bytes. Everything skipped here is skipped visibly — see the caller.
     *
     * The part is NOT read here. A provider read plus a Storage upload per
     * message, on the thread walking a backlog, is exactly the work that must
     * not happen during a sync — so this returns the part's content URI and
     * the upload happens once, at write time, for a message that is actually
     * new. [MessagingIngestPlugin] exposes the same fetch to the Dart drain.
     */
    private fun mmsAttachments(id: Long): List<Map<String, Any?>> {
        val out = ArrayList<Map<String, Any?>>()
        context.contentResolver.query(
            MMS_PART_URI,
            arrayOf(
                Telephony.Mms.Part._ID,
                Telephony.Mms.Part.CONTENT_TYPE,
                Telephony.Mms.Part.NAME,
                Telephony.Mms.Part.FILENAME,
            ),
            "${Telephony.Mms.Part.MSG_ID} = ?",
            arrayOf(id.toString()),
            null,
        )?.use { c ->
            val iPart = c.getColumnIndexOrThrow(Telephony.Mms.Part._ID)
            val iCt = c.getColumnIndexOrThrow(Telephony.Mms.Part.CONTENT_TYPE)
            val iName = c.getColumnIndexOrThrow(Telephony.Mms.Part.NAME)
            val iFile = c.getColumnIndexOrThrow(Telephony.Mms.Part.FILENAME)
            while (c.moveToNext()) {
                val contentType = c.getString(iCt)?.trim().orEmpty().lowercase()
                if (!contentType.startsWith("image/")) continue
                val partId = c.getLong(iPart)
                // The provider fills one of these, neither, or both, depending
                // on the sending client. A name the member recognises is nice;
                // a name at all is required, because it is the chip's label.
                val named = c.getString(iFile)?.trim().takeUnless { it.isNullOrEmpty() }
                    ?: c.getString(iName)?.trim().takeUnless { it.isNullOrEmpty() }
                out.add(
                    hashMapOf(
                        "uri" to Uri.withAppendedPath(
                            MMS_PART_URI,
                            partId.toString(),
                        ).toString(),
                        "mimeType" to contentType,
                        "fileName" to (named ?: "photo-$partId${extensionFor(contentType)}"),
                    )
                )
            }
        }
        return out
    }

    /** TO addresses of one MMS, minus the handset's own placeholder. */
    private fun mmsRecipients(id: Long): List<String> {
        val out = ArrayList<String>()
        val uri = Uri.parse("content://mms/$id/addr")
        context.contentResolver.query(
            uri,
            arrayOf(Telephony.Mms.Addr.ADDRESS, Telephony.Mms.Addr.TYPE),
            "${Telephony.Mms.Addr.TYPE} = ?",
            arrayOf(MMS_ADDR_TO.toString()),
            null,
        )?.use { c ->
            val iAddr = c.getColumnIndexOrThrow(Telephony.Mms.Addr.ADDRESS)
            while (c.moveToNext()) {
                val a = c.getString(iAddr)?.trim().orEmpty()
                if (a.isEmpty() || a == SELF_TOKEN) continue
                if (a !in out) out.add(a)
            }
        }
        return out
    }

    // ── One conversation's history ──────────────────────────────────────────

    /**
     * One handset conversation's SMS and MMS, BOTH directions, older than
     * [beforeMillis]: the newest [limit] of them, returned oldest first. Each
     * row is the [read] shape plus `outgoing` and, for an incoming one,
     * `sender` / `senderName`. Dedup keys are the provider row ids [read] uses,
     * so a sent text seen by both paths is the same document.
     */
    fun readThread(threadId: Long, beforeMillis: Long, limit: Int): List<Map<String, Any?>> {
        if (!isGranted()) return emptyList()
        val out = ArrayList<Map<String, Any?>>()
        runCatching { readThreadSms(threadId, beforeMillis, limit, out) }
        runCatching { readThreadMms(threadId, beforeMillis, limit, out) }
        out.sortByDescending { it["timestamp"] as Long }
        val newest = ArrayList(if (out.size > limit) out.subList(0, limit) else out)
        newest.sortBy { it["timestamp"] as Long }
        return newest
    }

    /**
     * The handset conversation [number] texts in, found among messages the
     * phone already stores — never created (`Threads.getOrCreateThreadId`
     * would add an empty thread to the member's Messages). Matched on the
     * last ten digits, however the provider spelled the address. Null when
     * there is no SMS with that number at all.
     */
    fun threadIdFor(number: String): String? {
        if (!isGranted()) return null
        val digits = number.filter { it.isDigit() }
        if (digits.length < 7) return null
        val last10 = if (digits.length > 10) digits.takeLast(10) else digits
        context.contentResolver.query(
            Telephony.Sms.CONTENT_URI,
            arrayOf(Telephony.Sms.THREAD_ID, Telephony.Sms.ADDRESS),
            "${Telephony.Sms.ADDRESS} LIKE ?",
            arrayOf("%" + digits.takeLast(4)),
            "${Telephony.Sms.DATE} DESC LIMIT 50",
        )?.use { c ->
            while (c.moveToNext()) {
                val d = c.getString(1)?.filter { it.isDigit() }.orEmpty()
                val tail = if (d.length > 10) d.takeLast(10) else d
                if (tail == last10) return c.getLong(0).toString()
            }
        }
        return null
    }

    private fun readThreadSms(
        threadId: Long,
        beforeMillis: Long,
        limit: Int,
        out: MutableList<Map<String, Any?>>,
    ) {
        context.contentResolver.query(
            Telephony.Sms.CONTENT_URI,
            arrayOf(
                Telephony.Sms._ID,
                Telephony.Sms.ADDRESS,
                Telephony.Sms.BODY,
                Telephony.Sms.DATE,
                Telephony.Sms.TYPE,
            ),
            "${Telephony.Sms.THREAD_ID} = ? AND ${Telephony.Sms.DATE} < ? AND " +
                "${Telephony.Sms.TYPE} IN (?, ?)",
            arrayOf(
                threadId.toString(),
                beforeMillis.toString(),
                Telephony.Sms.MESSAGE_TYPE_INBOX.toString(),
                Telephony.Sms.MESSAGE_TYPE_SENT.toString(),
            ),
            "${Telephony.Sms.DATE} DESC LIMIT $limit",
        )?.use { c ->
            val iId = c.getColumnIndexOrThrow(Telephony.Sms._ID)
            val iAddr = c.getColumnIndexOrThrow(Telephony.Sms.ADDRESS)
            val iBody = c.getColumnIndexOrThrow(Telephony.Sms.BODY)
            val iDate = c.getColumnIndexOrThrow(Telephony.Sms.DATE)
            val iType = c.getColumnIndexOrThrow(Telephony.Sms.TYPE)
            while (c.moveToNext()) {
                val body = c.getString(iBody) ?: continue
                if (body.isBlank()) continue
                val address = c.getString(iAddr)?.trim().orEmpty()
                val outgoing = c.getInt(iType) == Telephony.Sms.MESSAGE_TYPE_SENT
                out.add(
                    historyPayload(
                        kind = "sms",
                        id = c.getLong(iId),
                        threadId = threadId,
                        outgoing = outgoing,
                        sender = if (outgoing || address.isEmpty()) null else address,
                        addresses = if (address.isEmpty()) emptyList() else listOf(address),
                        text = body,
                        timestamp = c.getLong(iDate),
                    )
                )
            }
        }
    }

    private fun readThreadMms(
        threadId: Long,
        beforeMillis: Long,
        limit: Int,
        out: MutableList<Map<String, Any?>>,
    ) {
        // MMS dates are SECONDS — see readMms.
        context.contentResolver.query(
            Telephony.Mms.CONTENT_URI,
            arrayOf(Telephony.Mms._ID, Telephony.Mms.DATE, Telephony.Mms.MESSAGE_BOX),
            "${Telephony.Mms.THREAD_ID} = ? AND ${Telephony.Mms.DATE} < ? AND " +
                "${Telephony.Mms.MESSAGE_BOX} IN (?, ?)",
            arrayOf(
                threadId.toString(),
                (beforeMillis / 1000).toString(),
                Telephony.Mms.MESSAGE_BOX_INBOX.toString(),
                Telephony.Mms.MESSAGE_BOX_SENT.toString(),
            ),
            "${Telephony.Mms.DATE} DESC LIMIT $limit",
        )?.use { c ->
            val iId = c.getColumnIndexOrThrow(Telephony.Mms._ID)
            val iDate = c.getColumnIndexOrThrow(Telephony.Mms.DATE)
            val iBox = c.getColumnIndexOrThrow(Telephony.Mms.MESSAGE_BOX)
            while (c.moveToNext()) {
                val id = c.getLong(iId)
                val text = mmsText(id).orEmpty()
                val attachments = mmsAttachments(id)
                if (text.isBlank() && attachments.isEmpty()) continue
                val outgoing = c.getInt(iBox) == Telephony.Mms.MESSAGE_BOX_SENT
                out.add(
                    historyPayload(
                        kind = "mms",
                        id = id,
                        threadId = threadId,
                        outgoing = outgoing,
                        sender = if (outgoing) null else mmsFrom(id),
                        addresses = if (outgoing) mmsRecipients(id) else emptyList(),
                        text = text,
                        timestamp = c.getLong(iDate) * 1000,
                        attachments = attachments,
                    )
                )
            }
        }
    }

    /** The FROM address of one incoming MMS, or null. */
    private fun mmsFrom(id: Long): String? {
        context.contentResolver.query(
            Uri.parse("content://mms/$id/addr"),
            arrayOf(Telephony.Mms.Addr.ADDRESS),
            "${Telephony.Mms.Addr.TYPE} = ?",
            arrayOf(MMS_ADDR_FROM.toString()),
            null,
        )?.use { c ->
            while (c.moveToNext()) {
                val a = c.getString(0)?.trim().orEmpty()
                if (a.isNotEmpty() && a != SELF_TOKEN) return a
            }
        }
        return null
    }

    private fun historyPayload(
        kind: String,
        id: Long,
        threadId: Long,
        outgoing: Boolean,
        sender: String?,
        addresses: List<String>,
        text: String,
        timestamp: Long,
        attachments: List<Map<String, Any?>> = emptyList(),
    ): Map<String, Any?> = hashMapOf(
        "dedupKey" to "$kind|$id",
        "kind" to kind,
        "threadId" to threadId.toString(),
        "outgoing" to outgoing,
        "sender" to sender,
        "senderName" to sender?.let { nameFor(it) },
        "addresses" to ArrayList(addresses),
        "text" to text,
        "timestamp" to timestamp,
        "attachments" to ArrayList(attachments),
    )

    private fun payload(
        kind: String,
        id: Long,
        threadId: Long,
        addresses: List<String>,
        text: String,
        timestamp: Long,
        attachments: List<Map<String, Any?>> = emptyList(),
    ): Map<String, Any?> {
        val resolved = ArrayList<String?>(addresses.size)
        for (a in addresses) resolved.add(nameFor(a))
        return hashMapOf(
            // Provider row ids are stable for the life of the store; a
            // reinstall re-reads and rewrites the same ids, which is the
            // idempotency the Firestore side relies on.
            "dedupKey" to "$kind|$id",
            "kind" to kind,
            "threadId" to threadId.toString(),
            "addresses" to ArrayList(addresses),
            "names" to resolved,
            "text" to text,
            "timestamp" to timestamp,
            "attachments" to ArrayList(attachments),
        )
    }

    /** Contact display name for a number, or null without a contact (or
     *  without the permission). A short code has no contact either. */
    private fun nameFor(address: String): String? {
        if (!contactsGranted()) return null
        return names.getOrPut(address) {
            runCatching {
                val uri = Uri.withAppendedPath(
                    ContactsContract.PhoneLookup.CONTENT_FILTER_URI,
                    Uri.encode(address),
                )
                context.contentResolver.query(
                    uri,
                    arrayOf(ContactsContract.PhoneLookup.DISPLAY_NAME),
                    null,
                    null,
                    null,
                )?.use { c ->
                    if (c.moveToFirst()) c.getString(0)?.takeIf { it.isNotBlank() } else null
                }
            }.getOrNull()
        }
    }
}
