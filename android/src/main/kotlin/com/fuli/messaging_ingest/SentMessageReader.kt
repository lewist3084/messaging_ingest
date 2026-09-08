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

        /** The provider's placeholder for "this handset" in an MMS addr row. */
        private const val SELF_TOKEN = "insert-address-token"

        private val MMS_PART_URI: Uri = Uri.parse("content://mms/part")
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
                val text = mmsText(id) ?: continue
                if (text.isBlank()) continue
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

    private fun payload(
        kind: String,
        id: Long,
        threadId: Long,
        addresses: List<String>,
        text: String,
        timestamp: Long,
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
