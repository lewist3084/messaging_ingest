package com.fuli.messaging_ingest

import android.Manifest
import android.content.Context
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import android.provider.ContactsContract
import android.telephony.SmsManager
import androidx.core.content.ContextCompat

/**
 * Sends a text FROM THIS HANDSET on behalf of a reply queued elsewhere — the
 * member typed it on their computer, the phone is what carries it. Same
 * shape as Phone Link: the PC never talks to a carrier.
 *
 * Two routes, tried in this order by the Dart side:
 *
 *  1. The conversation's own notification, if it is still posted —
 *     [MessageNotificationListener.replyInline]. That is the only way any
 *     third-party app can answer an RCS chat, and it lands in the exact
 *     thread with no number needed. It is gone the moment the member reads
 *     the thread on the phone.
 *  2. This class: `SmsManager`, plain SMS to one number under `SEND_SMS`.
 *     Reliable, SMS only. Android writes the sent row to the SMS provider
 *     itself for an app that is not the default messenger, so
 *     [SentMessageReader] picks the text up on the next sync and it appears
 *     in the thread the normal way — nothing here writes a message.
 *
 * A group is NOT sent by route 2: `SmsManager` would fan it out as separate
 * 1:1 texts, which is a different conversation for everyone on it. A group
 * reply needs route 1, and the Dart side says so when it cannot.
 */
internal class SmsSender(private val context: Context) {

    fun isGranted(): Boolean =
        ContextCompat.checkSelfPermission(context, Manifest.permission.SEND_SMS) ==
            PackageManager.PERMISSION_GRANTED

    /** Sends [text] to one [address]. Throws on refusal or a bad number. */
    fun send(address: String, text: String) {
        require(isGranted()) { "SEND_SMS not granted" }
        require(address.isNotBlank()) { "empty address" }
        require(text.isNotBlank()) { "empty text" }
        val manager: SmsManager = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            context.getSystemService(SmsManager::class.java)
        } else {
            @Suppress("DEPRECATION")
            SmsManager.getDefault()
        }
        val parts = manager.divideMessage(text)
        if (parts.size == 1) {
            manager.sendTextMessage(address, null, text, null, null)
        } else {
            manager.sendMultipartTextMessage(address, null, parts, null, null)
        }
    }

    /**
     * The phone number behind a contact display name, or null when the phone
     * has no such contact — or more than one, because guessing which "Chris"
     * a text goes to is worse than not sending it. The captured inbound
     * thread carries only the name, so this is how a reply finds its number
     * when no sent text has taught the thread one yet.
     */
    fun resolveNumber(displayName: String): String? {
        val name = displayName.trim()
        if (name.isEmpty()) return null
        if (ContextCompat.checkSelfPermission(context, Manifest.permission.READ_CONTACTS) !=
            PackageManager.PERMISSION_GRANTED
        ) return null
        val contactIds = LinkedHashSet<Long>()
        var number: String? = null
        context.contentResolver.query(
            ContactsContract.CommonDataKinds.Phone.CONTENT_URI,
            arrayOf(
                ContactsContract.CommonDataKinds.Phone.CONTACT_ID,
                ContactsContract.CommonDataKinds.Phone.NUMBER,
                ContactsContract.CommonDataKinds.Phone.IS_PRIMARY,
            ),
            "${ContactsContract.CommonDataKinds.Phone.DISPLAY_NAME} = ?",
            arrayOf(name),
            "${ContactsContract.CommonDataKinds.Phone.IS_PRIMARY} DESC",
        )?.use { c ->
            val iContact = c.getColumnIndexOrThrow(ContactsContract.CommonDataKinds.Phone.CONTACT_ID)
            val iNumber = c.getColumnIndexOrThrow(ContactsContract.CommonDataKinds.Phone.NUMBER)
            while (c.moveToNext()) {
                contactIds.add(c.getLong(iContact))
                if (number == null) number = c.getString(iNumber)?.trim()?.takeIf { it.isNotEmpty() }
            }
        }
        return if (contactIds.size == 1) number else null
    }

    companion object {
        /** `(801) 694-9606` as a thread title is a number, not a name. */
        fun looksLikeNumber(s: String): Boolean {
            val bare = s.replace(Regex("[\\s()+\\-.]"), "")
            return bare.length >= 5 && bare.all { it.isDigit() }
        }

        fun toDialable(s: String): String = Uri.decode(s.replace(Regex("[\\s()\\-.]"), ""))
    }
}
