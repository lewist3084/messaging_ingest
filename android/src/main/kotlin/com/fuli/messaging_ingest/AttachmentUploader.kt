package com.fuli.messaging_ingest

import android.content.Context
import android.net.Uri
import android.util.Log
import com.google.firebase.storage.FirebaseStorage
import java.io.ByteArrayOutputStream
import java.util.concurrent.TimeUnit

/**
 * Puts a captured picture somewhere the member's BROWSER can load it.
 *
 * A text message's picture lives in one of two places on the handset and
 * neither is reachable from anywhere else: an MMS part
 * (`content://mms/part/<id>`, readable under READ_SMS) or a notification data
 * URI (readable only by the listener the shade granted, and only while the
 * notification is posted). The member reads their threads on a desktop
 * browser. So the bytes have to be copied to Storage at capture time or they
 * are gone — there is no lazy fetch to fall back on later.
 *
 * ── Why this is native and not Dart ──
 *
 * Both capture paths write through [DeviceTextWriter], which runs in the
 * notification listener's process with no Flutter engine — that is the whole
 * reason a text reaches the member's computer while the app is dead. Uploading
 * from Dart instead would mean a picture message could not be written until
 * the app next ran, and a message written text-first and backfilled later
 * would render as an empty bubble that silently grows an image. One upload
 * here keeps a picture message atomic: it arrives whole or it stays queued.
 *
 * ── The size cap ──
 *
 * A carrier caps MMS near 1 MB, but a notification data URI is whatever the
 * sending app posted and RCS carries full-resolution photos. [MAX_BYTES] is
 * the ceiling this will spend on one message; past it the picture is skipped
 * and the message still lands with its text, because a text that arrives
 * without its picture beats a thread that stops moving.
 */
internal class AttachmentUploader(private val context: Context) {

    companion object {
        private const val TAG = "AttachmentUploader"

        /** Refuse to buffer more than this into memory for one picture. */
        const val MAX_BYTES = 12 * 1024 * 1024

        /** One upload's budget. Past it the message is deferred, not dropped. */
        private const val UPLOAD_TIMEOUT_S = 45L

        /**
         * Where captured pictures live. Under the bucket's catch-all rule
         * (`allow read, write: if isNamedUser()`), so this needs no rules
         * change — but it is its own prefix so a retention sweep can find
         * them without walking `file/`.
         */
        private const val PREFIX = "deviceText"
    }

    /**
     * Reads [uri] and uploads it, returning the download URL — or null when
     * the bytes could not be read, were too big, or the upload failed.
     *
     * Null is not an error the caller should retry forever: a notification's
     * URI grant dies with the notification, so a picture that could not be
     * read once will never read. The caller writes the message without it.
     */
    fun upload(
        uri: String,
        mimeType: String,
        fileName: String,
        companyId: String,
        messageDocId: String,
    ): String? {
        val bytes = readBytes(uri) ?: return null
        return try {
            val safeName = sanitize(fileName)
            val ref = FirebaseStorage.getInstance()
                .reference
                .child("$PREFIX/$companyId/$messageDocId/$safeName")
            val metadata = com.google.firebase.storage.StorageMetadata.Builder()
                .setContentType(mimeType.ifBlank { "application/octet-stream" })
                .build()
            com.google.android.gms.tasks.Tasks.await(
                ref.putBytes(bytes, metadata),
                UPLOAD_TIMEOUT_S,
                TimeUnit.SECONDS,
            )
            com.google.android.gms.tasks.Tasks.await(
                ref.downloadUrl,
                UPLOAD_TIMEOUT_S,
                TimeUnit.SECONDS,
            ).toString()
        } catch (e: Exception) {
            Log.w(TAG, "upload of $fileName failed: ${e.message}")
            null
        }
    }

    /**
     * The bytes behind a content URI, or null when it cannot be read or runs
     * past [MAX_BYTES].
     *
     * Reads one byte past the cap on purpose: a stream that is still giving
     * at MAX_BYTES + 1 is over the line, and stopping AT the cap would upload
     * a truncated image that renders as a grey box.
     */
    fun readBytes(uri: String, limit: Int = MAX_BYTES): ByteArray? = try {
        context.contentResolver.openInputStream(Uri.parse(uri))?.use { input ->
            val buffer = ByteArrayOutputStream()
            val chunk = ByteArray(64 * 1024)
            var total = 0
            while (true) {
                val read = input.read(chunk)
                if (read <= 0) break
                total += read
                if (total > limit) {
                    Log.w(TAG, "attachment past ${limit}B cap; skipped")
                    return null
                }
                buffer.write(chunk, 0, read)
            }
            buffer.toByteArray().takeIf { it.isNotEmpty() }
        }
    } catch (e: Exception) {
        // A notification URI whose grant has expired lands here, and so does
        // an MMS part row whose file the provider has already reclaimed.
        Log.w(TAG, "could not read $uri: ${e.message}")
        null
    }

    /** A Storage path segment cannot carry a slash, and should not carry the
     *  rest of what a sending client puts in a file name either. */
    private fun sanitize(fileName: String): String {
        val cleaned = fileName.trim()
            .replace(Regex("[^A-Za-z0-9._-]"), "_")
            .trimStart('.')
            .take(120)
        return cleaned.ifEmpty { "photo" }
    }
}
