package com.fuli.messaging_ingest

import android.content.Context
import org.json.JSONArray

/**
 * What the phone needs to know to write a captured text to Firestore on its
 * own: WHICH company, WHICH member, and as WHOM. The Dart side resolves all of
 * that (sign-in, the active membership, the member's display name) and hands
 * it down through `configure`; it is kept in the plugin's own SharedPreferences
 * so the listener service — which runs for days with no Flutter engine — has
 * it on a cold process start.
 *
 * Absent (never configured, or cleared on sign-out) means the phone cannot
 * write, and the listener falls back to the pending queue the Dart side drains
 * the next time the app runs. That is the pre-2026-09-07 behaviour, kept as
 * the safety net.
 *
 * [uid] is checked against `FirebaseAuth.currentUser` at write time: a config
 * left behind by one sign-in must never write as the next.
 */
internal class IngestConfig(context: Context) {

    private val prefs =
        context.getSharedPreferences("messaging_ingest", Context.MODE_PRIVATE)

    data class Values(
        val uid: String,
        val companyId: String,
        val memberId: String,
        val memberName: String,
        /** Packages whose MessagingStyle notifications count as the member's
         *  texts. Pushed from Dart's `kDeviceTextPackages` so there is ONE
         *  list; the two defaults below are only for a config written by an
         *  older Dart side. */
        val packages: Set<String>,
        /** The persistent-cache size the Dart side sets on its Firestore
         *  instance. The phone must start Firestore with the SAME settings —
         *  see [DeviceTextWriter.firestore]. */
        val cacheSizeBytes: Long,
    )

    companion object {
        private const val KEY_UID = "cfg_uid"
        private const val KEY_COMPANY = "cfg_companyId"
        private const val KEY_MEMBER = "cfg_memberId"
        private const val KEY_MEMBER_NAME = "cfg_memberName"
        private const val KEY_PACKAGES = "cfg_packages"
        private const val KEY_CACHE_BYTES = "cfg_cacheSizeBytes"

        val DEFAULT_PACKAGES: Set<String> = setOf(
            "com.google.android.apps.messaging",
            "com.samsung.android.messaging",
        )
    }

    @Synchronized
    fun write(v: Values) {
        val arr = JSONArray()
        v.packages.forEach { arr.put(it) }
        prefs.edit()
            .putString(KEY_UID, v.uid)
            .putString(KEY_COMPANY, v.companyId)
            .putString(KEY_MEMBER, v.memberId)
            .putString(KEY_MEMBER_NAME, v.memberName)
            .putString(KEY_PACKAGES, arr.toString())
            .putLong(KEY_CACHE_BYTES, v.cacheSizeBytes)
            .apply()
    }

    @Synchronized
    fun clear() {
        prefs.edit()
            .remove(KEY_UID)
            .remove(KEY_COMPANY)
            .remove(KEY_MEMBER)
            .remove(KEY_MEMBER_NAME)
            .remove(KEY_PACKAGES)
            .remove(KEY_CACHE_BYTES)
            .apply()
    }

    /** Null until configured, and after a clear. */
    @Synchronized
    fun read(): Values? {
        val uid = prefs.getString(KEY_UID, null)?.takeIf { it.isNotBlank() } ?: return null
        val company = prefs.getString(KEY_COMPANY, null)?.takeIf { it.isNotBlank() } ?: return null
        val member = prefs.getString(KEY_MEMBER, null)?.takeIf { it.isNotBlank() } ?: return null
        val name = prefs.getString(KEY_MEMBER_NAME, null) ?: ""
        val packages = runCatching {
            val arr = JSONArray(prefs.getString(KEY_PACKAGES, "[]"))
            val out = HashSet<String>()
            for (i in 0 until arr.length()) arr.optString(i).takeIf { it.isNotBlank() }?.let(out::add)
            out
        }.getOrElse { emptySet() }
        return Values(
            uid = uid,
            companyId = company,
            memberId = member,
            memberName = name,
            packages = if (packages.isEmpty()) DEFAULT_PACKAGES else packages,
            cacheSizeBytes = prefs.getLong(KEY_CACHE_BYTES, 50L * 1024 * 1024),
        )
    }

    fun isConfigured(): Boolean = read() != null
}
