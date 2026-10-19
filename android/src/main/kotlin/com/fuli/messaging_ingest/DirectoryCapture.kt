package com.fuli.messaging_ingest

import com.google.firebase.firestore.DocumentReference
import com.google.firebase.firestore.DocumentSnapshot
import com.google.firebase.firestore.Source
import com.google.android.gms.tasks.Tasks
import java.util.concurrent.TimeUnit

/**
 * Who a member's own phone brings texts in FROM, and whether the directory
 * knows a given party.
 *
 * 🛑 Mirrors `shared_widgets/lib/messaging/device_capture_scope.dart` line
 * for line — the scope's two values, how an older member document is read,
 * what "in the directory" means, and how a party is looked up. The phone
 * files most captures itself, inside the notification listener, so a rule
 * that lives only in Dart filters almost nothing. Change both or neither.
 *
 * The scope is ONE choice on the member doc (`deviceIncomingScope`):
 * [EVERYONE] — every text the phone sees comes in (FuLi, the personal app) —
 * or [DIRECTORY] — only a text to or from someone in the company's directory
 * (kleenops, Eagle Mountain). The directory IS the list: adding someone is how
 * they come in, archiving them is how they stop. The hand-kept include /
 * exclude list it replaced (2026-09-15 → 2026-10-15) is read only to derive
 * the scope of a phone set up before the field existed.
 *
 * "In the directory" is a `person` row a human has ruled on: a member's own
 * row (`memberId` set), or a contact whose standing is PRIMARY (`status:
 * 'active'`, or no status at all). A `suggested` row — minted by the server
 * for every unknown sender the phone files — does NOT count, or the scope
 * would mean "everyone who ever texted". An `archived` row does not count:
 * that is the member saying no.
 *
 * A number is looked up through the person's `partyKeys`
 * (`phone:<last ten digits>`, derived on the server) and, for a row written
 * before those keys existed, the bare `phones` list under every spelling the
 * app has stored a number in. A notification carries a contact NAME and no
 * digits, so a name is first resolved to a number through the phone's own
 * contacts (the caller hands in the resolver) and, failing that, matched
 * exactly against the person's `name`. A group is in when anyone in it is.
 *
 * 🛑 A lookup that cannot reach the server THROWS rather than answering.
 * "Not found" drops a text for good; "could not look" must leave it where it
 * is for the next try. Every read here is [Source.SERVER] for that reason —
 * the default source would answer from an empty cache, offline, as "not
 * found".
 *
 * Answers are cached for [TTL_MS] across sessions (one process, one phone):
 * a conversation is one read, and a person added to the directory is in
 * before their next reply lands.
 */
internal class DirectoryCapture(private val company: DocumentReference) {

    companion object {
        const val FIELD_SCOPE = "deviceIncomingScope"
        const val EVERYONE = "everyone"
        const val DIRECTORY = "directory"

        /** The retired list's switch and mode — read to migrate, never written. */
        private const val FIELD_LIMIT = "deviceIncomingLimit"
        private const val FIELD_MODE = "deviceIncomingMode"

        private const val COL_PERSON = "person"
        private const val TTL_MS = 5L * 60 * 1000
        private const val MAX_PARTIES = 12
        private const val READ_TIMEOUT_S = 25L

        /** Shared across sessions: `(known, answeredAtMillis)` per party key. */
        private val cache = HashMap<String, Pair<Boolean, Long>>()

        /**
         * The scope a member document says. Mirrors `deviceIncomingScopeOf`,
         * without the app default: the app writes the explicit field on setup,
         * before this writer is configured, so a document with neither field
         * is one the app has not touched since — and those were all FuLi's,
         * where everything comes in. A kleenops phone always carried the
         * retired switch, seeded on setup, which reads as the directory.
         */
        fun scopeOf(data: Map<String, Any?>): String {
            val explicit = data[FIELD_SCOPE]
            if (explicit == EVERYONE || explicit == DIRECTORY) return explicit as String
            if (data.containsKey(FIELD_LIMIT)) {
                val limited = data[FIELD_LIMIT] == true
                val exclude = data[FIELD_MODE] == "exclude"
                return if (limited && !exclude) DIRECTORY else EVERYONE
            }
            return EVERYONE
        }

        /** Mirrors Dart `personIsInDirectory`. */
        fun personIsInDirectory(data: Map<String, Any?>): Boolean {
            if (data["isDeleted"] == true) return false
            if ((data["mergedInto"] as? String)?.trim().orEmpty().isNotEmpty()) return false
            if ((data["memberId"] as? String)?.trim().orEmpty().isNotEmpty()) return true
            val status = (data["status"] ?: "active").toString()
            return status == "active"
        }

        /** Just the digits. Mirrors Dart `phoneDigits`. */
        private fun phoneDigits(raw: String): String = raw.filter { it.isDigit() }

        /** The ten significant digits, or "" for fewer than seven. Mirrors
         *  Dart `phoneMatchKey`. */
        fun phoneMatchKey(raw: String): String {
            val digits = phoneDigits(raw)
            if (digits.length < 7) return ""
            return if (digits.length > 10) digits.substring(digits.length - 10) else digits
        }

        /** A number rather than a name: digits and the punctuation a number is
         *  written with, seven digits or more. Mirrors Dart `isPhoneNumberText`. */
        fun isPhoneNumberText(raw: String): Boolean {
            val t = raw.trim()
            if (t.isEmpty()) return false
            for (ch in t) {
                if (!ch.isDigit() && ch != ' ' && ch != '(' && ch != ')' && ch != '+' &&
                    ch != '.' && ch != '-' && !ch.isWhitespace()
                ) return false
            }
            return phoneDigits(t).length >= 7
        }

        /** The spellings one number may already be stored under — at most ten,
         *  `whereArrayContainsAny`'s ceiling. Mirrors Dart `phoneVariants`. */
        fun phoneVariants(raw: String): List<String> {
            val digits = phoneDigits(raw)
            if (digits.length < 7) return emptyList()
            val local = if (digits.length == 11 && digits.startsWith("1")) digits.substring(1) else digits
            val out = LinkedHashSet<String>()
            out.add("+$digits")
            out.add(digits)
            if (local.length == 10) {
                val a = local.substring(0, 3)
                val b = local.substring(3, 6)
                val c = local.substring(6)
                out.add("+1$local")
                out.add("1$local")
                out.add(local)
                out.add("($a) $b-$c")
                out.add("$a-$b-$c")
                out.add("$a.$b.$c")
            }
            return out.take(10)
        }
    }

    private val people get() = company.collection(COL_PERSON)

    /**
     * True when any of [parties] — numbers, or the contact names a
     * notification carries — is in the directory. [resolveNumber] turns a
     * contact name into a number through the phone's contacts, or null.
     * Throws when the server could not be asked; a cached answer never does.
     */
    fun knowsAny(parties: List<String?>, resolveNumber: (String) -> String?): Boolean {
        val seen = HashSet<String>()
        var asked = 0
        for (raw in parties) {
            var party = raw?.trim().orEmpty()
            // Google Messages prefixes a non-contact RCS name with `~`.
            if (party.startsWith("~")) party = party.substring(1).trim()
            if (party.isEmpty() || !seen.add(party.lowercase())) continue
            if (++asked > MAX_PARTIES) break
            if (knows(party, resolveNumber)) return true
        }
        return false
    }

    /** One party: a number, or a name. */
    fun knows(party: String, resolveNumber: (String) -> String?): Boolean {
        if (isPhoneNumberText(party)) {
            val key = phoneMatchKey(party)
            return if (key.isEmpty()) false else knownNumber(key, party)
        }
        val number = resolveNumber(party)
        if (number != null && phoneMatchKey(number).isNotEmpty()) {
            if (knownNumber(phoneMatchKey(number), number)) return true
        }
        return knownName(party)
    }

    private fun knownNumber(key: String, raw: String): Boolean = remember("n:$key") {
        val keyed = read(
            people.whereArrayContains("partyKeys", "phone:$key")
                // One number is one person; a handful covers duplicates
                // awaiting merge.
                .limit(5),
        )
        if (keyed.any { personIsInDirectory(it.data.orEmpty()) }) return@remember true
        // A row written before `partyKeys` were derived.
        val variants = phoneVariants(raw)
        if (variants.isEmpty()) return@remember false
        val legacy = read(people.whereArrayContainsAny("phones", variants).limit(5))
        legacy.any { personIsInDirectory(it.data.orEmpty()) }
    }

    /** Exact equality, not a token search — see the Dart twin. */
    private fun knownName(name: String): Boolean = remember("name:${name.lowercase()}") {
        read(people.whereEqualTo("name", name).limit(5))
            .any { personIsInDirectory(it.data.orEmpty()) }
    }

    private fun read(query: com.google.firebase.firestore.Query): List<DocumentSnapshot> =
        try {
            Tasks.await(query.get(Source.SERVER), READ_TIMEOUT_S, TimeUnit.SECONDS).documents
        } catch (e: java.util.concurrent.ExecutionException) {
            throw (e.cause as? Exception) ?: e
        }

    private fun remember(key: String, ask: () -> Boolean): Boolean {
        val now = System.currentTimeMillis()
        synchronized(cache) {
            cache[key]?.let { (known, at) -> if (now - at < TTL_MS) return known }
        }
        val known = ask()
        synchronized(cache) { cache[key] = known to now }
        return known
    }
}
