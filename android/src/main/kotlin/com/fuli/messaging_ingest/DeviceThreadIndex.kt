package com.fuli.messaging_ingest

/**
 * The member's device threads, indexed for filing texts in EITHER direction
 * from ANY of the three capture paths. Mirrors `DeviceThreadIndex` in
 * `FuLi/lib/services/device_thread_index.dart` — keep them equal.
 *
 * ── Why a thread is reachable by more than its title (2026-10-23) ──
 *
 * The notification listener and the SMS/MMS provider used to meet on ONE
 * key: the parties in the thread's title. For a contact that works — both
 * sides print the contact's name. For a sender who is NOT in the phone's
 * contacts it cannot: Google Messages titles the notification with the
 * sender's RCS profile name (`~Carrie Folsom`) and never carries the number,
 * while the provider has the number and no name. The two keys could never
 * agree, so each path minted its own thread for the same person and every
 * text she sent arrived twice, on two cards, and the agent briefed on both.
 *
 * So a thread is now keyed on EVERYTHING it has been told it is:
 *
 *  - its handset SMS thread id (`deviceSmsThreadId`) and notification key
 *    (`deviceConversationKey`), as before;
 *  - the parties in its `title`, as before;
 *  - the NUMBERS it knows — `deviceRecipients`, which the sent path, the app's
 *    own composer (`phoneThreadWith`) and now the inbound paths all write;
 *  - the NAMES it has been called — `deviceAliases`, learned on a 1:1 thread
 *    from whichever path saw them.
 *
 * Every resolver tries the number before the title, and a title match on a
 * one-to-one thread is REFUSED when that thread already knows a different
 * number — two people can share a display name; the same guard
 * `phoneThreadWith` applies in the app.
 *
 * And when no key matches and a path is about to mint a new one-to-one
 * thread, [threadHoldingTwin] asks whether another of the member's threads,
 * active within the dedupe window, already holds THIS text from the other
 * capture path — the per-thread [DeviceTextWriter.ThreadDedupe] rule lifted
 * one level up. If one does, that is the thread: [adopt] moves the new keys
 * onto it and the message folds in as a twin, and the thread learns the
 * alias (the name onto the number thread, the number onto the name thread),
 * so the next capture is a plain lookup.
 *
 * Pure: no Firestore. The writer builds the [ThreadRow]s from the member's
 * `channel: 'device'` conversations and hands them in.
 */
internal class DeviceThreadIndex(rows: List<ThreadRow>) {

    /** One device conversation document, as the index needs it. */
    class ThreadRow(
        val id: String,
        val handsetThreadId: String?,
        val conversationKey: String?,
        val title: String,
        /** `deviceRecipients`: the numbers this thread is known to reach. */
        val numbers: List<String>,
        /** `deviceAliases`: the names a 1:1 thread has been called. */
        val aliases: List<String>,
        val isGroup: Boolean,
        /** `lastMessageAt` in epoch millis, null for a thread with no message yet. */
        val lastMessageMillis: Long?,
    )

    /** What [learn] found NEW about a thread, for the writer to persist. */
    class Learned(
        /** Numbers the thread did not know, as given (E.164 as the provider
         *  or the handset spelled them). */
        val numbers: List<String>,
        /** Names the thread did not know, `~` stripped. */
        val names: List<String>,
    ) {
        val isEmpty: Boolean get() = numbers.isEmpty() && names.isEmpty()
    }

    private val byHandsetThread = HashMap<String, String>()
    private val byConversationKey = HashMap<String, String>()

    /** [partiesKey] of a party set → conversation id. First one wins: two
     *  threads with the same people keep filing into the same one. */
    private val byParties = HashMap<String, String>()

    /** Conversation id → the single-party keys it is known by (1:1 only). */
    private val known = HashMap<String, MutableSet<String>>()

    /** Conversation id → the phone keys (last ten digits) it is known to reach. */
    private val numbersOf = HashMap<String, MutableSet<String>>()

    private val groups = HashSet<String>()
    private val lastMessage = HashMap<String, Long>()

    /** Conversations this sync had to create, so their first write names them. */
    private val created = HashSet<String>()

    init {
        for (row in rows) {
            row.handsetThreadId?.trim()?.takeIf { it.isNotEmpty() }?.let { byHandsetThread[it] = row.id }
            row.conversationKey?.trim()?.takeIf { it.isNotEmpty() }?.let { byConversationKey[it] = row.id }
            if (row.isGroup) groups.add(row.id)
            row.lastMessageMillis?.let { lastMessage[row.id] = it }
            // The number set first: it is the key that cannot be two people.
            registerSet(row.id, row.numbers)
            registerSet(row.id, row.title.split(','))
            for (n in row.numbers) noteNumber(row.id, n)
            if (!row.isGroup) {
                for (a in row.aliases) registerOne(row.id, a)
                if (row.numbers.size == 1) registerOne(row.id, row.numbers[0])
                val titleParts = row.title.split(',').filter { it.isNotBlank() }
                if (titleParts.size == 1) registerOne(row.id, titleParts[0])
            }
        }
    }

    // ── Resolvers ───────────────────────────────────────────────────────────

    /**
     * The conversation a captured NOTIFICATION belongs in: the thread that
     * already carries this `deviceConversationKey`, else the thread that
     * reaches these [numbers] (when the handset gave any), else the thread
     * with the same people in its title, else a new `device_<conversationKey>`.
     */
    fun resolveInbound(
        conversationKey: String,
        title: String?,
        isGroup: Boolean,
        numbers: List<String>,
    ): String {
        byConversationKey[conversationKey]?.let { return it }
        val numberKey = partiesKey(numbers)
        val titleKey = partiesKey(title.orEmpty().split(','))
        val found = (if (numberKey.isEmpty()) null else byParties[numberKey])
            ?: titleMatch(titleKey, isGroup, numbers)
        val id = found ?: DartDocId.deviceConversationId(conversationKey).also { created.add(it) }
        if (isGroup) groups.add(id)
        byConversationKey[conversationKey] = id
        if (numberKey.isNotEmpty()) byParties.putIfAbsent(numberKey, id)
        if (titleKey.isNotEmpty()) byParties.putIfAbsent(titleKey, id)
        return id
    }

    /**
     * The conversation a text the member SENT belongs in: by handset thread,
     * by the recipient NUMBERS, by the recipients as the title would print
     * them (contact name where the phone has one), else a new
     * `device_sms_<threadId>`.
     */
    fun resolveSent(handsetThreadId: String, addresses: List<String>, names: List<String?>): String {
        byHandsetThread[handsetThreadId]?.let { return it }
        val numberKey = partiesKey(addresses)
        val parties = ArrayList<String>(addresses.size)
        for (i in addresses.indices) {
            val name = names.getOrNull(i)?.trim().orEmpty()
            parties.add(if (name.isNotEmpty()) name else addresses[i])
        }
        val isGroup = addresses.size > 1
        val mixedKey = partiesKey(parties)
        val found = byParties[numberKey] ?: titleMatch(mixedKey, isGroup, addresses)
        val id = found ?: DartDocId.deviceSmsConversationId(handsetThreadId).also { created.add(it) }
        if (isGroup) groups.add(id)
        byHandsetThread[handsetThreadId] = id
        if (numberKey.isNotEmpty()) byParties.putIfAbsent(numberKey, id)
        if (mixedKey.isNotEmpty()) byParties.putIfAbsent(mixedKey, id)
        return id
    }

    /**
     * The conversation an INBOUND provider row belongs in, or null when it
     * cannot be placed safely: by handset thread, by the sender's NUMBER, by
     * the sender as a title would print them, else — for a text that names
     * nobody else — a new `device_sms_<threadId>`.
     *
     * 🛑 An MMS that names other recipients beside us may be a GROUP, and
     * which of those addresses is the phone's own number is not knowable
     * here. Nothing is created for it; the notification path, which carries
     * the group's title, is the one that files it.
     *
     * 🛑 And such a row is NEVER placed by its SENDER (2026-10-26). Fred
     * texted his brother and his father in one MMS; the listener filed the
     * new group "Dad, Fred Lewis", and this path — matching the sender's
     * number, then his name — filed the same text into Fred's one-to-one
     * thread, which then also learned the GROUP's handset thread id and
     * would have taken every later group MMS by direct match. The member
     * saw the text twice, once under the group and once under Fred. A row
     * that names others is placed only by a handset thread id the index
     * already knows, which only a group thread that has sent or received
     * through this path can hold.
     */
    fun resolveInbox(
        handsetThreadId: String,
        sender: String,
        displayName: String,
        others: List<String>,
    ): String? {
        byHandsetThread[handsetThreadId]?.let { return it }
        if (others.isNotEmpty()) return null
        val numberKey = partiesKey(listOf(sender))
        val nameKey = partiesKey(listOf(displayName))
        val found = byParties[numberKey] ?: titleMatch(nameKey, false, listOf(sender))
        if (found != null) {
            byHandsetThread[handsetThreadId] = found
            return found
        }
        val id = DartDocId.deviceSmsConversationId(handsetThreadId)
        created.add(id)
        byHandsetThread[handsetThreadId] = id
        if (numberKey.isNotEmpty()) byParties.putIfAbsent(numberKey, id)
        if (nameKey.isNotEmpty()) byParties.putIfAbsent(nameKey, id)
        return id
    }

    /**
     * A title match, refused on a one-to-one thread that already knows a
     * DIFFERENT number than the one this capture carries: two people may
     * share a display name, and a number is the one key that cannot.
     */
    private fun titleMatch(key: String, isGroup: Boolean, numbers: List<String>): String? {
        if (key.isEmpty()) return null
        val id = byParties[key] ?: return null
        if (isGroup || numbers.isEmpty()) return id
        val knownNumbers = numbersOf[id] ?: return id
        if (knownNumbers.isEmpty()) return id
        return if (numbers.any { phoneKey(it) in knownNumbers }) id else null
    }

    // ── The cross-thread twin join ──────────────────────────────────────────

    /**
     * Among the member's OTHER one-to-one threads active within the dedupe
     * window of [atMillis], the first for which [probe] says it already
     * holds this very text from the other capture path — or null. A thread
     * that knows its numbers and none of [numbers] is never a candidate.
     *
     * Only called when [newId] was just minted: an existing thread found by
     * a key is trusted, and a group never adopts or is adopted.
     */
    fun threadHoldingTwin(
        newId: String,
        atMillis: Long,
        numbers: List<String>,
        probe: (threadId: String) -> Boolean,
    ): String? {
        val mine = numbers.map { phoneKey(it) }.filter { it.isNotEmpty() }
        val candidates = lastMessage.entries
            .filter { (id, last) ->
                id != newId && id !in groups && id !in created && last >= atMillis - TWIN_WINDOW_MS
            }
            .sortedByDescending { it.value }
            .map { it.key }
        for (id in candidates) {
            val theirs = numbersOf[id].orEmpty()
            if (mine.isNotEmpty() && theirs.isNotEmpty() && mine.none { it in theirs }) continue
            if (probe(id)) return id
        }
        return null
    }

    /** Everything that pointed at the just-minted [newId] now points at [into]. */
    fun adopt(newId: String, into: String) {
        if (newId == into) return
        for ((k, v) in byHandsetThread.entries) if (v == newId) byHandsetThread[k] = into
        for ((k, v) in byConversationKey.entries) if (v == newId) byConversationKey[k] = into
        for ((k, v) in byParties.entries) if (v == newId) byParties[k] = into
        known.remove(newId)?.let { known.getOrPut(into) { HashSet() }.addAll(it) }
        numbersOf.remove(newId)?.let { numbersOf.getOrPut(into) { HashSet() }.addAll(it) }
        created.remove(newId)
        groups.remove(newId)
    }

    // ── Learning ────────────────────────────────────────────────────────────

    /**
     * Tells the index what a capture filed into [threadId] knew about the
     * other party — numbers and names, in any order, blanks ignored — and
     * answers what the THREAD did not know yet, for the writer to persist
     * (`deviceRecipients` / `deviceAliases`). A group learns nothing: its
     * identity is the whole party set, already keyed. The member's own name
     * is not an identity of the other party; callers leave it out.
     */
    fun learn(threadId: String, identities: Iterable<String?>, isGroup: Boolean): Learned {
        if (isGroup || threadId in groups) return Learned(emptyList(), emptyList())
        val newNumbers = ArrayList<String>()
        val newNames = ArrayList<String>()
        for (raw in identities) {
            var p = raw?.trim().orEmpty()
            if (p.startsWith("~")) p = p.substring(1).trim()
            if (p.isEmpty()) continue
            val key = partiesKey(listOf(p))
            if (key.isEmpty()) continue
            val fresh = registerOne(threadId, p)
            if (phoneKey(p).isNotEmpty()) {
                if (noteNumber(threadId, p) || fresh) newNumbers.add(p)
            } else if (fresh) {
                newNames.add(p)
            }
        }
        return Learned(newNumbers.distinct(), newNames.distinct())
    }

    /** Whether this index had to create [threadId] during this sync. */
    fun isNew(threadId: String): Boolean = threadId in created

    /** The numbers [threadId] is known to reach (phone keys), for callers
     *  deciding whether a thread already has a number at all. */
    fun knowsANumber(threadId: String): Boolean = numbersOf[threadId].orEmpty().isNotEmpty()

    // ── Keys ────────────────────────────────────────────────────────────────

    private fun registerSet(id: String, parts: List<String>) {
        val key = partiesKey(parts)
        if (key.isNotEmpty()) byParties.putIfAbsent(key, id)
    }

    /** Registers one party as a key of a 1:1 thread. True when it was new to it. */
    private fun registerOne(id: String, party: String): Boolean {
        val key = partiesKey(listOf(party))
        if (key.isEmpty()) return false
        byParties.putIfAbsent(key, id)
        return known.getOrPut(id) { HashSet() }.add(key)
    }

    /** Notes a number the thread reaches. True when it was new to it. */
    private fun noteNumber(id: String, number: String): Boolean {
        val key = phoneKey(number)
        if (key.isEmpty()) return false
        return numbersOf.getOrPut(id) { HashSet() }.add(key)
    }

    companion object {
        /** The dedupe window, `ThreadDedupe.WINDOW_MS`: how far apart the two
         *  copies of one text have been observed to land (~2 s), with slack. */
        const val TWIN_WINDOW_MS = 120_000L

        /** A number's key: its last ten digits; empty for anything that is
         *  not a number. The same reduction [partiesKey] applies. */
        fun phoneKey(raw: String): String {
            val p = raw.trim()
            val digits = p.filter { it.isDigit() }
            val bare = p.filter { it != ' ' && it != '(' && it != ')' && it != '+' && it != '-' && it != '.' && !it.isWhitespace() }
            if (digits.length < 5 || digits.length != bare.length) return ""
            return if (digits.length > 10) digits.substring(digits.length - 10) else digits
        }
    }
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
