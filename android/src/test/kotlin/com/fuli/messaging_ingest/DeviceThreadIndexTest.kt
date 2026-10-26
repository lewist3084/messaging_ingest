package com.fuli.messaging_ingest

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * The law of the thread index (2026-10-23), the same cases as
 * `FuLi/test/device_thread_index_test.dart`: a number beats a title, a
 * title never crosses a known different number, and a name-only capture
 * meets its number-only twin through the cross-thread join and the thread
 * learns the alias.
 *
 *     cd <app>/android && ./gradlew :messaging_ingest:testDebugUnitTest
 */
class DeviceThreadIndexTest {

    private val carrieNumber = "+19723659039"
    private val now = 1_761_200_000_000L

    private fun row(
        id: String,
        title: String,
        numbers: List<String> = emptyList(),
        aliases: List<String> = emptyList(),
        handset: String? = null,
        convKey: String? = null,
        isGroup: Boolean = false,
        last: Long? = now,
    ) = DeviceThreadIndex.ThreadRow(
        id = id,
        handsetThreadId = handset,
        conversationKey = convKey,
        title = title,
        numbers = numbers,
        aliases = aliases,
        isGroup = isGroup,
        lastMessageMillis = last,
    )

    @Test
    fun partiesKeyStripsTheTildeAndReducesNumbers() {
        assertEquals("carrie folsom", partiesKey(listOf("~Carrie Folsom")))
        assertEquals("9723659039", partiesKey(listOf("(972) 365-9039")))
        assertEquals("9723659039", partiesKey(listOf("+19723659039")))
        assertEquals("9723659039|carrie folsom", partiesKey(listOf("Carrie Folsom", "+19723659039")))
        assertEquals("", partiesKey(listOf("  ", "~")))
    }

    @Test
    fun aNameAndANumberAreDifferentKeys_theGapThisIndexCloses() {
        assertNotEquals(partiesKey(listOf("~Carrie Folsom")), partiesKey(listOf("(972) 365-9039")))
    }

    // ── Number before title ───────────────────────────────────────────────

    @Test
    fun inboundWithANumberFindsTheNumberThreadOverANameMatch() {
        val idx = DeviceThreadIndex(
            listOf(
                row("byName", "Carrie Folsom"),
                row("byNumber", "(972) 365-9039", numbers = listOf(carrieNumber)),
            ),
        )
        val id = idx.resolveInbound("conv-1", "~Carrie Folsom", false, listOf(carrieNumber))
        assertEquals("byNumber", id)
        assertFalse(idx.isNew(id))
    }

    @Test
    fun sentToANumberFindsTheThreadThatLearnedIt() {
        val idx = DeviceThreadIndex(listOf(row("carrie", "~Carrie Folsom", numbers = listOf(carrieNumber))))
        val id = idx.resolveSent("handset-7", listOf(carrieNumber), listOf(null))
        assertEquals("carrie", id)
        // Remembered: the next sent row is a handset lookup.
        assertEquals("carrie", idx.resolveSent("handset-7", listOf("+10000000000"), listOf(null)))
    }

    @Test
    fun inboxRowFindsTheThreadByTheSendersNumberThenByAlias() {
        val idx = DeviceThreadIndex(
            listOf(
                row("carrie", "~Carrie Folsom", numbers = listOf(carrieNumber)),
                row("sam", "(801) 555-0100", aliases = listOf("Sam Davis")),
            ),
        )
        assertEquals("carrie", idx.resolveInbox("h1", carrieNumber, "(972) 365-9039", emptyList()))
        assertEquals("sam", idx.resolveInbox("h2", "+18015550199", "Sam Davis", emptyList()))
    }

    // ── A title never crosses a known different number ────────────────────

    @Test
    fun aTitleMatchIsRefusedWhenTheThreadKnowsAnotherNumber() {
        val idx = DeviceThreadIndex(listOf(row("sam1", "Sam Davis", numbers = listOf("+18015550100"))))
        val id = idx.resolveInbox("h9", "+19725550100", "Sam Davis", emptyList())
        assertNotEquals("sam1", id)
        assertTrue(idx.isNew(id!!))
    }

    @Test
    fun aTitleMatchStandsWhenTheThreadKnowsNoNumber() {
        val idx = DeviceThreadIndex(listOf(row("sam", "Sam Davis")))
        assertEquals("sam", idx.resolveInbox("h9", "+19725550100", "Sam Davis", emptyList()))
    }

    @Test
    fun aGroupMmsThatNamesOthersIsNotPlacedByThisPath() {
        val idx = DeviceThreadIndex(emptyList())
        assertNull(idx.resolveInbox("h3", carrieNumber, "(972) 365-9039", listOf("+18015550100")))
    }

    /** The Fred Lewis case (2026-10-26): a group MMS from a sender whose
     *  one-to-one thread the index knows — by name AND by number — is still
     *  not the one-to-one thread's. Only a handset thread id places it. */
    @Test
    fun aGroupMmsNeverLandsInTheSendersOneToOneThread() {
        val fred = "+16787278712"
        val idx = DeviceThreadIndex(
            listOf(
                row("fred", "Fred Lewis", numbers = listOf(fred), handset = "7"),
                row("family", "Dad, Fred Lewis", isGroup = true, convKey = "1123"),
            ),
        )
        assertNull(idx.resolveInbox("2067", fred, "Fred Lewis", listOf("+18015550100")))
        // His one-to-one text still files where it always did.
        assertEquals("fred", idx.resolveInbox("7", fred, "Fred Lewis", emptyList()))
        // Once the group thread knows its handset thread, the row is a lookup.
        idx.resolveSent("2067", listOf(fred, "+18015550100"), listOf("Fred Lewis", "Dad"))
        assertEquals("family", idx.resolveInbox("2067", fred, "Fred Lewis", listOf("+18015550100")))
    }

    // ── The cross-thread join: the Carrie Folsom case ─────────────────────

    @Test
    fun theProviderCopyOfANameOnlyThreadJoinsItAndTheThreadLearnsTheNumber() {
        // The listener filed Carrie's text under her RCS name, with no number.
        val idx = DeviceThreadIndex(listOf(row("carrie", "~Carrie Folsom", convKey = "conv-c", last = now)))
        // Seconds later the provider hands the same text over by number alone.
        val minted = idx.resolveInbox("handset-42", carrieNumber, "(972) 365-9039", emptyList())!!
        assertTrue(idx.isNew(minted))
        assertNotEquals("carrie", minted)

        val probed = ArrayList<String>()
        val into = idx.threadHoldingTwin(minted, now + 2_000, listOf(carrieNumber)) { id ->
            probed.add(id)
            id == "carrie"
        }
        assertEquals("carrie", into)
        assertEquals(listOf("carrie"), probed)

        idx.adopt(minted, into!!)
        assertFalse(idx.isNew(minted))
        // Every key the minted thread had now reaches Carrie's.
        assertEquals("carrie", idx.resolveInbox("handset-42", carrieNumber, "(972) 365-9039", emptyList()))

        val learned = idx.learn("carrie", listOf(carrieNumber, null), false)
        assertEquals(listOf(carrieNumber), learned.numbers)
        assertTrue(learned.names.isEmpty())
        assertTrue(idx.knowsANumber("carrie"))
        // And the next time it is a plain lookup, before any join.
        assertEquals("carrie", idx.resolveInbound("conv-other", "~Carrie Folsom", false, listOf(carrieNumber)))
        assertEquals("carrie", idx.resolveSent("handset-99", listOf(carrieNumber), listOf(null)))
    }

    @Test
    fun theNotificationCopyOfANumberOnlyThreadJoinsItAndTheThreadLearnsTheName() {
        // The provider filed first (the thread was open on the phone, so no
        // notification posted); the name arrives with a later notification.
        val idx = DeviceThreadIndex(
            listOf(row("num", "(972) 365-9039", numbers = listOf(carrieNumber), handset = "h5", last = now)),
        )
        val minted = idx.resolveInbound("conv-c", "~Carrie Folsom", false, emptyList())
        assertTrue(idx.isNew(minted))
        val into = idx.threadHoldingTwin(minted, now + 1_500, emptyList()) { it == "num" }
        assertEquals("num", into)
        idx.adopt(minted, "num")
        val learned = idx.learn("num", listOf("~Carrie Folsom"), false)
        assertEquals(listOf("Carrie Folsom"), learned.names)
        assertTrue(learned.numbers.isEmpty())
        // The conversation key moved with it.
        assertEquals("num", idx.resolveInbound("conv-c", null, false, emptyList()))
        // And the alias is now a title key of its own — for a capture that
        // names no number. One that names a DIFFERENT number is someone else.
        assertEquals("num", idx.resolveInbound("conv-z", "Carrie Folsom", false, emptyList()))
        assertNotEquals("num", idx.resolveInbox("h6", "+10000000000", "Carrie Folsom", emptyList()))
    }

    @Test
    fun aThreadThatKnowsADifferentNumberIsNeverACandidate() {
        val idx = DeviceThreadIndex(listOf(row("sam", "Sam Davis", numbers = listOf("+18015550100"), last = now)))
        val minted = idx.resolveInbox("h7", carrieNumber, "(972) 365-9039", emptyList())!!
        val probed = ArrayList<String>()
        val into = idx.threadHoldingTwin(minted, now, listOf(carrieNumber)) { probed.add(it); true }
        assertNull(into)
        assertTrue(probed.isEmpty())
    }

    @Test
    fun groupsQuietThreadsAndThreadsMintedThisPassAreNeverCandidates() {
        val idx = DeviceThreadIndex(
            listOf(
                row("group", "Carrie Folsom, Sam Davis", isGroup = true, last = now),
                row("quiet", "~Carrie Folsom", last = now - DeviceThreadIndex.TWIN_WINDOW_MS - 1),
                row("silent", "~Carrie Folsom", last = null),
            ),
        )
        val first = idx.resolveInbound("conv-x", "~Someone Else", false, emptyList())
        val minted = idx.resolveInbox("h8", carrieNumber, "(972) 365-9039", emptyList())!!
        val probed = ArrayList<String>()
        assertNull(idx.threadHoldingTwin(minted, now, listOf(carrieNumber)) { probed.add(it); true })
        assertTrue(probed.isEmpty())
        assertTrue(idx.isNew(first))
    }

    @Test
    fun theNewestActiveThreadIsProbedFirst() {
        val idx = DeviceThreadIndex(
            listOf(
                row("older", "~A", last = now - 60_000),
                row("newer", "~B", last = now - 1_000),
            ),
        )
        val minted = idx.resolveInbox("h8", carrieNumber, "(972) 365-9039", emptyList())!!
        val probed = ArrayList<String>()
        idx.threadHoldingTwin(minted, now, emptyList()) { probed.add(it); false }
        assertEquals(listOf("newer", "older"), probed)
    }

    // ── Learning ──────────────────────────────────────────────────────────

    @Test
    fun learnReportsOnlyWhatTheThreadDidNotKnow() {
        val idx = DeviceThreadIndex(listOf(row("carrie", "~Carrie Folsom", numbers = listOf(carrieNumber))))
        val first = idx.learn("carrie", listOf(carrieNumber, "Carrie Folsom", "", "~"), false)
        assertTrue(first.isEmpty)
        val second = idx.learn("carrie", listOf("Carrie F.", "(972) 365-9039"), false)
        assertEquals(listOf("Carrie F."), second.names)
        // The same number in another spelling is the number it already knows.
        assertTrue(second.numbers.isEmpty())
    }

    @Test
    fun aGroupLearnsNothing() {
        val idx = DeviceThreadIndex(listOf(row("g", "A, B", isGroup = true)))
        assertTrue(idx.learn("g", listOf("+18015550100", "A"), true).isEmpty)
        assertTrue(idx.learn("g", listOf("+18015550100", "A"), false).isEmpty)
    }

    @Test
    fun phoneKeyIsTheLastTenDigitsOrNothing() {
        assertEquals("9723659039", DeviceThreadIndex.phoneKey("+1 (972) 365-9039"))
        assertEquals("", DeviceThreadIndex.phoneKey("Carrie Folsom"))
        assertEquals("", DeviceThreadIndex.phoneKey("88607x"))
    }
}
