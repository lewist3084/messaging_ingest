package com.fuli.messaging_ingest

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.File

/**
 * The plugin's half of the three-parser contract (2026-10-15). Reads the SAME
 * fixtures as `comms_kit/test/shared_contact_test.dart` and expects the same
 * answers — the Dart drain and this native writer file the same field.
 *
 *     cd <app>/android && ./gradlew :messaging_ingest:testDebugUnitTest
 */
class VCardParserTest {

    /** Walks up from the module dir to the workspace's comms_kit fixtures. */
    private fun fixture(name: String): String {
        var dir: File? = File(System.getProperty("user.dir")).absoluteFile
        while (dir != null) {
            val f = File(dir, "comms_kit/test/fixtures/vcard/$name")
            if (f.isFile) return f.readText(Charsets.UTF_8)
            dir = dir.parentFile
        }
        error("fixture $name not found above ${System.getProperty("user.dir")}")
    }

    @Suppress("UNCHECKED_CAST")
    private fun list(m: Map<String, Any>, key: String) = m[key] as List<Map<String, Any>>

    @Test
    fun ios30GroupsAbLabelFoldedPhotoEscapes() {
        val cards = VCardParser.parse(fixture("ios_3_0.vcf"))
        assertEquals(1, cards.size)
        val c = cards.single()
        assertEquals("James Lecheminant", c["name"])
        assertEquals("James", c["givenName"])
        assertEquals("Lecheminant", c["familyName"])
        assertEquals("Provo Stake", c["organization"])
        assertEquals("Facilities Rep", c["title"])
        val phones = list(c, "phones")
        assertEquals(listOf("(801) 555-0134", "+1 801 555 0199"), phones.map { it["value"] })
        assertEquals("mobile", phones.first()["label"])
        assertEquals("work", phones.last()["label"])
        assertEquals("james@example.com", list(c, "emails").single()["value"])
        val adr = list(c, "addresses").single()
        assertEquals("home", adr["label"])
        assertEquals("123 N Main St, Apt 4, Provo, UT 84604, United States", adr["formatted"])
        assertEquals("Building access, keys", c["note"])
        assertEquals("1975-03-02", c["birthday"])
        assertFalse(c.containsKey("urls"))
    }

    @Test
    fun android21QuotedPrintableBareTypesSeveralCards() {
        val cards = VCardParser.parse(fixture("android_2_1_two_cards.vcf"))
        // The empty third card is dropped.
        assertEquals(2, cards.size)
        val c = cards.first()
        assertEquals("José Muñoz", c["name"])
        assertEquals("José", c["givenName"])
        assertEquals("mobile", list(c, "phones").first()["label"])
        assertEquals("home", list(c, "phones").last()["label"])
        assertEquals("45 Long Street North", list(c, "addresses").single()["street"])
        assertEquals("Orem", list(c, "addresses").single()["city"])
        // A card with only a number is still a card.
        assertEquals("", cards.last()["name"])
        assertEquals("801-555-0123", list(cards.last(), "phones").single()["value"])
    }

    @Test
    fun isVCardMime() {
        assertTrue(VCardParser.isVCardMime("text/x-vcard"))
        assertTrue(VCardParser.isVCardMime("text/vcard; charset=utf-8"))
        assertTrue(VCardParser.isVCardMime("application/octet-stream", fileName = "Sam.VCF"))
        assertFalse(VCardParser.isVCardMime("image/jpeg"))
    }
}
