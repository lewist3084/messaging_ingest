package com.fuli.messaging_ingest

import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction

/**
 * Reads a CONTACT CARD someone texted — the vCard an iPhone's "Share Contact"
 * or Android's "Send contact" attaches to a message — into the map the
 * message document stores as `attachments[i].contact`.
 *
 * Until 2026-10-15 every capture path kept images only and dropped the vCard,
 * so a shared contact reached the thread as the words "Contact card" with
 * nothing behind them. Now the card is parsed where it is captured, because
 * the member reads threads in a desktop browser and fetching the raw `.vcf`
 * from Storage there needs CORS the bucket does not grant.
 *
 * 🛑 This is a PORT of `comms_kit/lib/src/conversation/shared_contact.dart`
 * (`parseVCards`, `SharedContact.toMap`), and the two must produce the same
 * shape — the Dart drain, this native writer and the Twilio webhook's
 * `vcard.js` all write the same field. Change one, change all three; the
 * fixtures in `comms_kit/test/fixtures/vcard/` are the shared statement of
 * what "the same" means (`src/test/kotlin/.../VCardParserTest.kt` reads them).
 *
 * Pure JVM on purpose — no `android.*` — so it is unit-testable off-device.
 *
 * Senders seen in the wild: iOS writes 3.0 (UTF-8, `item1.TEL` groups labelled
 * by `item1.X-ABLabel`, a PHOTO line tens of KB long); Android contacts write
 * 2.1 (`TEL;CELL:`, `ENCODING=QUOTED-PRINTABLE;CHARSET=UTF-8` with `=` soft
 * line breaks). The photo is never kept: it is the bulk of the file and
 * nothing renders it.
 */
internal object VCardParser {

    /**
     * The MIME types a vCard travels under, or a `.vcf` name when the sending
     * client stamped something generic. Twin of Dart's `isVCardMime`.
     */
    fun isVCardMime(mimeType: String?, fileName: String? = null): Boolean {
        val m = (mimeType ?: "").lowercase().split(';').first().trim()
        if (m == "text/vcard" ||
            m == "text/x-vcard" ||
            m == "text/directory" ||
            m == "text/x-vcalendar-contact" ||
            m == "application/vcard"
        ) {
            return true
        }
        return (fileName ?: "").lowercase().trim().endsWith(".vcf")
    }

    /**
     * Every card in a `.vcf`, each as a `SharedContact.toMap()`-shaped map.
     * A file may carry several; cards with nothing fileable are dropped.
     */
    fun parse(source: String): List<Map<String, Any>> {
        val out = ArrayList<Map<String, Any>>()
        var card: CardBuilder? = null
        for (line in unfold(source)) {
            val prop = Property.parse(line) ?: continue
            if (prop.name == "BEGIN" && prop.rawValue.uppercase() == "VCARD") {
                card = CardBuilder()
                continue
            }
            if (prop.name == "END" && prop.rawValue.uppercase() == "VCARD") {
                card?.build()?.let { out.add(it) }
                card = null
                continue
            }
            card?.add(prop)
        }
        return out
    }

    /** The bytes of a `.vcf` as text: UTF-8 (what every phone writes), else
     *  Latin-1 so a 2.1 card in a legacy charset still yields its numbers. */
    fun decode(bytes: ByteArray): String = decodeUtf8OrLatin1(bytes)

    /** The name a card shows, for a preview line — "" when it has none. */
    fun nameOf(contact: Map<String, Any>): String = (contact["name"] as? String).orEmpty()

    /**
     * The words a sender's app puts BESIDE a vCard instead of a caption —
     * Google Messages' notification reads "Contact card"; others say
     * "Contact" or "vCard". Beside a parsed card they are noise, and on a
     * copy captured WITHOUT the card they are the twin of one captured with
     * it (see `ThreadDedupe`). Twin of Dart's `isContactCardPlaceholder`.
     */
    fun isPlaceholderText(text: String?): Boolean =
        (text ?: "").trim().lowercase() in PLACEHOLDERS

    private val PLACEHOLDERS = setOf(
        "contact card",
        "contact",
        "vcard",
        "v-card",
        "shared contact",
        "contact attached",
    )

    // ── Parsing ─────────────────────────────────────────────────────────────

    private fun str(v: String?): String? {
        if (v == null) return null
        val t = v.trim()
        return if (t.isEmpty()) null else t
    }

    /**
     * Logical lines: RFC 6350 folding (CRLF + space/tab) is joined, and a 2.1
     * QUOTED-PRINTABLE value's `=` soft break is joined to the next line.
     */
    private fun unfold(source: String): List<String> {
        val raw = source.replace("\r\n", "\n").replace("\r", "\n").split("\n")
        val out = ArrayList<String>()
        var qpOpen = false
        for (line in raw) {
            if (out.isNotEmpty() && qpOpen) {
                val last = out[out.size - 1]
                out[out.size - 1] = last.substring(0, last.length - 1) + line
            } else if (out.isNotEmpty() &&
                line.isNotEmpty() &&
                (line[0] == ' ' || line[0] == '\t')
            ) {
                out[out.size - 1] = out[out.size - 1] + line.substring(1)
            } else {
                out.add(line)
            }
            val last = out[out.size - 1]
            qpOpen = last.endsWith("=") &&
                last.uppercase().contains("QUOTED-PRINTABLE") &&
                last.contains(':')
        }
        return out.filter { it.trim().isNotEmpty() }
    }

    private class Property(
        val group: String?,
        val name: String,
        val params: Map<String, List<String>>,
        val rawValue: String,
    ) {
        companion object {
            fun parse(line: String): Property? {
                // The value starts at the first ':' outside a quoted parameter.
                var inQuote = false
                var colon = -1
                for (i in line.indices) {
                    val c = line[i]
                    if (c == '"') inQuote = !inQuote
                    if (c == ':' && !inQuote) {
                        colon = i
                        break
                    }
                }
                if (colon <= 0) return null
                val head = line.substring(0, colon)
                val value = line.substring(colon + 1)
                val segs = head.split(';')
                var name = segs.first().trim()
                var group: String? = null
                val dot = name.indexOf('.')
                if (dot > 0) {
                    group = name.substring(0, dot).lowercase()
                    name = name.substring(dot + 1)
                }
                val params = LinkedHashMap<String, MutableList<String>>()
                for (seg in segs.drop(1)) {
                    val s = seg.trim()
                    if (s.isEmpty()) continue
                    val eq = s.indexOf('=')
                    if (eq < 0) {
                        // vCard 2.1 bare parameter: `TEL;CELL;VOICE:` means
                        // TYPE=CELL,VOICE; `;QUOTED-PRINTABLE` is an ENCODING.
                        val up = s.uppercase()
                        if (up == "QUOTED-PRINTABLE" || up == "BASE64" || up == "B") {
                            params.getOrPut("ENCODING") { ArrayList() }.add(up)
                        } else {
                            params.getOrPut("TYPE") { ArrayList() }.add(up)
                        }
                        continue
                    }
                    val key = s.substring(0, eq).trim().uppercase()
                    val vals = s.substring(eq + 1)
                        .replace("\"", "")
                        .split(',')
                        .map { it.trim() }
                        .filter { it.isNotEmpty() }
                    params.getOrPut(key) { ArrayList() }
                        .addAll(if (key == "TYPE") vals.map { it.uppercase() } else vals)
                }
                return Property(group, name.uppercase(), params, value)
            }
        }

        val isQuotedPrintable: Boolean
            get() = (params["ENCODING"] ?: emptyList())
                .any { it.uppercase() == "QUOTED-PRINTABLE" }

        val types: List<String>
            get() = params["TYPE"] ?: emptyList()

        /** The value decoded — quoted-printable bytes as UTF-8 — then split
         *  on unescaped [sep]. */
        fun components(sep: Char = ';'): List<String> {
            var v = rawValue
            if (isQuotedPrintable) v = decodeQuotedPrintable(v)
            val parts = ArrayList<String>()
            val buf = StringBuilder()
            var i = 0
            while (i < v.length) {
                val c = v[i]
                if (c == '\\' && i + 1 < v.length) {
                    val n = v[i + 1]
                    buf.append(if (n == 'n' || n == 'N') '\n' else n)
                    i += 2
                    continue
                } else if (c == sep) {
                    parts.add(buf.toString())
                    buf.setLength(0)
                } else {
                    buf.append(c)
                }
                i++
            }
            parts.add(buf.toString())
            return parts.map { it.trim() }
        }

        val text: String
            get() = components(sep = '\u0000').first()
    }

    private fun decodeQuotedPrintable(v: String): String {
        val bytes = java.io.ByteArrayOutputStream()
        var i = 0
        while (i < v.length) {
            val c = v[i]
            if (c == '=' && i + 2 < v.length) {
                val hex = v.substring(i + 1, i + 3).toIntOrNull(16)
                if (hex != null && hex in 0..255) {
                    bytes.write(hex)
                    i += 3
                    continue
                }
            }
            val b = c.toString().toByteArray(Charsets.UTF_8)
            bytes.write(b, 0, b.size)
            i++
        }
        return decodeUtf8OrLatin1(bytes.toByteArray())
    }

    private fun decodeUtf8OrLatin1(bytes: ByteArray): String = try {
        Charsets.UTF_8.newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT)
            .decode(ByteBuffer.wrap(bytes))
            .toString()
    } catch (e: CharacterCodingException) {
        String(bytes, Charsets.ISO_8859_1)
    }

    /** A TYPE list as the word a member would read. iOS's own label
     *  (`X-ABLabel`, by group) wins over this. */
    private fun labelOf(types: List<String>): String? {
        for (t in types) {
            val w = when (t.uppercase()) {
                "CELL", "MOBILE" -> "mobile"
                "IPHONE" -> "iPhone"
                "HOME" -> "home"
                "WORK" -> "work"
                "MAIN" -> "main"
                "FAX" -> "fax"
                "PAGER" -> "pager"
                "OTHER" -> "other"
                else -> null
            }
            if (w != null) return w
        }
        return null
    }

    // `\\\$` = a regex-escaped literal `$` (a bare `$_` would be a Kotlin
    // string template).
    private val AB_LABEL = Regex("^_\\\$!<(.+)>!\\\$_\$")

    /** iOS writes `_$!<Mobile>!$_` for a built-in label and plain text for
     *  one the member typed. */
    private fun cleanAbLabel(raw: String?): String? {
        if (raw == null) return null
        val m = AB_LABEL.find(raw.trim())
        val s = (m?.groupValues?.get(1) ?: raw).trim()
        return if (s.isEmpty()) null else s.lowercase()
    }

    private class Value(val group: String?, val value: String, val label: String?)

    private class Address(
        val group: String?,
        val label: String?,
        val street: String?,
        val city: String?,
        val region: String?,
        val postalCode: String?,
        val country: String?,
    ) {
        /** One line, the way an envelope reads: street, city, region postal,
         *  country. Same as Dart's `SharedContactAddress.formatted`. */
        val formatted: String
            get() {
                val cityLine = listOf(
                    city.orEmpty(),
                    listOf(region, postalCode).filter { !it.isNullOrEmpty() }.joinToString(" "),
                ).filter { it.isNotEmpty() }.joinToString(", ")
                return listOf(street, cityLine, country)
                    .filter { !it.isNullOrBlank() }
                    .joinToString(", ") { it!!.trim() }
            }

        val isEmpty: Boolean get() = formatted.isEmpty()

        fun toMap(labelOverride: String?): Map<String, Any> {
            val m = LinkedHashMap<String, Any>()
            (labelOverride ?: label)?.let { m["label"] = it }
            street?.let { m["street"] = it }
            city?.let { m["city"] = it }
            region?.let { m["region"] = it }
            postalCode?.let { m["postalCode"] = it }
            country?.let { m["country"] = it }
            m["formatted"] = formatted
            return m
        }
    }

    private class CardBuilder {
        var fn: String? = null
        var n: List<String>? = null
        var org: String? = null
        var title: String? = null
        var note: String? = null
        var bday: String? = null
        val phones = ArrayList<Value>()
        val emails = ArrayList<Value>()
        val urls = ArrayList<Value>()
        val addresses = ArrayList<Address>()
        val abLabels = HashMap<String, String>()

        fun add(p: Property) {
            when (p.name) {
                "FN" -> fn = str(p.text)
                "N" -> n = p.components()
                "ORG" -> org = str(p.components().filter { it.isNotEmpty() }.joinToString(", "))
                "TITLE" -> title = str(p.text)
                "NOTE" -> note = str(p.text)
                "BDAY" -> bday = str(p.text)
                "TEL" -> {
                    val v = str(p.text.replaceFirst(Regex("^tel:", RegexOption.IGNORE_CASE), ""))
                    if (v != null) phones.add(Value(p.group, v, labelOf(p.types)))
                }
                "EMAIL" -> {
                    val v = str(p.text.replaceFirst(Regex("^mailto:", RegexOption.IGNORE_CASE), ""))
                    if (v != null) emails.add(Value(p.group, v, labelOf(p.types)))
                }
                "URL" -> {
                    val v = str(p.text)
                    if (v != null) urls.add(Value(p.group, v, labelOf(p.types)))
                }
                "ADR" -> {
                    // PO box ; extended ; street ; locality ; region ; postal ; country
                    val c = p.components()
                    fun at(i: Int): String? = if (i < c.size) str(c[i]) else null
                    val street = listOfNotNull(at(0), at(1), at(2))
                        .joinToString(", ")
                        .replace("\n", ", ")
                    val a = Address(
                        group = p.group,
                        label = labelOf(p.types),
                        street = str(street),
                        city = at(3),
                        region = at(4),
                        postalCode = at(5),
                        country = at(6),
                    )
                    if (!a.isEmpty) addresses.add(a)
                }
                "X-ABLABEL" -> {
                    val g = p.group
                    val l = cleanAbLabel(p.text)
                    if (g != null && l != null) abLabels[g] = l
                }
            }
        }

        private fun valueMaps(values: List<Value>): List<Map<String, Any>> = values.map { e ->
            val label = (if (e.group == null) null else abLabels[e.group]) ?: e.label
            val m = LinkedHashMap<String, Any>()
            m["value"] = e.value
            if (label != null) m["label"] = label
            m
        }

        fun build(): Map<String, Any>? {
            val parts = n
            val given = if (parts != null && parts.size > 1) str(parts[1]) else null
            val family = if (parts != null && parts.isNotEmpty()) str(parts[0]) else null
            val assembled = listOfNotNull(
                if (parts != null && parts.size > 3) str(parts[3]) else null,
                given,
                if (parts != null && parts.size > 2) str(parts[2]) else null,
                family,
                if (parts != null && parts.size > 4) str(parts[4]) else null,
            ).joinToString(" ")
            val name = fn ?: str(assembled) ?: org ?: ""

            // SharedContact.isEmpty: nothing a member could file.
            if (name.trim().isEmpty() && phones.isEmpty() && emails.isEmpty()) return null

            // Same keys, same order, same omissions as SharedContact.toMap.
            val m = LinkedHashMap<String, Any>()
            m["name"] = name
            given?.let { m["givenName"] = it }
            family?.let { m["familyName"] = it }
            org?.let { m["organization"] = it }
            title?.let { m["title"] = it }
            m["phones"] = valueMaps(phones)
            m["emails"] = valueMaps(emails)
            if (urls.isNotEmpty()) m["urls"] = valueMaps(urls)
            if (addresses.isNotEmpty()) {
                m["addresses"] = addresses.map { a ->
                    a.toMap(if (a.group == null) null else abLabels[a.group])
                }
            }
            note?.let { m["note"] = it }
            bday?.let { m["birthday"] = it }
            return m
        }
    }
}
