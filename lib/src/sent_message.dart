/// One text the member SENT, read from the handset's SMS/MMS store.
///
/// The notification listener never sees these: Google Messages posts a
/// notification only for a message that arrives. This is the other half of a
/// conversation, and it covers SMS and MMS only — an RCS chat has no readable
/// store on Android, so a thread that has gone RCS still shows one side.
///
/// Recipients come back as the raw provider address ([addresses]) and, where
/// the phone holds a contact for it and READ_CONTACTS was granted, that
/// contact's display name in the same position of [names]. The captured
/// inbound thread is titled with exactly those display names, which is what
/// the consumer matches on to file this in the right conversation.
/// One picture carried by a sent MMS.
///
/// [uri] is a `content://mms/part/<id>` handle readable only inside the app's
/// own process under READ_SMS — the bytes are fetched with
/// `MessagingIngest.readAttachmentBytes` and uploaded, because the member
/// reads their threads in a browser that can reach neither.
class SentAttachment {
  const SentAttachment({
    required this.uri,
    required this.mimeType,
    required this.fileName,
  });

  final String uri;

  /// Always `image/*`: the reader keeps pictures and skips the SMIL layout
  /// part, vCards and audio, none of which a bubble can render.
  final String mimeType;
  final String fileName;

  static SentAttachment? fromMap(Map<dynamic, dynamic> map) {
    final uri = (map['uri'] as String?)?.trim();
    if (uri == null || uri.isEmpty) return null;
    return SentAttachment(
      uri: uri,
      mimeType: (map['mimeType'] as String?)?.trim().isNotEmpty == true
          ? (map['mimeType'] as String).trim()
          : 'image/jpeg',
      fileName: (map['fileName'] as String?)?.trim().isNotEmpty == true
          ? (map['fileName'] as String).trim()
          : 'photo',
    );
  }
}

class SentMessage {
  const SentMessage({
    required this.dedupKey,
    required this.kind,
    required this.threadId,
    required this.addresses,
    required this.names,
    required this.text,
    required this.timestamp,
    this.attachments = const <SentAttachment>[],
  });

  /// `sms|<row id>` or `mms|<row id>`. Provider row ids are stable for the
  /// life of the store, so this is safe as a document id.
  final String dedupKey;

  /// `sms` or `mms`.
  final String kind;

  /// The telephony provider's conversation id. Stable per recipient set on
  /// one handset; it is NOT the notification's conversation key.
  final String threadId;

  /// Recipient addresses as the provider stores them (E.164 or short code).
  final List<String> addresses;

  /// Contact display name per recipient, null where there is none.
  final List<String?> names;

  final String text;

  /// When it was sent, per the provider.
  final DateTime timestamp;

  /// The pictures it carried. An MMS sent from the handset's own Messages app
  /// is very often a picture and NOTHING else.
  final List<SentAttachment> attachments;

  bool get isGroup => addresses.length > 1;

  static SentMessage? fromMap(Map<dynamic, dynamic> map) {
    final text = map['text'] as String?;
    final dedupKey = map['dedupKey'] as String?;
    final attachments = ((map['attachments'] as List?) ?? const [])
        .whereType<Map<dynamic, dynamic>>()
        .map(SentAttachment.fromMap)
        .whereType<SentAttachment>()
        .toList(growable: false);
    if (dedupKey == null) return null;
    // 🛑 A picture message has no text/plain part at all. Requiring text here
    // was the third of three places a sent photo vanished without a trace.
    if ((text == null || text.isEmpty) && attachments.isEmpty) return null;
    final addresses = (map['addresses'] as List?)
            ?.map((e) => e?.toString() ?? '')
            .where((e) => e.isNotEmpty)
            .toList(growable: false) ??
        const <String>[];
    if (addresses.isEmpty) return null;
    final rawNames = (map['names'] as List?) ?? const [];
    final names = List<String?>.generate(
      addresses.length,
      (i) => i < rawNames.length ? rawNames[i] as String? : null,
      growable: false,
    );
    final ms = map['timestamp'];
    return SentMessage(
      dedupKey: dedupKey,
      kind: (map['kind'] as String?) ?? 'sms',
      threadId: (map['threadId'] ?? '').toString(),
      addresses: addresses,
      names: names,
      text: text ?? '',
      timestamp: DateTime.fromMillisecondsSinceEpoch(ms is int ? ms : 0),
      attachments: attachments,
    );
  }
}
