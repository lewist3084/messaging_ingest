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
class SentMessage {
  const SentMessage({
    required this.dedupKey,
    required this.kind,
    required this.threadId,
    required this.addresses,
    required this.names,
    required this.text,
    required this.timestamp,
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

  bool get isGroup => addresses.length > 1;

  static SentMessage? fromMap(Map<dynamic, dynamic> map) {
    final text = map['text'] as String?;
    final dedupKey = map['dedupKey'] as String?;
    if (text == null || text.isEmpty || dedupKey == null) return null;
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
      text: text,
      timestamp: DateTime.fromMillisecondsSinceEpoch(ms is int ? ms : 0),
    );
  }
}
