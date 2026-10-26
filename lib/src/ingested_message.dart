/// One message lifted out of a MessagingStyle notification.
///
/// This is a *capture*, not a wire format — it records what the notification
/// actually carried, including the gaps. [senderName] is a contact display
/// name, never a phone number: Android's `Person.uri` is optional and Google
/// Messages mostly does not populate it. When it does, [senderNumber] and
/// [partyNumbers] carry what it said (2026-10-23); when it does not, the
/// thread index joins a name-only capture to its provider copy by content.
class IngestedMessage {
  const IngestedMessage({
    required this.dedupKey,
    required this.packageName,
    required this.conversationKey,
    required this.conversationTitle,
    required this.isGroup,
    required this.senderName,
    required this.isFromMe,
    required this.text,
    required this.timestamp,
    required this.postedAt,
    required this.canReply,
    this.attachmentUri,
    this.attachmentMimeType,
    this.senderNumber,
    this.partyNumbers = const <String>[],
  });

  /// Stable identity for this message. Notifications re-post their whole recent
  /// history on every update, so the native side dedups on this before
  /// enqueueing — but it is carried through so the Firestore write can use it
  /// as a document id and stay idempotent across reinstalls.
  final String dedupKey;

  /// Originating app, e.g. `com.google.android.apps.messaging`.
  final String packageName;

  /// Stable-ish thread key: the notification's shortcut id where available,
  /// falling back to conversation title, tag, then notification key.
  final String conversationKey;

  /// Group name for group threads; usually null for one-to-one.
  final String? conversationTitle;

  final bool isGroup;

  /// Contact display name of the sender. Null when the message is ours.
  final String? senderName;

  /// The sender's number from the Person's `tel:` URI, `+` and digits, when
  /// the handset set one. Null when it did not, and when the message is ours.
  final String? senderNumber;

  /// Every `tel:` the notification named for the thread — the sender's and
  /// the people list's — in the handset's order. Empty on most RCS threads.
  final List<String> partyNumbers;

  final bool isFromMe;

  final String text;

  /// When the message was sent, per the notification.
  final DateTime timestamp;

  /// When the notification carrying it was posted. Differs from [timestamp] on
  /// the first capture after a grant, when a backlog arrives at once.
  final DateTime postedAt;

  /// Whether a free-form RemoteInput reply action was attached when captured.
  /// Goes stale as soon as the notification is dismissed — recorded so the
  /// staleness rate is measurable before any reply path is built on it.
  final bool canReply;

  /// A `content://` URI for the picture (or contact card) this message
  /// carried, when it carried one. Readable ONLY inside the app's own process, and only while the
  /// notification that granted it is posted — so it is a handle to copy from
  /// promptly, never something to store. Fetch the bytes with
  /// `MessagingIngest.readAttachmentBytes`.
  final String? attachmentUri;

  /// An `image/*` type, or a vCard type since 2026-10-15 (RCS from Google
  /// Messages may post a shared contact as the data URI). The capture side
  /// drops every other kind, because a bubble has nothing to render it with.
  final String? attachmentMimeType;

  /// A picture message has no text at all. Anything reading these must say
  /// what it shows for one — see the thread preview in `MessageIngestService`.
  bool get hasAttachment => (attachmentUri?.isNotEmpty ?? false);

  static IngestedMessage? fromMap(Map<dynamic, dynamic> map) {
    final text = map['text'] as String?;
    final dedupKey = map['dedupKey'] as String?;
    final attachmentUri = (map['attachmentUri'] as String?)?.trim();
    final hasAttachment = attachmentUri != null && attachmentUri.isNotEmpty;
    // 🛑 Empty text is a PICTURE, not a blank. Rejecting it here was one of
    // three places an incoming photo was dropped without trace.
    if (dedupKey == null) return null;
    if ((text == null || text.isEmpty) && !hasAttachment) return null;
    return IngestedMessage(
      attachmentUri: hasAttachment ? attachmentUri : null,
      attachmentMimeType:
          hasAttachment ? (map['attachmentMime'] as String?) : null,
      dedupKey: dedupKey,
      packageName: (map['packageName'] as String?) ?? '',
      conversationKey: (map['conversationKey'] as String?) ?? '',
      conversationTitle: map['conversationTitle'] as String?,
      isGroup: (map['isGroup'] as bool?) ?? false,
      senderName: map['senderName'] as String?,
      senderNumber: _number(map['senderNumber']),
      partyNumbers: <String>[
        for (final n in (map['partyNumbers'] as List?) ?? const <Object?>[])
          if (_number(n) case final String number) number,
      ],
      isFromMe: (map['isFromMe'] as bool?) ?? false,
      text: text ?? '',
      timestamp: _millis(map['timestamp']),
      postedAt: _millis(map['postedAt']),
      canReply: (map['canReply'] as bool?) ?? false,
    );
  }

  static String? _number(Object? raw) {
    final s = raw is String ? raw.trim() : '';
    return s.isEmpty ? null : s;
  }

  static DateTime _millis(Object? raw) {
    final ms = raw is int ? raw : 0;
    return DateTime.fromMillisecondsSinceEpoch(ms);
  }

  Map<String, Object?> toJson() => {
        'dedupKey': dedupKey,
        'packageName': packageName,
        'conversationKey': conversationKey,
        'conversationTitle': conversationTitle,
        'isGroup': isGroup,
        'senderName': senderName,
        'senderNumber': senderNumber,
        'partyNumbers': partyNumbers,
        'isFromMe': isFromMe,
        'text': text,
        'attachmentUri': attachmentUri,
        'attachmentMime': attachmentMimeType,
        'timestamp': timestamp.toIso8601String(),
        'postedAt': postedAt.toIso8601String(),
        'canReply': canReply,
      };
}
