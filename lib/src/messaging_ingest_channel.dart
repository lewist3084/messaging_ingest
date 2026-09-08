import 'dart:async';

import 'package:flutter/foundation.dart';
import 'package:flutter/services.dart';

import 'ingested_message.dart';
import 'sent_message.dart';

/// Dart face of the notification listener.
///
/// Android only. Every method is a no-op returning a safe default on other
/// platforms so callers do not have to guard each site — iOS has no
/// notification-access API of any kind, and there is no partial version of this
/// to fall back to.
class MessagingIngest {
  MessagingIngest._();

  static final MessagingIngest instance = MessagingIngest._();

  static const MethodChannel _methods =
      MethodChannel('messaging_ingest/methods');
  static const EventChannel _events = EventChannel('messaging_ingest/events');

  bool get isSupported => defaultTargetPlatform == TargetPlatform.android;

  Stream<IngestedMessage>? _stream;

  /// Live messages, for while the app is in the foreground. This is a
  /// convenience, not the source of truth — the listener service runs with no
  /// Dart isolate most of the time, so [drainPending] is what actually carries
  /// the traffic.
  Stream<IngestedMessage> get stream {
    if (!isSupported) return const Stream<IngestedMessage>.empty();
    return _stream ??= _events
        .receiveBroadcastStream()
        .map((event) => event is Map ? IngestedMessage.fromMap(event) : null)
        .where((m) => m != null)
        .cast<IngestedMessage>();
  }

  /// Whether the user has granted notification access in Settings.
  Future<bool> isPermissionGranted() async {
    if (!isSupported) return false;
    final granted = await _methods.invokeMethod<bool>('isPermissionGranted');
    return granted ?? false;
  }

  /// Opens the system notification-access screen. There is no in-app dialog for
  /// this permission, so the caller has to explain what the user is looking for
  /// before sending them there.
  Future<void> openPermissionSettings() async {
    if (!isSupported) return;
    await _methods.invokeMethod<void>('openPermissionSettings');
  }

  /// Whether the system currently has the listener bound. Granted-but-not-bound
  /// is a real state: Android drops the binding on app update, and the service
  /// asks for a rebind but does not always get one immediately.
  Future<bool> isServiceConnected() async {
    if (!isSupported) return false;
    final connected = await _methods.invokeMethod<bool>('isServiceConnected');
    return connected ?? false;
  }

  /// Tells the phone WHO it captures for and WHERE the documents go, so the
  /// listener service can write a captured text to Firestore itself — with no
  /// Flutter engine running — instead of queueing it for [drainPending].
  ///
  /// Call once the active membership is known, and again whenever it changes.
  /// [uid] is checked against the native `FirebaseAuth` user at write time, so
  /// a config left by one sign-in never writes as the next. [packages] is the
  /// allow-list of messaging apps whose notifications count as texts.
  /// [cacheSizeBytes] must equal the persistent-cache size the Dart side sets
  /// on its Firestore instance: the SDK refuses to start the same instance
  /// twice with different settings, and either side may be first.
  ///
  /// Returns false off Android or when the native side rejected the values.
  Future<bool> configure({
    required String uid,
    required String companyId,
    required String memberId,
    required String memberName,
    required List<String> packages,
    required int cacheSizeBytes,
  }) async {
    if (!isSupported) return false;
    try {
      final ok = await _methods.invokeMethod<bool>('configure', {
        'uid': uid,
        'companyId': companyId,
        'memberId': memberId,
        'memberName': memberName,
        'packages': packages,
        'cacheSizeBytes': cacheSizeBytes,
      });
      return ok ?? false;
    } on PlatformException {
      return false;
    }
  }

  /// Forgets [configure]. After this the listener queues again. Call on
  /// sign-out and on leaving the company.
  Future<void> clearConfig() async {
    if (!isSupported) return;
    await _methods.invokeMethod<void>('clearConfig');
  }

  /// Whether the phone currently writes captured texts itself.
  Future<bool> isNativeWriterConfigured() async {
    if (!isSupported) return false;
    return await _methods.invokeMethod<bool>('isNativeWriterConfigured') ??
        false;
  }

  /// Runs the phone's own sent-text sync — the SMS/MMS store read and filed
  /// from native, the same pages and watermark the Dart sync used. Returns
  /// how many were written. Needs [configure].
  Future<int> syncSentNative() async {
    if (!isSupported) return 0;
    return await _methods.invokeMethod<int>('syncSent') ?? 0;
  }

  /// Takes everything captured while the engine was dead and clears the native
  /// queue. Destructive — whatever this returns is no longer stored natively,
  /// so persist it before dropping the result.
  ///
  /// Since 2026-09-07 the phone writes most captures itself (see
  /// [configure]); this queue holds only what it could not.
  Future<List<IngestedMessage>> drainPending() async {
    if (!isSupported) return const [];
    final raw = await _methods.invokeListMethod<Object?>('drainPending');
    if (raw == null) return const [];
    return raw
        .whereType<Map>()
        .map(IngestedMessage.fromMap)
        .whereType<IngestedMessage>()
        .toList(growable: false);
  }

  /// Undrained message count, without draining. For surfacing backlog.
  Future<int> queueDepth() async {
    if (!isSupported) return 0;
    final depth = await _methods.invokeMethod<int>('queueDepth');
    return depth ?? 0;
  }

  // ── Sent texts ──────────────────────────────────────────────────────────
  //
  // A separate grant (READ_SMS, a runtime permission) from notification
  // access, and a separate store. Nothing here touches the notification
  // queue: [readSent] is non-destructive and [ackSent] is the caller saying
  // "written", so an interrupted sync re-reads instead of losing.

  /// Whether READ_SMS is held. Check only; ask through the app's permission
  /// flow at the point of use.
  Future<bool> isSmsReadGranted() async {
    if (!isSupported) return false;
    final granted = await _methods.invokeMethod<bool>('isSmsReadGranted');
    return granted ?? false;
  }

  /// The newest sent message already acknowledged, or null before the first.
  Future<DateTime?> sentWatermark() async {
    if (!isSupported) return null;
    final ms = await _methods.invokeMethod<int>('sentWatermark');
    if (ms == null || ms <= 0) return null;
    return DateTime.fromMillisecondsSinceEpoch(ms);
  }

  /// Sent SMS and MMS newer than [since] (default: the watermark), oldest
  /// first, at most [limit]. Empty without READ_SMS.
  Future<List<SentMessage>> readSent({DateTime? since, int limit = 300}) async {
    if (!isSupported) return const [];
    final raw = await _methods.invokeListMethod<Object?>('readSent', {
      if (since != null) 'since': since.millisecondsSinceEpoch,
      'limit': limit,
    });
    if (raw == null) return const [];
    return raw
        .whereType<Map>()
        .map(SentMessage.fromMap)
        .whereType<SentMessage>()
        .toList(growable: false);
  }

  /// Marks everything up to [until] as handed over. Call after the write
  /// that used it has committed, never before.
  Future<void> ackSent(DateTime until) async {
    if (!isSupported) return;
    await _methods.invokeMethod<void>('ackSent', {
      'until': until.millisecondsSinceEpoch,
    });
  }

  // ── Replying from this handset ──────────────────────────────────────────
  //
  // The member typed the reply somewhere else (their computer); this phone
  // is what sends it. Two routes. The conversation's posted notification is
  // tried first: it answers RCS as well as SMS and lands in the exact thread,
  // but exists only until the thread is read on the phone. Then plain SMS to
  // one number. Neither writes a message doc — the sent text is captured
  // back through the normal paths (the re-posted notification, or the SMS
  // provider's sent row on the next sync).

  /// Whether [conversationKey]'s notification is still posted with a reply
  /// box attached.
  Future<bool> canReplyInline(String conversationKey) async {
    if (!isSupported || conversationKey.isEmpty) return false;
    final ok = await _methods.invokeMethod<bool>('canReplyInline', {
      'conversationKey': conversationKey,
    });
    return ok ?? false;
  }

  /// Fires [text] into the posted notification's reply action. False when
  /// there is no live reply box (the notification was dismissed); throws a
  /// [PlatformException] if the system refused to fire it.
  Future<bool> replyInline(String conversationKey, String text) async {
    if (!isSupported) return false;
    final ok = await _methods.invokeMethod<bool>('replyInline', {
      'conversationKey': conversationKey,
      'text': text,
    });
    return ok ?? false;
  }

  /// Whether SEND_SMS is held. Check only.
  Future<bool> isSmsSendGranted() async {
    if (!isSupported) return false;
    final ok = await _methods.invokeMethod<bool>('isSmsSendGranted');
    return ok ?? false;
  }

  /// Sends one SMS to one [address] from this handset. Throws a
  /// [PlatformException] on refusal or a bad number.
  Future<void> sendSms({required String address, required String text}) async {
    if (!isSupported) return;
    await _methods.invokeMethod<bool>('sendSms', {
      'address': address,
      'text': text,
    });
  }

  /// The number behind a contact display name, or null when the phone has
  /// no such contact or more than one (never guess a recipient).
  Future<String?> resolveContactNumber(String displayName) async {
    if (!isSupported) return null;
    return _methods.invokeMethod<String>('resolveContactNumber', {
      'name': displayName,
    });
  }
}
