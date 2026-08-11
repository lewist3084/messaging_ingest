import 'dart:async';

import 'package:flutter/foundation.dart';
import 'package:flutter/services.dart';

import 'ingested_message.dart';

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

  /// Takes everything captured while the engine was dead and clears the native
  /// queue. Destructive — whatever this returns is no longer stored natively,
  /// so persist it before dropping the result.
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
}
