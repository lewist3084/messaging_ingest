/// Android message capture via MessagingStyle notifications.
///
/// Reads conversations the app has no protocol-level access to — RCS in
/// particular, whose framework APIs are `@hide` and allowlisted to Google
/// Messages. Covers Google Messages (RCS + SMS), WhatsApp, Signal, Telegram,
/// and anything else posting MessagingStyle notifications.
///
/// Capture only: this package reads. It does not send.
///
/// Android-only by construction. iOS exposes no notification-access API to
/// third-party apps at all, so there is no iOS implementation to write.
library messaging_ingest;

export 'src/ingested_message.dart';
export 'src/messaging_ingest_channel.dart';
