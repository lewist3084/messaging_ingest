# messaging_ingest

Captures device messages by reading `MessagingStyle` notifications. Android only.

## Why this exists rather than reading SMS

The predecessor (`sms_plugin`) listened on `SMS_RECEIVED` and read the SMS
content provider. That works for SMS and **cannot see RCS at all** — RCS never
touches the SMS provider and fires no SMS broadcast. Since iOS 18, iPhones speak
RCS, so a mixed iPhone/Android group is now an RCS group, and those were exactly
the threads the old plugin could not see.

There is no legitimate alternative. Android's RCS APIs are annotated `@hide` and
allowlisted to Google Messages (plus Samsung's client), so no third-party app can
read RCS directly, even on its own device. Notifications are the only surface
that carries the content.

## What it captures

`NotificationCompat.MessagingStyle` gives per-message sender attribution, so a
group thread comes through as *who said what* rather than one blob:

| Field | Source | Caveat |
|---|---|---|
| `text`, `timestamp` | `MessagingStyle.Message` | — |
| `senderName` | `Message.person.name` | **Contact display name, never a phone number.** `Person.uri` is optional and Google Messages does not set it. Resolving to a number means matching Contacts yourself. |
| `isFromMe` | null `Person` | MessagingStyle convention for the user's own messages. |
| `conversationKey` | shortcut id → title → tag → key | Stable in that order of preference. |
| `isGroup`, `conversationTitle` | `MessagingStyle` | — |
| `canReply` | free-form `RemoteInput` on an action | Goes stale when the notification is dismissed. |

Media is **not** captured. Notifications carry a preview, not the file.

## The two things that garble a naive implementation

1. **Notifications re-post their whole recent history.** Google Messages updates
   a conversation by re-posting its notification with every recent message
   attached. Treating each `onNotificationPosted` as one new message duplicates
   the entire thread on every incoming text. `PendingStore` keeps a dedup ledger
   keyed on package + conversation + timestamp + sender + text hash.

2. **The Flutter engine is usually dead.** The system binds the listener on boot
   and keeps it bound; no Dart isolate exists for most captures. Everything is
   queued to SharedPreferences and drained via `drainPending()`. The live
   `stream` is a foreground convenience, not the source of truth.

## Limits worth stating up front

- **No backfill.** Capture starts when access is granted. History already on the
  phone is unreachable.
- **Muted threads post no notification**, so they are invisible.
- **Granted ≠ bound.** Android drops the binding on app update; the service calls
  `requestRebind()` but does not always get it back immediately. Check
  `isServiceConnected()`, not just `isPermissionGranted()`.
- **Capture only.** This package reads. Replying means firing a notification's
  `RemoteInput` action while it is still live — deliberately not built until the
  `canReply` rate says how often that window is actually open.
- **No iOS.** iOS exposes no notification-access API to third-party apps at all.
  Notification Service/Content Extensions only see your own app's notifications.
  The only iOS route is a Mac reading `~/Library/Messages/chat.db`, which is a
  different architecture, not an implementation of this one.

## Permission

Granted manually in system Settings — there is no runtime dialog.
`openPermissionSettings()` opens `ACTION_NOTIFICATION_LISTENER_SETTINGS`; the
caller must explain what the user is looking for once they arrive.
