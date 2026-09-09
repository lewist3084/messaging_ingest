package com.fuli.messaging_ingest

import android.content.Context
import android.content.Intent
import android.os.Handler
import android.os.Looper
import android.provider.Settings
import androidx.core.app.NotificationManagerCompat
import io.flutter.embedding.engine.plugins.FlutterPlugin
import io.flutter.plugin.common.EventChannel
import io.flutter.plugin.common.MethodCall
import io.flutter.plugin.common.MethodChannel

class MessagingIngestPlugin : FlutterPlugin, MethodChannel.MethodCallHandler,
    EventChannel.StreamHandler {

    private lateinit var context: Context
    private lateinit var methodChannel: MethodChannel
    private lateinit var eventChannel: EventChannel
    private lateinit var store: PendingStore
    private lateinit var sent: SentMessageReader
    private lateinit var sender: SmsSender

    private val main = Handler(Looper.getMainLooper())

    override fun onAttachedToEngine(binding: FlutterPlugin.FlutterPluginBinding) {
        context = binding.applicationContext
        store = PendingStore(context)
        sent = SentMessageReader(context)
        sender = SmsSender(context)
        methodChannel = MethodChannel(binding.binaryMessenger, "messaging_ingest/methods")
        methodChannel.setMethodCallHandler(this)
        eventChannel = EventChannel(binding.binaryMessenger, "messaging_ingest/events")
        eventChannel.setStreamHandler(this)
    }

    override fun onDetachedFromEngine(binding: FlutterPlugin.FlutterPluginBinding) {
        methodChannel.setMethodCallHandler(null)
        eventChannel.setStreamHandler(null)
        MessageNotificationListener.sink = null
    }

    override fun onMethodCall(call: MethodCall, result: MethodChannel.Result) {
        when (call.method) {
            "isPermissionGranted" -> result.success(isPermissionGranted())
            "openPermissionSettings" -> {
                // No runtime-permission dialog exists for notification access;
                // the user has to toggle it in Settings themselves.
                val intent = Intent(Settings.ACTION_NOTIFICATION_LISTENER_SETTINGS)
                    .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                context.startActivity(intent)
                result.success(null)
            }
            "isServiceConnected" -> result.success(MessageNotificationListener.connected)
            // ── The phone writing Firestore itself (DeviceTextWriter) ──
            // Dart resolves WHO and WHERE and hands it down; from then on a
            // captured text is written from this process with no engine.
            "configure" -> {
                val packages = call.argument<List<String>>("packages") ?: emptyList()
                val values = IngestConfig.Values(
                    uid = call.argument<String>("uid") ?: "",
                    companyId = call.argument<String>("companyId") ?: "",
                    memberId = call.argument<String>("memberId") ?: "",
                    memberName = call.argument<String>("memberName") ?: "",
                    packages = packages.toSet(),
                    cacheSizeBytes = (call.argument<Number>("cacheSizeBytes") ?: (50L * 1024 * 1024)).toLong(),
                )
                if (values.uid.isEmpty() || values.companyId.isEmpty() || values.memberId.isEmpty()) {
                    result.error("bad_config", "uid, companyId and memberId are required", null)
                } else {
                    IngestConfig(context).write(values)
                    // The grant may have arrived since the service started.
                    SentTextObserver.ensureRegistered(context)
                    result.success(true)
                }
            }
            "clearConfig" -> {
                IngestConfig(context).clear()
                result.success(null)
            }
            "isNativeWriterConfigured" -> result.success(IngestConfig(context).isConfigured())
            // One sent-text sync on the writer's thread, as the Dart sync
            // would have done it; the count comes back when it is durable.
            "syncSent" -> {
                DeviceTextWriter.executor.execute {
                    val n = runCatching { DeviceTextWriter(context).syncSent() }.getOrElse { -1 }
                    main.post {
                        if (n < 0) result.error("sync_failed", "sent-text sync failed", null)
                        else result.success(n)
                    }
                }
            }
            "drainPending" -> result.success(store.drain())
            "queueDepth" -> result.success(store.queueDepth())
            // ── Sent texts, from the handset's own store (READ_SMS) ──
            "isSmsReadGranted" -> result.success(sent.isGranted())
            "sentWatermark" -> result.success(sent.watermark())
            "readSent" -> {
                val since = (call.argument<Number>("since") ?: sent.watermark()).toLong()
                val limit = (call.argument<Number>("limit") ?: 300).toInt()
                // Provider queries plus one contact lookup per recipient can
                // run to a few hundred ms on a backlog; keep them off the
                // UI thread the channel handler is called on.
                Thread {
                    val rows = runCatching { sent.read(since, limit) }.getOrElse { emptyList() }
                    main.post { result.success(rows) }
                }.start()
            }
            "ackSent" -> {
                val until = call.argument<Number>("until")?.toLong()
                if (until != null) sent.ack(until)
                result.success(null)
            }
            // ── A captured picture's bytes, on demand ──
            // The native writer uploads these itself (AttachmentUploader) and
            // never calls this. It exists for the DART drain — the fallback
            // path on a phone whose native writer is not configured — which
            // otherwise has a content URI it cannot open from Dart.
            //
            // Bytes rather than a URI on purpose: the URI is readable only by
            // this process, under this app's READ_SMS grant or the shade's
            // grant to the listener, and neither travels.
            "readAttachmentBytes" -> {
                val uri = call.argument<String>("uri").orEmpty()
                if (uri.isEmpty()) {
                    result.success(null)
                } else {
                    // A photo is megabytes; reading it on the channel's
                    // calling thread would jank whatever frame is in flight.
                    Thread {
                        val bytes = AttachmentUploader(context).readBytes(uri)
                        main.post { result.success(bytes) }
                    }.start()
                }
            }
            // ── Replying FROM this handset (queued on another device) ──
            "canReplyInline" -> {
                val key = call.argument<String>("conversationKey") ?: ""
                result.success(key.isNotEmpty() && MessageNotificationListener.canReplyInline(key))
            }
            "replyInline" -> {
                val key = call.argument<String>("conversationKey") ?: ""
                val text = call.argument<String>("text") ?: ""
                if (key.isEmpty() || text.isBlank()) {
                    result.success(false)
                } else {
                    runCatching { MessageNotificationListener.replyInline(context, key, text) }
                        .onSuccess { result.success(it) }
                        .onFailure { result.error("reply_failed", it.message, null) }
                }
            }
            "isSmsSendGranted" -> result.success(sender.isGranted())
            "sendSms" -> {
                val address = call.argument<String>("address") ?: ""
                val text = call.argument<String>("text") ?: ""
                runCatching { sender.send(address, text) }
                    .onSuccess { result.success(true) }
                    .onFailure { result.error("send_failed", it.message, null) }
            }
            "resolveContactNumber" -> {
                val name = call.argument<String>("name") ?: ""
                Thread {
                    val number = runCatching { sender.resolveNumber(name) }.getOrNull()
                    main.post { result.success(number) }
                }.start()
            }
            else -> result.notImplemented()
        }
    }

    private fun isPermissionGranted(): Boolean =
        NotificationManagerCompat.getEnabledListenerPackages(context)
            .contains(context.packageName)

    override fun onListen(arguments: Any?, events: EventChannel.EventSink?) {
        // The listener service runs on its own thread and usually with no Dart
        // isolate at all; hop to main before touching the sink.
        MessageNotificationListener.sink = { payload ->
            main.post { events?.success(payload) }
        }
    }

    override fun onCancel(arguments: Any?) {
        MessageNotificationListener.sink = null
    }
}
