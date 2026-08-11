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

    private val main = Handler(Looper.getMainLooper())

    override fun onAttachedToEngine(binding: FlutterPlugin.FlutterPluginBinding) {
        context = binding.applicationContext
        store = PendingStore(context)
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
            "drainPending" -> result.success(store.drain())
            "queueDepth" -> result.success(store.queueDepth())
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
