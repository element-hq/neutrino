package org.jakebot.blew

import android.annotation.SuppressLint
import android.bluetooth.*
import android.bluetooth.le.AdvertiseCallback
import android.bluetooth.le.AdvertiseData
import android.bluetooth.le.AdvertiseSettings
import android.bluetooth.le.AdvertisingSet
import android.bluetooth.le.AdvertisingSetCallback
import android.bluetooth.le.AdvertisingSetParameters
import android.bluetooth.le.BluetoothLeAdvertiser
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.os.Build
import android.os.ParcelUuid
import android.os.SystemClock
import android.util.Log
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Singleton managing the Android BLE peripheral role (GATT server + advertiser).
 *
 * Kotlin methods are called from Rust via JNI. Callbacks from Android BLE are
 * forwarded to Rust via [external fun] JNI hooks.
 */
@SuppressLint("MissingPermission")
object BlePeripheralManager {
    private const val TAG = "BlePeripheralManager"

    private var context: Context? = null
    private var bluetoothManager: BluetoothManager? = null
    private var gattServer: BluetoothGattServer? = null
    private var advertiser: BluetoothLeAdvertiser? = null
    // Extended-advertising set + callback, used when manufacturer data is
    // present (a full node id + name overflows the 31-byte legacy budget).
    private var advertisingSet: AdvertisingSet? = null
    private var advertisingSetCallback: AdvertisingSetCallback? = null

    // Track connected devices for notification delivery.
    private val connectedDevices = ConcurrentHashMap<String, BluetoothDevice>()

    // Track which (device, characteristic) pairs are subscribed for notifications.
    private val subscriptions = ConcurrentHashMap<String, MutableSet<UUID>>()

    // Map characteristic UUID -> BluetoothGattCharacteristic for notification sending.
    private val characteristics = ConcurrentHashMap<UUID, BluetoothGattCharacteristic>()

    // Static characteristic values — auto-responded on read, matching CoreBluetooth behaviour.
    private val staticValues = ConcurrentHashMap<UUID, ByteArray>()

    // Latch to serialize addService calls (Android requires waiting for onServiceAdded).
    @Volatile private var serviceAddedLatch: CountDownLatch? = null

    // Per-device notify gate. Android's BluetoothGattServer allows only one
    // outstanding notifyCharacteristicChanged per device — the next is silently
    // dropped until onNotificationSent. We serialize on that, storing the
    // monotonic deadline (elapsedRealtime ms) until which a notification is
    // considered in-flight.
    //
    // Self-healing: the gate reopens either when onNotificationSent clears the
    // entry (the healthy path, typically sub-50ms) OR when the deadline lapses.
    // The latter is essential — the stack can accept notifyCharacteristicChanged
    // yet fail to actually transmit ("Unable to send GATT server response"), in
    // which case onNotificationSent never fires. A plain held-until-callback
    // semaphore would then wedge for the entire connection, blocking every
    // subsequent notification and killing the peripheral->central link (the
    // receiver can't send its QUIC handshake response → LINK_DEAD → 2-3 retries).
    private val notifyInFlightUntil = ConcurrentHashMap<String, Long>()

    // Longest a healthy onNotificationSent should take; past this we presume the
    // completion callback was lost and reopen the gate.
    private const val NOTIFY_INFLIGHT_TIMEOUT_MS = 500L

    // Marks a notification in-flight (returns true) iff none is in-flight or the
    // previous one's deadline has lapsed. Atomic via `compute`.
    private fun acquireNotify(addr: String): Boolean {
        val now = SystemClock.elapsedRealtime()
        var acquired = false
        notifyInFlightUntil.compute(addr) { _, until ->
            if (until == null || now >= until) {
                acquired = true
                now + NOTIFY_INFLIGHT_TIMEOUT_MS
            } else {
                until
            }
        }
        return acquired
    }

    private fun releaseNotify(addr: String) {
        notifyInFlightUntil.remove(addr)
    }

    // ── L2CAP state ──
    private val l2cap =
        L2capSocketManager(
            tag = TAG,
            onData = { socketId, data -> nativeOnL2capChannelData(socketId, data) },
            onClosed = { socketId -> nativeOnL2capChannelClosed(socketId) },
            startId = 100_000,
        )

    @Volatile private var l2capServerSocket: BluetoothServerSocket? = null

    // Serializes addService calls (Android requires waiting for onServiceAdded
    // before adding the next service).
    private val serviceAddLock = Any()

    @JvmStatic
    external fun nativeOnReadRequest(
        requestId: Int,
        deviceAddr: String,
        serviceUuid: String,
        charUuid: String,
        offset: Int,
    )

    @JvmStatic
    external fun nativeOnWriteRequest(
        requestId: Int,
        deviceAddr: String,
        serviceUuid: String,
        charUuid: String,
        value: ByteArray,
        responseNeeded: Boolean,
    )

    @JvmStatic
    external fun nativeOnSubscriptionChanged(
        deviceAddr: String,
        charUuid: String,
        subscribed: Boolean,
    )

    @JvmStatic
    external fun nativeOnConnectionStateChanged(
        deviceAddr: String,
        connected: Boolean,
    )

    @JvmStatic
    external fun nativeOnAdapterStateChanged(powered: Boolean)

    // ── L2CAP JNI hooks ──

    @JvmStatic
    external fun nativeOnL2capServerOpened(psm: Int)

    @JvmStatic
    external fun nativeOnL2capServerError(errorMessage: String)

    @JvmStatic
    external fun nativeOnL2capChannelOpened(
        deviceAddr: String,
        socketId: Int,
        fromServer: Boolean,
    )

    @JvmStatic
    external fun nativeOnL2capChannelData(
        socketId: Int,
        data: ByteArray,
    )

    @JvmStatic
    external fun nativeOnL2capChannelClosed(socketId: Int)

    private val adapterStateReceiver =
        object : BroadcastReceiver() {
            override fun onReceive(
                context: Context,
                intent: Intent,
            ) {
                if (intent.action == BluetoothAdapter.ACTION_STATE_CHANGED) {
                    val state = intent.getIntExtra(BluetoothAdapter.EXTRA_STATE, BluetoothAdapter.ERROR)
                    when (state) {
                        BluetoothAdapter.STATE_ON -> nativeOnAdapterStateChanged(true)
                        BluetoothAdapter.STATE_OFF -> nativeOnAdapterStateChanged(false)
                    }
                }
            }
        }

    fun init(ctx: Context) {
        context = ctx
        bluetoothManager = ctx.getSystemService(Context.BLUETOOTH_SERVICE) as? BluetoothManager
        val adapter = bluetoothManager?.adapter
        if (adapter != null) {
            advertiser = adapter.bluetoothLeAdvertiser
        }
        Log.d(TAG, "initialized, adapter=${adapter != null}, advertiser=${advertiser != null}")
        val filter = IntentFilter(BluetoothAdapter.ACTION_STATE_CHANGED)
        ctx.registerReceiver(adapterStateReceiver, filter)
    }

    private val gattCallback =
        object : BluetoothGattServerCallback() {
            override fun onServiceAdded(
                status: Int,
                service: BluetoothGattService?,
            ) {
                Log.d(TAG, "onServiceAdded status=$status uuid=${service?.uuid}")
                serviceAddedLatch?.countDown()
            }

            override fun onConnectionStateChange(
                device: BluetoothDevice,
                status: Int,
                newState: Int,
            ) {
                val addr = device.address
                if (newState == BluetoothProfile.STATE_CONNECTED) {
                    connectedDevices[addr] = device
                    nativeOnConnectionStateChanged(addr, true)
                } else if (newState == BluetoothProfile.STATE_DISCONNECTED) {
                    connectedDevices.remove(addr)
                    subscriptions.remove(addr)
                    // Clear the notify gate so a reconnect starts fresh.
                    notifyInFlightUntil.remove(addr)
                    nativeOnConnectionStateChanged(addr, false)
                }
            }

            override fun onNotificationSent(
                device: BluetoothDevice,
                status: Int,
            ) {
                releaseNotify(device.address)
            }

            override fun onCharacteristicReadRequest(
                device: BluetoothDevice,
                requestId: Int,
                offset: Int,
                characteristic: BluetoothGattCharacteristic,
            ) {
                // Auto-respond for static characteristics (matches CoreBluetooth behaviour
                // where characteristics with a non-nil value are served by the framework).
                val staticValue = staticValues[characteristic.uuid]
                if (staticValue != null) {
                    if (offset > staticValue.size) {
                        gattServer?.sendResponse(
                            device,
                            requestId,
                            BluetoothGatt.GATT_INVALID_OFFSET,
                            offset,
                            null,
                        )
                        return
                    }
                    gattServer?.sendResponse(
                        device,
                        requestId,
                        BluetoothGatt.GATT_SUCCESS,
                        offset,
                        staticValue.copyOfRange(offset, staticValue.size),
                    )
                    return
                }

                nativeOnReadRequest(
                    requestId,
                    device.address,
                    characteristic.service.uuid.toString(),
                    characteristic.uuid.toString(),
                    offset,
                )
            }

            override fun onCharacteristicWriteRequest(
                device: BluetoothDevice,
                requestId: Int,
                characteristic: BluetoothGattCharacteristic,
                preparedWrite: Boolean,
                responseNeeded: Boolean,
                offset: Int,
                value: ByteArray?,
            ) {
                nativeOnWriteRequest(
                    requestId,
                    device.address,
                    characteristic.service.uuid.toString(),
                    characteristic.uuid.toString(),
                    value ?: ByteArray(0),
                    responseNeeded,
                )
            }

            override fun onDescriptorWriteRequest(
                device: BluetoothDevice,
                requestId: Int,
                descriptor: BluetoothGattDescriptor,
                preparedWrite: Boolean,
                responseNeeded: Boolean,
                offset: Int,
                value: ByteArray?,
            ) {
                // Client Characteristic Configuration Descriptor (0x2902) — subscription toggle.
                val cccdUuid = UUID.fromString("00002902-0000-1000-8000-00805f9b34fb")
                if (descriptor.uuid == cccdUuid) {
                    val charUuid = descriptor.characteristic.uuid
                    val addr = device.address
                    val subscribed = value != null && value.isNotEmpty() && value[0].toInt() != 0

                    if (subscribed) {
                        subscriptions.getOrPut(addr) { mutableSetOf() }.add(charUuid)
                    } else {
                        subscriptions[addr]?.remove(charUuid)
                    }

                    nativeOnSubscriptionChanged(addr, charUuid.toString(), subscribed)
                }

                if (responseNeeded) {
                    gattServer?.sendResponse(device, requestId, BluetoothGatt.GATT_SUCCESS, 0, null)
                }
            }
        }

    private fun ensureGattServer() {
        if (gattServer == null) {
            gattServer = bluetoothManager?.openGattServer(context, gattCallback)
        }
    }

    /**
     * Add a GATT service. Called from Rust via JNI.
     *
     * Parameters are kept flat to simplify JNI marshalling:
     * - serviceUuid: service UUID string
     * - charUuids: array of characteristic UUID strings
     * - charProperties: array of property bitflags (matching Android's BluetoothGattCharacteristic constants)
     * - charPermissions: array of permission bitflags
     * - charValues: array of initial values (empty byte arrays for dynamic characteristics)
     */
    @JvmStatic
    fun addService(
        serviceUuid: String,
        charUuids: Array<String>,
        charProperties: IntArray,
        charPermissions: IntArray,
        charValues: Array<ByteArray>,
    ) {
        synchronized(serviceAddLock) {
            ensureGattServer()

            val service =
                BluetoothGattService(
                    UUID.fromString(serviceUuid),
                    BluetoothGattService.SERVICE_TYPE_PRIMARY,
                )

            val cccdUuid = UUID.fromString("00002902-0000-1000-8000-00805f9b34fb")

            for (i in charUuids.indices) {
                val uuid = UUID.fromString(charUuids[i])
                val props = charProperties[i]
                val perms = charPermissions[i]

                val char = BluetoothGattCharacteristic(uuid, props, perms)

                // Set static value if non-empty.
                if (charValues[i].isNotEmpty()) {
                    char.value = charValues[i]
                    staticValues[uuid] = charValues[i]
                }

                // Add CCCD if the characteristic supports notifications or indications.
                if (props and (
                        BluetoothGattCharacteristic.PROPERTY_NOTIFY or
                            BluetoothGattCharacteristic.PROPERTY_INDICATE
                    ) != 0
                ) {
                    val cccd =
                        BluetoothGattDescriptor(
                            cccdUuid,
                            BluetoothGattDescriptor.PERMISSION_READ or BluetoothGattDescriptor.PERMISSION_WRITE,
                        )
                    char.addDescriptor(cccd)
                }

                characteristics[uuid] = char
                service.addCharacteristic(char)
            }

            val latch = CountDownLatch(1)
            serviceAddedLatch = latch
            gattServer?.addService(service)
            if (!latch.await(5, TimeUnit.SECONDS)) {
                Log.w(TAG, "addService timed out for $serviceUuid")
            }
            Log.d(TAG, "added service $serviceUuid with ${charUuids.size} characteristics")
        }
    }

    private var advertiseCallback: AdvertiseCallback? = null

    @JvmStatic
    fun startAdvertising(
        name: String,
        serviceUuids: Array<String>,
        // Manufacturer-specific data. `manufacturerId < 0` (with `null` bytes)
        // means "no manufacturer data" → legacy advertising, preserving the
        // prior behaviour. When present, we must use BLE-5 extended advertising:
        // a full 32-byte node id + display name overflows the 31-byte legacy
        // advertisement budget.
        manufacturerId: Int,
        manufacturerData: ByteArray?,
    ) {
        val adv =
            advertiser ?: run {
                Log.e(TAG, "advertiser not available")
                return
            }

        bluetoothManager?.adapter?.name = name

        if (manufacturerData != null && manufacturerId >= 0) {
            startExtendedAdvertising(adv, serviceUuids, manufacturerId, manufacturerData)
            return
        }

        val settings =
            AdvertiseSettings
                .Builder()
                .setAdvertiseMode(AdvertiseSettings.ADVERTISE_MODE_LOW_LATENCY)
                .setConnectable(true)
                .setTxPowerLevel(AdvertiseSettings.ADVERTISE_TX_POWER_HIGH)
                .build()

        val dataBuilder =
            AdvertiseData
                .Builder()
                .setIncludeDeviceName(false)
        for (uuid in serviceUuids) {
            dataBuilder.addServiceUuid(ParcelUuid(UUID.fromString(uuid)))
        }
        val data = dataBuilder.build()

        // Scan response can carry the device name.
        val scanResponse =
            AdvertiseData
                .Builder()
                .setIncludeDeviceName(true)
                .build()

        advertiseCallback =
            object : AdvertiseCallback() {
                override fun onStartSuccess(settingsInEffect: AdvertiseSettings?) {
                    Log.d(TAG, "advertising started")
                }

                override fun onStartFailure(errorCode: Int) {
                    Log.e(TAG, "advertising failed: errorCode=$errorCode")
                }
            }

        adv.startAdvertising(settings, data, scanResponse, advertiseCallback)
    }

    // BLE-5 extended advertising carrying manufacturer data (node id + display
    // name). Connectable extended sets cannot also be scannable, so the payload
    // rides entirely in the primary advertising data.
    private fun startExtendedAdvertising(
        adv: BluetoothLeAdvertiser,
        serviceUuids: Array<String>,
        manufacturerId: Int,
        manufacturerData: ByteArray,
    ) {
        val params =
            AdvertisingSetParameters
                .Builder()
                .setLegacyMode(false)
                .setConnectable(true)
                .setScannable(false)
                .setInterval(AdvertisingSetParameters.INTERVAL_LOW)
                .setTxPowerLevel(AdvertisingSetParameters.TX_POWER_HIGH)
                .build()

        val dataBuilder =
            AdvertiseData
                .Builder()
                .setIncludeDeviceName(false)
        for (uuid in serviceUuids) {
            dataBuilder.addServiceUuid(ParcelUuid(UUID.fromString(uuid)))
        }
        dataBuilder.addManufacturerData(manufacturerId, manufacturerData)
        val data = dataBuilder.build()

        val cb =
            object : AdvertisingSetCallback() {
                override fun onAdvertisingSetStarted(
                    set: AdvertisingSet?,
                    txPower: Int,
                    status: Int,
                ) {
                    if (status == AdvertisingSetCallback.ADVERTISE_SUCCESS) {
                        advertisingSet = set
                        Log.d(TAG, "extended advertising started (txPower=$txPower)")
                    } else {
                        Log.e(TAG, "extended advertising failed: status=$status")
                    }
                }
            }
        advertisingSetCallback = cb
        try {
            adv.startAdvertisingSet(params, data, null, null, null, cb)
        } catch (e: Exception) {
            Log.e(TAG, "startAdvertisingSet threw (extended advertising unsupported?): $e")
            advertisingSetCallback = null
        }
    }

    @JvmStatic
    fun stopAdvertising() {
        advertiseCallback?.let { cb ->
            advertiser?.stopAdvertising(cb)
            advertiseCallback = null
        }
        advertisingSetCallback?.let { cb ->
            advertiser?.stopAdvertisingSet(cb)
            advertisingSetCallback = null
            advertisingSet = null
        }
        Log.d(TAG, "advertising stopped")
    }

    /**
     * Send a notification on a characteristic to a single subscribed device.
     *
     * Returns:
     *   0 = success
     *   1 = busy (semaphore not available — caller should retry after a short delay)
     *   2 = device not connected or not subscribed to this characteristic
     *   3 = characteristic not found
     */
    @JvmStatic
    fun notifyCharacteristic(
        deviceAddr: String,
        charUuid: String,
        value: ByteArray,
    ): Int {
        val uuid = UUID.fromString(charUuid)
        val char = characteristics[uuid] ?: return 3
        val device = connectedDevices[deviceAddr] ?: return 2
        val subs = subscriptions[deviceAddr] ?: return 2
        if (uuid !in subs) return 2
        if (!acquireNotify(deviceAddr)) return 1
        val sent = sendNotification(device, char, value)
        if (!sent) {
            releaseNotify(deviceAddr)
            return 1
        }
        return 0
    }

    /**
     * Send a single notification, handling the API 33+ / legacy split.
     * On API < 33, synchronizes on [char] to prevent concurrent `char.value`
     * races when multiple devices are notified from different threads.
     */
    private fun sendNotification(
        device: BluetoothDevice,
        char: BluetoothGattCharacteristic,
        value: ByteArray,
    ): Boolean =
        if (Build.VERSION.SDK_INT >= 33) {
            gattServer?.notifyCharacteristicChanged(device, char, false, value) ==
                BluetoothStatusCodes.SUCCESS
        } else {
            @Suppress("DEPRECATION")
            synchronized(char) {
                char.value = value
                gattServer?.notifyCharacteristicChanged(device, char, false) ?: false
            }
        }

    @JvmStatic
    fun respondToRead(
        deviceAddr: String,
        requestId: Int,
        value: ByteArray,
    ) {
        val device = connectedDevices[deviceAddr] ?: return
        gattServer?.sendResponse(device, requestId, BluetoothGatt.GATT_SUCCESS, 0, value)
    }

    @JvmStatic
    fun respondToReadError(
        deviceAddr: String,
        requestId: Int,
    ) {
        val device = connectedDevices[deviceAddr] ?: return
        gattServer?.sendResponse(
            device,
            requestId,
            BluetoothGatt.GATT_FAILURE,
            0,
            null,
        )
    }

    @JvmStatic
    fun respondToWrite(
        deviceAddr: String,
        requestId: Int,
        success: Boolean,
    ) {
        val device = connectedDevices[deviceAddr] ?: return
        val status = if (success) BluetoothGatt.GATT_SUCCESS else BluetoothGatt.GATT_FAILURE
        gattServer?.sendResponse(device, requestId, status, 0, null)
    }

    @JvmStatic
    fun isPowered(): Boolean = bluetoothManager?.adapter?.isEnabled == true

    @JvmStatic
    fun areBlePermissionsGranted(): Boolean {
        val ctx = context ?: run {
            Log.w(TAG, "areBlePermissionsGranted: init() never called, reporting not-granted")
            return false
        }

        fun granted(p: String) =
            androidx.core.content.ContextCompat
                .checkSelfPermission(ctx, p) ==
                android.content.pm.PackageManager.PERMISSION_GRANTED
        return if (android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.S) {
            arrayOf(
                android.Manifest.permission.BLUETOOTH_SCAN,
                android.Manifest.permission.BLUETOOTH_CONNECT,
                android.Manifest.permission.BLUETOOTH_ADVERTISE,
            ).all(::granted)
        } else {
            granted(android.Manifest.permission.ACCESS_FINE_LOCATION)
        }
    }

    // ── L2CAP ──

    @JvmStatic
    fun openL2capServer() {
        if (android.os.Build.VERSION.SDK_INT < 29) {
            nativeOnL2capServerError("L2CAP requires API 29+")
            return
        }

        val adapter =
            bluetoothManager?.adapter ?: run {
                nativeOnL2capServerError("adapter not available")
                return
            }

        try {
            val serverSocket = adapter.listenUsingInsecureL2capChannel()
            l2capServerSocket = serverSocket
            val psm = serverSocket.psm
            nativeOnL2capServerOpened(psm)

            Thread {
                while (true) {
                    try {
                        val socket = serverSocket.accept()
                        val addr = socket.remoteDevice.address
                        val socketId = l2cap.register(socket)
                        nativeOnL2capChannelOpened(addr, socketId, true)
                        Thread { l2cap.startReadLoop(socketId, addr, socket) }.start()
                    } catch (e: Exception) {
                        Log.d(TAG, "L2CAP accept ended: ${e.message}")
                        break
                    }
                }
            }.start()
        } catch (e: Exception) {
            Log.e(TAG, "L2CAP server failed: ${e.message}")
            nativeOnL2capServerError(e.message ?: "server open failed")
        }
    }

    @JvmStatic
    fun closeL2capServer() {
        try {
            l2capServerSocket?.close()
        } catch (_: Exception) {
        }
        l2capServerSocket = null
    }

    @JvmStatic
    fun writeL2cap(
        socketId: Int,
        data: ByteArray,
    ) = l2cap.write(socketId, data)

    @JvmStatic
    fun closeL2cap(socketId: Int) = l2cap.close(socketId)
}
