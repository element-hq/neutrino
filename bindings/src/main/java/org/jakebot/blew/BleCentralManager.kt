package org.jakebot.blew

import android.annotation.SuppressLint
import android.bluetooth.*
import android.bluetooth.le.ScanCallback
import android.bluetooth.le.ScanFilter
import android.bluetooth.le.ScanResult
import android.bluetooth.le.ScanSettings
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.os.ParcelUuid
import android.util.Log
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * Singleton managing the Android BLE central role (scanner + GATT client).
 *
 * Kotlin methods are called from Rust via JNI. Android BLE callbacks are
 * forwarded to Rust via [external fun] JNI hooks.
 *
 * ## GATT operation serialization
 *
 * Android's [BluetoothGatt] allows only one in-flight operation at a time
 * (read, write, descriptor write, discover services, request MTU). Each device
 * gets its own [GattOperationQueue] that serializes operations for that device,
 * with per-operation timeouts to guard against firmware bugs where callbacks
 * never fire.
 */
@SuppressLint("MissingPermission")
object BleCentralManager {
    private const val TAG = "BleCentralManager"

    // Status codes returned to Rust via JNI.
    private const val STATUS_SUCCESS = 0
    private const val STATUS_NOT_CONNECTED = 1
    private const val STATUS_CHAR_NOT_FOUND = 2
    private const val STATUS_GATT_BUSY = 3
    private const val STATUS_GATT_FAILED = 4

    private var context: Context? = null
    private var bluetoothManager: BluetoothManager? = null
    private var adapter: BluetoothAdapter? = null

    // Active GATT connections keyed by device address.
    private val gattConnections = ConcurrentHashMap<String, BluetoothGatt>()

    // Per-device MTU (default 23 until negotiated).
    private val mtuMap = ConcurrentHashMap<String, Int>()

    // Per-device GATT operation queues.
    private val gattQueues = ConcurrentHashMap<String, GattOperationQueue>()

    // Nonces for in-flight GATT operations. Callbacks consume these before
    // completing the queue or notifying Rust so stale callbacks are ignored.
    private val pendingNonces = ConcurrentHashMap<String, Long>()

    // Device addresses with a write-without-response already completed from the
    // kick lambda. onCharacteristicWrite may still fire on some devices; the
    // entry here tells the callback to skip completeCurrent and the native
    // notification (the coroutine has already delivered both).
    private val noResponseHandled = ConcurrentHashMap<String, Boolean>()

    // Coroutine scope for launching GATT operation coroutines.
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    // ── L2CAP state ──
    private val l2cap =
        L2capSocketManager(
            tag = TAG,
            onData = { socketId, data -> nativeOnL2capChannelData(socketId, data) },
            onClosed = { socketId -> nativeOnL2capChannelClosed(socketId) },
        )

    // ── JNI hooks (Kotlin → Rust) ──

    @JvmStatic
    external fun nativeOnDeviceDiscovered(
        deviceAddr: String,
        deviceName: String?,
        rssi: Int,
        serviceUuids: String,
    )

    @JvmStatic
    external fun nativeOnConnectionStateChanged(
        deviceAddr: String,
        connected: Boolean,
        gattStatus: Int,
    )

    @JvmStatic
    external fun nativeOnServicesDiscovered(
        deviceAddr: String,
        servicesJson: String,
    )

    @JvmStatic
    external fun nativeOnCharacteristicRead(
        deviceAddr: String,
        charUuid: String,
        value: ByteArray,
        status: Int,
    )

    @JvmStatic
    external fun nativeOnCharacteristicWrite(
        deviceAddr: String,
        charUuid: String,
        status: Int,
    )

    @JvmStatic
    external fun nativeOnCharacteristicChanged(
        deviceAddr: String,
        charUuid: String,
        value: ByteArray,
    )

    @JvmStatic
    external fun nativeOnMtuChanged(
        deviceAddr: String,
        mtu: Int,
    )

    @JvmStatic
    external fun nativeOnAdapterStateChanged(powered: Boolean)

    // ── L2CAP JNI hooks ──

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

    @JvmStatic
    external fun nativeOnL2capChannelError(
        deviceAddr: String,
        errorMessage: String,
    )

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
        adapter = bluetoothManager?.adapter
        Log.d(TAG, "initialized, adapter=${adapter != null}")
        val filter = IntentFilter(BluetoothAdapter.ACTION_STATE_CHANGED)
        ctx.registerReceiver(adapterStateReceiver, filter)
    }

    // ── Per-device queue helper ──

    private fun queueFor(addr: String): GattOperationQueue = gattQueues.getOrPut(addr) { GattOperationQueue("gatt-$addr") }

    // ── Scanning ──

    private var scanCallback: ScanCallback? = null

    @JvmStatic
    fun startScan(
        serviceUuids: Array<String>,
        lowPower: Boolean = false,
    ) {
        val scanner =
            adapter?.bluetoothLeScanner ?: run {
                Log.e(TAG, "scanner not available")
                return
            }

        stopScan()

        val filters =
            if (serviceUuids.isNotEmpty()) {
                serviceUuids.map { uuid ->
                    ScanFilter
                        .Builder()
                        .setServiceUuid(ParcelUuid(UUID.fromString(uuid)))
                        .build()
                }
            } else {
                null
            }

        val scanMode =
            if (lowPower) {
                ScanSettings.SCAN_MODE_LOW_POWER
            } else {
                ScanSettings.SCAN_MODE_LOW_LATENCY
            }
        val settings =
            ScanSettings
                .Builder()
                .setScanMode(scanMode)
                .build()

        scanCallback =
            object : ScanCallback() {
                override fun onScanResult(
                    callbackType: Int,
                    result: ScanResult,
                ) {
                    val device = result.device
                    val addr = device.address
                    val name = device.name
                    val rssi = result.rssi

                    val uuids =
                        result.scanRecord
                            ?.serviceUuids
                            ?.joinToString(",") { it.uuid.toString() }
                            ?: ""

                    nativeOnDeviceDiscovered(addr, name, rssi, uuids)
                }

                override fun onScanFailed(errorCode: Int) {
                    Log.e(TAG, "scan failed: errorCode=$errorCode")
                }
            }

        scanner.startScan(filters, settings, scanCallback)
        Log.d(TAG, "scan started (filters=${serviceUuids.size} UUIDs)")
    }

    @JvmStatic
    fun stopScan() {
        scanCallback?.let { cb ->
            adapter?.bluetoothLeScanner?.stopScan(cb)
            scanCallback = null
        }
    }

    // ── GATT callback ──

    private val gattCallback =
        object : BluetoothGattCallback() {
            override fun onConnectionStateChange(
                gatt: BluetoothGatt,
                status: Int,
                newState: Int,
            ) {
                val addr = gatt.device.address

                if (newState == BluetoothProfile.STATE_CONNECTED) {
                    gattConnections[addr] = gatt
                    // Capture the queue reference before launching so a racing
                    // disconnect (which removes the entry from gattQueues) can't
                    // cause this coroutine to create an orphaned queue.
                    val q = queueFor(addr)
                    scope.launch {
                        // Enqueue MTU request so other ops queue behind it per device.
                        val mtuResult =
                            q.enqueue<Int>(
                                name = "request-mtu",
                                timeoutMs = 5000L,
                                kick = {
                                    val nonce = q.currentNonce() ?: return@enqueue false
                                    val key = "$addr:mtu"
                                    pendingNonces[key] = nonce
                                    val started = gatt.requestMtu(512)
                                    if (!started) {
                                        pendingNonces.remove(key)
                                    }
                                    started
                                },
                            )
                        if (mtuResult.isFailure) {
                            Log.w(TAG, "MTU negotiation failed for $addr: ${mtuResult.exceptionOrNull()?.message}")
                        }
                        nativeOnConnectionStateChanged(addr, true, 0)
                    }
                } else if (newState == BluetoothProfile.STATE_DISCONNECTED) {
                    gattConnections.remove(addr)
                    mtuMap.remove(addr)
                    noResponseHandled.remove(addr)
                    pendingNonces.keys.removeIf { it.startsWith("$addr:") }
                    gattQueues.remove(addr)?.close(CancellationException("device $addr disconnected"))
                    gatt.close()
                    nativeOnConnectionStateChanged(addr, false, status)
                }
            }

            override fun onMtuChanged(
                gatt: BluetoothGatt,
                mtu: Int,
                status: Int,
            ) {
                val addr = gatt.device.address
                if (status == BluetoothGatt.GATT_SUCCESS) {
                    mtuMap[addr] = mtu
                    nativeOnMtuChanged(addr, mtu)
                }
                val nonce = pendingNonces.remove("$addr:mtu") ?: return
                gattQueues[addr]?.completeCurrent<Int>(nonce, mtu)
                // Do NOT call nativeOnConnectionStateChanged here; the coroutine in the CONNECTED branch does it.
            }

            override fun onServicesDiscovered(
                gatt: BluetoothGatt,
                status: Int,
            ) {
                val addr = gatt.device.address
                val nonce = pendingNonces.remove("$addr:services") ?: return
                gattQueues[addr]?.completeCurrent<Unit>(nonce, Unit)
                if (status == BluetoothGatt.GATT_SUCCESS) {
                    nativeOnServicesDiscovered(addr, servicesToJson(gatt.services))
                } else {
                    nativeOnServicesDiscovered(addr, "[]")
                }
            }

            override fun onCharacteristicRead(
                gatt: BluetoothGatt,
                characteristic: BluetoothGattCharacteristic,
                value: ByteArray,
                status: Int,
            ) {
                val addr = gatt.device.address
                val charUuid = characteristic.uuid.toString()
                val nonce = pendingNonces.remove("$addr:read:$charUuid") ?: return
                gattQueues[addr]?.completeCurrent<Unit>(nonce, Unit)
                nativeOnCharacteristicRead(addr, charUuid, value, status)
            }

            override fun onCharacteristicWrite(
                gatt: BluetoothGatt,
                characteristic: BluetoothGattCharacteristic,
                status: Int,
            ) {
                val addr = gatt.device.address
                if (noResponseHandled.remove(addr) != null) {
                    // Kick lambda already completed the queue and fired the
                    // native callback for this no-response write.
                    return
                }
                val charUuid = characteristic.uuid.toString()
                val nonce = pendingNonces.remove("$addr:write:$charUuid") ?: return
                gattQueues[addr]?.completeCurrent<Unit>(nonce, Unit)
                nativeOnCharacteristicWrite(
                    addr,
                    charUuid,
                    status,
                )
            }

            override fun onDescriptorWrite(
                gatt: BluetoothGatt,
                descriptor: BluetoothGattDescriptor,
                status: Int,
            ) {
                val addr = gatt.device.address
                val charUuid = descriptor.characteristic.uuid.toString()
                val nonce = pendingNonces.remove("$addr:cccd:$charUuid") ?: return
                gattQueues[addr]?.completeCurrent<Unit>(nonce, Unit)
            }

            override fun onCharacteristicChanged(
                gatt: BluetoothGatt,
                characteristic: BluetoothGattCharacteristic,
                value: ByteArray,
            ) {
                // Notifications are passive; don't touch the queue.
                nativeOnCharacteristicChanged(
                    gatt.device.address,
                    characteristic.uuid.toString(),
                    value,
                )
            }
        }

    // ── Connection management ──

    @JvmStatic
    fun connect(deviceAddr: String) {
        val ctx =
            context ?: run {
                Log.e(TAG, "context not initialized")
                return
            }

        // Close any stale GATT connection to avoid leaking clientIf slots.
        // Android has a limit of ~7 concurrent GATT clients.
        gattConnections.remove(deviceAddr)?.let { oldGatt ->
            oldGatt.disconnect()
            oldGatt.close()
            Log.d(TAG, "closed stale GATT for $deviceAddr")
        }

        val device =
            adapter?.getRemoteDevice(deviceAddr) ?: run {
                Log.e(TAG, "could not get remote device $deviceAddr")
                return
            }
        // TRANSPORT_LE ensures we connect over BLE, not classic Bluetooth.
        val gatt = device.connectGatt(ctx, false, gattCallback, BluetoothDevice.TRANSPORT_LE)
        if (gatt == null) {
            Log.e(TAG, "connectGatt returned null for $deviceAddr")
            nativeOnConnectionStateChanged(deviceAddr, false, 0)
            return
        }
        Log.d(TAG, "connecting to $deviceAddr")
    }

    @JvmStatic
    fun disconnect(deviceAddr: String) {
        gattConnections[deviceAddr]?.let { gatt ->
            gatt.disconnect()
            Log.d(TAG, "disconnecting from $deviceAddr")
        }
    }

    /**
     * Clear the GATT service cache for [deviceAddr] by invoking the hidden
     * `BluetoothGatt.refresh()` method via reflection. Returns false if no
     * active GATT handle exists or the reflective call throws. Used to
     * recover from stale cached service tables after peer reboots (status
     * 133 errors).
     */
    @JvmStatic
    fun refresh(deviceAddr: String): Boolean {
        val gatt = gattConnections[deviceAddr] ?: return false
        return try {
            val method = gatt.javaClass.getMethod("refresh")
            method.invoke(gatt) as Boolean
        } catch (e: Exception) {
            Log.w(TAG, "refresh failed for $deviceAddr: ${e.message}")
            false
        }
    }

    // ── GATT operations (serialized via per-device queue) ──

    @JvmStatic
    fun discoverServices(deviceAddr: String): Int {
        val gatt = gattConnections[deviceAddr] ?: return STATUS_NOT_CONNECTED
        val q = queueFor(deviceAddr)
        scope.launch {
            val result =
                q.enqueue<Unit>(
                    name = "discover-services",
                    timeoutMs = 10000L,
                    kick = {
                        val nonce = q.currentNonce() ?: return@enqueue false
                        val key = "$deviceAddr:services"
                        pendingNonces[key] = nonce
                        val started = gatt.discoverServices()
                        if (!started) {
                            pendingNonces.remove(key)
                        }
                        started
                    },
                )
            if (result.isFailure) {
                Log.w(TAG, "discoverServices queue failed for $deviceAddr: ${result.exceptionOrNull()?.message}")
                nativeOnServicesDiscovered(deviceAddr, "[]")
            }
        }
        return STATUS_SUCCESS
    }

    @JvmStatic
    fun readCharacteristic(
        deviceAddr: String,
        charUuid: String,
    ): Int {
        val gatt = gattConnections[deviceAddr] ?: return STATUS_NOT_CONNECTED
        val char = findCharacteristic(gatt, charUuid) ?: return STATUS_CHAR_NOT_FOUND
        val q = queueFor(deviceAddr)
        scope.launch {
            val result =
                q.enqueue<Unit>(
                    name = "read-$charUuid",
                    timeoutMs = 5000L,
                    kick = {
                        val nonce = q.currentNonce() ?: return@enqueue false
                        val key = "$deviceAddr:read:$charUuid"
                        pendingNonces[key] = nonce
                        val started = gatt.readCharacteristic(char)
                        if (!started) {
                            pendingNonces.remove(key)
                        }
                        started
                    },
                )
            if (result.isFailure) {
                Log.w(TAG, "read $charUuid queue failed: ${result.exceptionOrNull()?.message}")
                nativeOnCharacteristicRead(deviceAddr, charUuid, byteArrayOf(), BluetoothGatt.GATT_FAILURE)
            }
        }
        return STATUS_SUCCESS
    }

    @JvmStatic
    fun writeCharacteristic(
        deviceAddr: String,
        charUuid: String,
        value: ByteArray,
        writeType: Int,
    ): Int {
        val gatt = gattConnections[deviceAddr] ?: return STATUS_NOT_CONNECTED
        val char = findCharacteristic(gatt, charUuid) ?: return STATUS_CHAR_NOT_FOUND
        val q = queueFor(deviceAddr)
        scope.launch {
            val result =
                q.enqueue<Int>(
                    name = "write-$charUuid",
                    timeoutMs = 5000L,
                    kick = {
                        val nonce = q.currentNonce() ?: return@enqueue false
                        val nonceKey = "$deviceAddr:write:$charUuid"
                        if (writeType == BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE) {
                            // Mark before the framework can fire onCharacteristicWrite.
                            noResponseHandled[deviceAddr] = true
                        } else {
                            pendingNonces[nonceKey] = nonce
                        }
                        val ret = gatt.writeCharacteristic(char, value, writeType)
                        if (ret != BluetoothStatusCodes.SUCCESS) {
                            noResponseHandled.remove(deviceAddr)
                            pendingNonces.remove(nonceKey)
                            return@enqueue false
                        }
                        if (writeType == BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE) {
                            // Don't wait for a callback the platform may not deliver.
                            q.completeCurrent<Int>(nonce, BluetoothGatt.GATT_SUCCESS)
                            noResponseHandled.remove(deviceAddr)
                        }
                        true
                    },
                )
            if (result.isFailure) {
                Log.w(TAG, "write $charUuid queue failed: ${result.exceptionOrNull()?.message}")
                nativeOnCharacteristicWrite(deviceAddr, charUuid, BluetoothGatt.GATT_FAILURE)
            } else if (writeType == BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE) {
                nativeOnCharacteristicWrite(deviceAddr, charUuid, BluetoothGatt.GATT_SUCCESS)
            }
            // For write-with-response, onCharacteristicWrite fires the native
            // callback after calling completeCurrent — don't duplicate here.
        }
        return STATUS_SUCCESS
    }

    @JvmStatic
    fun subscribeCharacteristic(
        deviceAddr: String,
        charUuid: String,
    ): Int {
        val gatt = gattConnections[deviceAddr] ?: return STATUS_NOT_CONNECTED
        val char = findCharacteristic(gatt, charUuid) ?: return STATUS_CHAR_NOT_FOUND

        if (!gatt.setCharacteristicNotification(char, true)) return STATUS_GATT_FAILED

        // Write to CCCD to enable notifications on the remote device.
        val cccdUuid = UUID.fromString("00002902-0000-1000-8000-00805f9b34fb")
        val descriptor = char.getDescriptor(cccdUuid) ?: return STATUS_CHAR_NOT_FOUND
        val q = queueFor(deviceAddr)
        scope.launch {
            val result =
                q.enqueue<Unit>(
                    name = "subscribe-cccd-$charUuid",
                    timeoutMs = 5000L,
                    kick = {
                        val nonce = q.currentNonce() ?: return@enqueue false
                        val key = "$deviceAddr:cccd:$charUuid"
                        pendingNonces[key] = nonce
                        val ret =
                            gatt.writeDescriptor(
                                descriptor,
                                BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE,
                            )
                        if (ret != BluetoothStatusCodes.SUCCESS) {
                            pendingNonces.remove(key)
                        }
                        ret == BluetoothStatusCodes.SUCCESS
                    },
                )
            if (result.isFailure) {
                Log.w(TAG, "subscribe $charUuid queue failed: ${result.exceptionOrNull()?.message}")
            }
        }
        return STATUS_SUCCESS
    }

    @JvmStatic
    fun unsubscribeCharacteristic(
        deviceAddr: String,
        charUuid: String,
    ): Int {
        val gatt = gattConnections[deviceAddr] ?: return STATUS_NOT_CONNECTED
        val char = findCharacteristic(gatt, charUuid) ?: return STATUS_CHAR_NOT_FOUND

        // Always disable local notification state first, even if the CCCD
        // write below fails. The remote side will eventually notice via timeout.
        gatt.setCharacteristicNotification(char, false)

        val cccdUuid = UUID.fromString("00002902-0000-1000-8000-00805f9b34fb")
        val descriptor = char.getDescriptor(cccdUuid)
        if (descriptor != null) {
            val q = queueFor(deviceAddr)
            scope.launch {
                val result =
                    q.enqueue<Unit>(
                        name = "unsubscribe-cccd-$charUuid",
                        timeoutMs = 5000L,
                        kick = {
                            val nonce = q.currentNonce() ?: return@enqueue false
                            val key = "$deviceAddr:cccd:$charUuid"
                            pendingNonces[key] = nonce
                            val ret =
                                gatt.writeDescriptor(
                                    descriptor,
                                    BluetoothGattDescriptor.DISABLE_NOTIFICATION_VALUE,
                                )
                            if (ret != BluetoothStatusCodes.SUCCESS) {
                                pendingNonces.remove(key)
                            }
                            ret == BluetoothStatusCodes.SUCCESS
                        },
                    )
                if (result.isFailure) {
                    Log.w(TAG, "unsubscribe $charUuid queue failed: ${result.exceptionOrNull()?.message}")
                }
            }
        }
        return STATUS_SUCCESS
    }

    @JvmStatic
    fun isPowered(): Boolean = adapter?.isEnabled == true

    @JvmStatic
    fun getMtu(deviceAddr: String): Int = mtuMap[deviceAddr] ?: 23

    // ── L2CAP ──

    @JvmStatic
    fun openL2capChannel(
        deviceAddr: String,
        psm: Int,
    ) {
        if (android.os.Build.VERSION.SDK_INT < 29) {
            nativeOnL2capChannelError(deviceAddr, "L2CAP requires API 29+")
            return
        }

        val device =
            adapter?.getRemoteDevice(deviceAddr) ?: run {
                nativeOnL2capChannelError(deviceAddr, "device not found")
                return
            }

        Thread {
            try {
                val socket = device.createInsecureL2capChannel(psm)
                socket.connect()
                val socketId = l2cap.register(socket)
                nativeOnL2capChannelOpened(deviceAddr, socketId, false)
                l2cap.startReadLoop(socketId, deviceAddr, socket)
            } catch (e: Exception) {
                Log.e(TAG, "L2CAP connect failed: ${e.message}")
                nativeOnL2capChannelError(deviceAddr, e.message ?: "connect failed")
            }
        }.start()
    }

    @JvmStatic
    fun writeL2cap(
        socketId: Int,
        data: ByteArray,
    ) = l2cap.write(socketId, data)

    @JvmStatic
    fun closeL2cap(socketId: Int) = l2cap.close(socketId)

    // ── Helpers ──

    private fun findCharacteristic(
        gatt: BluetoothGatt,
        charUuid: String,
    ): BluetoothGattCharacteristic? {
        val uuid = UUID.fromString(charUuid)
        for (service in gatt.services) {
            val char = service.getCharacteristic(uuid)
            if (char != null) return char
        }
        return null
    }

    /**
     * Serialize discovered services to a JSON array. Each service is:
     * {"uuid": "...", "characteristics": [{"uuid": "...", "properties": N}]}
     *
     * We build JSON manually to avoid pulling in a JSON library dependency.
     */
    private fun servicesToJson(services: List<BluetoothGattService>): String {
        val sb = StringBuilder("[")
        for ((i, svc) in services.withIndex()) {
            if (i > 0) sb.append(",")
            sb.append("{\"uuid\":\"").append(svc.uuid).append("\",\"characteristics\":[")
            for ((j, ch) in svc.characteristics.withIndex()) {
                if (j > 0) sb.append(",")
                sb
                    .append("{\"uuid\":\"")
                    .append(ch.uuid)
                    .append("\",\"properties\":")
                    .append(ch.properties)
                    .append("}")
            }
            sb.append("]}")
        }
        sb.append("]")
        return sb.toString()
    }
}
