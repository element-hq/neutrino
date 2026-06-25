package org.jakebot.blew

import android.bluetooth.BluetoothSocket
import android.util.Log
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Shared L2CAP socket management used by both [BleCentralManager] and [BlePeripheralManager].
 *
 * Handles socket registration, read loops, writes, and closes. JNI callbacks
 * (channel data, channel closed) are forwarded via the provided lambdas so each
 * manager routes to its own `external fun` declarations.
 *
 * @param startId  Starting socket ID. Central and peripheral use non-overlapping
 *                 ranges (1 vs 100 000) to avoid collisions in the shared Rust-side state.
 */
class L2capSocketManager(
    private val tag: String,
    private val onData: (socketId: Int, data: ByteArray) -> Unit,
    private val onClosed: (socketId: Int) -> Unit,
    startId: Int = 1,
) {
    private val sockets = ConcurrentHashMap<Int, BluetoothSocket>()
    private val nextId = AtomicInteger(startId)

    fun register(socket: BluetoothSocket): Int {
        val id = nextId.getAndIncrement()
        sockets[id] = socket
        return id
    }

    fun write(
        socketId: Int,
        data: ByteArray,
    ) {
        val socket = sockets[socketId] ?: return
        try {
            socket.outputStream.write(data)
            socket.outputStream.flush()
        } catch (e: Exception) {
            Log.e(tag, "L2CAP write failed (socket $socketId): ${e.message}")
            close(socketId)
        }
    }

    fun close(socketId: Int) {
        val socket = sockets.remove(socketId) ?: return
        try {
            socket.close()
        } catch (_: Exception) {
        }
        onClosed(socketId)
    }

    fun startReadLoop(
        socketId: Int,
        deviceAddr: String,
        socket: BluetoothSocket,
    ) {
        val buf = ByteArray(4096)
        try {
            val input = socket.inputStream
            while (true) {
                val n = input.read(buf)
                if (n < 0) break
                onData(socketId, buf.copyOf(n))
            }
        } catch (e: Exception) {
            Log.d(tag, "L2CAP read ended (socket $socketId): ${e.message}")
        }
        close(socketId)
    }
}
