/*
 * Copyright (c) 2026 Element Creations Ltd.
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial.
 * Please see LICENSE files in the repository root for full details.
 */

package io.element.neutrino

import android.content.Context

/**
 * Bootstraps the bundled `blew` BLE backend's JNI layer for the iroh-over-BLE
 * federation transport.
 *
 * `blew`'s Android backend needs the process `JavaVM` + Android [Context]
 * registered with the native `ndk_context` before any BLE call. Call
 * [initialise] **once at app startup** (with the application context) before
 * invoking `bleSmokeTest` (or, later, any BLE federation path) — UNLESS the host
 * already initialises `ndk_context` for its own Rust layer, in which case the
 * native side self-bootstraps and this call is unnecessary (but harmless).
 *
 * The native symbol (`Java_io_element_neutrino_NativeBle_initialise`) and all of
 * `blew`'s `nativeOn*` hooks live in `libneutrino.so`, so the `System.loadLibrary`
 * below makes every JNI `external fun` resolvable (uniffi loads the same library
 * separately via JNA for its own FFI).
 */
object NativeBle {
    init {
        System.loadLibrary("neutrino")
    }

    /**
     * Register the `JavaVM` + application [context] with the native layer and
     * initialise `blew`. Implemented in Rust (`crates/neutrino-ffi/src/ble_android.rs`).
     * Safe to call more than once; the native side initialises at most once.
     */
    external fun initialise(context: Context)
}
