// Copyright (c) 2026 Element Creations Ltd.
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial.

//! Android JNI bootstrap for `blew` (feature `ble`).
//!
//! `blew`'s Android BLE backend is not self-contained Rust: before any
//! `Central`/`Peripheral` call it needs the `JavaVM` + Android `Context`
//! registered with the `ndk_context` crate, and `blew::platform::android::init_jvm`
//! called exactly once. This replicates what `tauri-plugin-blew` does for Tauri
//! apps; here the host (EX Android) drives it.
//!
//! **NOTE (unverified):** none of the Android path below can be compiled in the
//! dev sandbox (no `aarch64-linux-android` std, no libdbus for the host `ble`
//! build), so it is written against the `jni` 0.22 / `blew` 0.2 / `ndk_context`
//! 0.1 source but compiled for the first time in the Android workspace. Expect
//! to iterate on first build.
//!
//! On non-Android hosts (the binding-generation build, which compiles `ble` on
//! the host so the Kotlin bindings include `ble_selftest`) this is a no-op:
//! `blew` uses CoreBluetooth / `bluer` there and needs no JNI bootstrap.

#[cfg(target_os = "android")]
mod imp {
    use std::os::raw::c_void;
    use std::sync::Once;

    use jni::objects::{JClass, JObject};
    use jni::refs::Reference;
    use jni::{EnvUnowned, JavaVM};

    const TARGET: &str = "neutrino::ble_selftest";

    /// `init_jvm` must be called at most once (it panics on a second call), so
    /// funnel every entry point through this.
    static INIT: Once = Once::new();

    /// Initialise `blew` from whatever the host has already registered with
    /// `ndk_context`. Idempotent. Returns an error (rather than panicking) if
    /// `ndk_context` has not been populated yet — i.e. the host neither set it
    /// for its own Rust nor called [`Java_io_element_neutrino_NativeBle_initialise`].
    pub(crate) fn ensure_initialised() -> Result<(), String> {
        let ctx = ndk_context::android_context();
        if ctx.vm().is_null() || ctx.context().is_null() {
            return Err(
                "ndk_context not set — call NativeBle.initialise(context) once at app startup"
                    .to_owned(),
            );
        }
        INIT.call_once(|| {
            // Safety: `ctx.vm()` is the process JavaVM pointer the host registered.
            let vm = unsafe { JavaVM::from_raw(ctx.vm().cast()) };
            // Reads `ndk_context().context()` for the APK classloader, then caches
            // blew's manager classes. Panics (logged via the self-test panic hook)
            // if the companion Kotlin classes aren't on the classpath.
            blew::platform::android::init_jvm(vm);
            tracing::info!(target: TARGET, "blew JNI initialised");
        });
        Ok(())
    }

    /// JNI entry the host calls once at startup, bound to a Kotlin
    /// `class io.element.neutrino.NativeBle { external fun initialise(context: Context) }`.
    /// Registers the `JavaVM` + application `Context` with `ndk_context`, then
    /// initialises `blew`. Only needed if the host has not already populated
    /// `ndk_context` for its own Rust layer.
    #[unsafe(no_mangle)]
    pub extern "system" fn Java_io_element_neutrino_NativeBle_initialise<'caller>(
        mut env: EnvUnowned<'caller>,
        _class: JClass<'caller>,
        context: JObject<'caller>,
    ) {
        let registered = env
            .with_env(|env| -> Result<bool, jni::errors::Error> {
                let vm = env.get_java_vm()?;
                // The context is a local ref valid only for this call; promote it
                // to a global ref and leak it so the raw pointer we hand to
                // `ndk_context` stays valid for the process lifetime.
                let ctx_global = env.new_global_ref(context)?;
                let ctx_ptr = ctx_global.as_raw();
                std::mem::forget(ctx_global);
                // Safety: `vm`/`ctx_ptr` are valid for the process lifetime.
                unsafe {
                    ndk_context::initialize_android_context(
                        vm.get_raw().cast::<c_void>(),
                        ctx_ptr.cast::<c_void>(),
                    );
                }
                Ok(true)
            })
            .resolve::<jni::errors::ThrowRuntimeExAndDefault>();

        if registered {
            if let Err(e) = ensure_initialised() {
                tracing::error!(target: TARGET, "blew init after NativeBle.initialise failed: {e}");
            }
        }
    }
}

#[cfg(not(target_os = "android"))]
mod imp {
    /// No-op: non-Android `blew` backends need no JNI bootstrap.
    pub(crate) fn ensure_initialised() -> Result<(), String> {
        Ok(())
    }
}

pub(crate) use imp::ensure_initialised;
