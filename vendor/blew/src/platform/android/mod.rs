pub mod central;
pub mod jni_globals;
pub mod jni_hooks;
pub(crate) mod l2cap_state;
pub mod peripheral;

pub use central::AndroidCentral;
pub use jni_globals::init_jvm;
pub use peripheral::AndroidPeripheral;

/// Check whether the app is running on an Android emulator.
///
/// Checks `Build.FINGERPRINT` for known emulator markers ("generic", "emulator", "sdk").
pub fn is_emulator() -> bool {
    use jni::{jni_sig, jni_str};
    let result: Result<bool, jni::errors::Error> =
        jni_globals::jvm().attach_current_thread(|env| {
            let build_class = env.find_class(jni_str!("android/os/Build"))?;
            let fingerprint = env.get_static_field(
                &build_class,
                jni_str!("FINGERPRINT"),
                jni_sig!("Ljava/lang/String;"),
            )?;
            let obj = fingerprint.l()?;
            let jstr = unsafe { jni::objects::JString::from_raw(env, obj.as_raw()) };
            #[allow(deprecated)]
            let fp: String = env.get_string(&jstr)?.into();
            let fp = fp.to_lowercase();
            Ok(fp.contains("generic") || fp.contains("emulator") || fp.contains("sdk"))
        });
    result.unwrap_or(false)
}

/// Check whether Android BLE runtime permissions have been granted.
///
/// Returns `true` when all required permissions are in place, `false` otherwise.
/// This is Android-specific; other platforms always return `true`.
pub fn are_ble_permissions_granted() -> bool {
    use jni::{jni_sig, jni_str};
    let result: Result<bool, jni::errors::Error> =
        jni_globals::jvm().attach_current_thread(|env| {
            let result = env.call_static_method(
                jni_globals::peripheral_class(),
                jni_str!("areBlePermissionsGranted"),
                jni_sig!("()Z"),
                &[],
            )?;
            Ok(result.z()?)
        });
    result.unwrap_or(false)
}

/// Trigger the Android BLE runtime permissions dialog.
///
/// Fire-and-forget: the OS dialog is shown asynchronously on the host
/// activity's UI thread. Callers should poll [`are_ble_permissions_granted`]
/// to learn the result.
///
/// Requires the `org.jakebot.blew.BlewPlugin` class to be loaded (i.e. the
/// Tauri plugin has been registered and its `load()` has run). If the class
/// is unavailable or the host activity hasn't been captured yet, this is a
/// no-op and a warning is logged.
pub fn request_ble_permissions() {
    use jni::objects::{JObject, JValue};
    use jni::{jni_sig, jni_str};
    let result: Result<(), jni::errors::Error> = jni_globals::jvm().attach_current_thread(|env| {
        let activity =
            unsafe { JObject::from_raw(env, ndk_context::android_context().context().cast()) };
        let class_loader = env
            .call_method(
                &activity,
                jni_str!("getClassLoader"),
                jni_sig!("()Ljava/lang/ClassLoader;"),
                &[],
            )?
            .l()?;
        let class_name = env.new_string("org.jakebot.blew.BlewPlugin")?;
        let class_obj = env
            .call_method(
                &class_loader,
                jni_str!("loadClass"),
                jni_sig!("(Ljava/lang/String;)Ljava/lang/Class;"),
                &[JValue::Object(&class_name)],
            )?
            .l()?;
        let class = unsafe { jni::objects::JClass::from_raw(env, class_obj.as_raw()) };
        env.call_static_method(
            &class,
            jni_str!("requestBlePermissions"),
            jni_sig!("()V"),
            &[],
        )?;
        drop(activity);
        Ok(())
    });
    if let Err(e) = result {
        tracing::warn!("request_ble_permissions failed: {e}");
    }
}
