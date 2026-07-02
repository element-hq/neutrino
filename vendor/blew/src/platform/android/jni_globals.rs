use std::sync::OnceLock;

use jni::objects::{JClass, JObject, JValue};
use jni::refs::Global;
use jni::{Env, JavaVM, jni_sig, jni_str};

static JVM: OnceLock<JavaVM> = OnceLock::new();
static CLASS_CENTRAL: OnceLock<Global<JClass<'static>>> = OnceLock::new();
static CLASS_PERIPHERAL: OnceLock<Global<JClass<'static>>> = OnceLock::new();

/// Store the JVM reference and cache class lookups for later use by the Android BLE backends.
///
/// Uses the activity's classloader (via `ndk_context`) to find classes, so this works
/// even when called from a Rust background thread. `env.find_class()` would fail there
/// because Rust threads get the system classloader which can't see APK classes.
///
/// Called once during plugin initialization (e.g. by `tauri-plugin-blew`).
/// Panics if called more than once or if class lookup fails.
pub fn init_jvm(vm: JavaVM) {
    vm.attach_current_thread(|env| {
        let activity =
            unsafe { JObject::from_raw(env, ndk_context::android_context().context().cast()) };
        let class_loader = env
            .call_method(
                &activity,
                jni_str!("getClassLoader"),
                jni_sig!("()Ljava/lang/ClassLoader;"),
                &[],
            )
            .expect("getClassLoader failed")
            .l()
            .expect("not an object");

        let central_ref = load_class(env, &class_loader, "org.jakebot.blew.BleCentralManager");
        let peripheral_ref =
            load_class(env, &class_loader, "org.jakebot.blew.BlePeripheralManager");

        let _ = CLASS_CENTRAL.set(central_ref);
        let _ = CLASS_PERIPHERAL.set(peripheral_ref);

        // Don't let JNI try to delete the activity reference -- we don't own it.
        std::mem::forget(activity);

        Ok::<_, jni::errors::Error>(())
    })
    .expect("init_jvm failed");

    JVM.set(vm).expect("JVM already initialized");
}

/// Load a class by name using the given classloader, returning a Global.
fn load_class(env: &mut Env, class_loader: &JObject, class_name: &str) -> Global<JClass<'static>> {
    let j_name = env.new_string(class_name).expect("new_string");
    let cls = env
        .call_method(
            class_loader,
            jni_str!("loadClass"),
            jni_sig!("(Ljava/lang/String;)Ljava/lang/Class;"),
            &[JValue::Object(&j_name)],
        )
        .unwrap_or_else(|e| panic!("{class_name} not found: {e}"))
        .l()
        .expect("not an object");
    let cls = unsafe { JClass::from_raw(env, cls.as_raw()) };
    env.new_global_ref(cls).expect("global ref")
}

/// Get a reference to the stored JVM.
///
/// Panics if [`init_jvm`] was not called first.
pub(crate) fn jvm() -> &'static JavaVM {
    JVM.get()
        .expect("JVM not initialized -- did you register tauri-plugin-blew?")
}

/// Get a cached `Global<JClass>` for `org.jakebot.blew.BleCentralManager`.
pub(crate) fn central_class() -> &'static Global<JClass<'static>> {
    CLASS_CENTRAL.get().expect("JNI classes not initialized")
}

/// Get a cached `Global<JClass>` for `org.jakebot.blew.BlePeripheralManager`.
pub(crate) fn peripheral_class() -> &'static Global<JClass<'static>> {
    CLASS_PERIPHERAL.get().expect("JNI classes not initialized")
}
