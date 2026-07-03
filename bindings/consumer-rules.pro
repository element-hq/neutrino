# Consumer R8/ProGuard rules shipped inside the neutrino bindings AAR.
#
# The blew BLE manager objects (org.jakebot.blew.*) are invoked from Rust via
# JNI name lookup (loadClass + GetStaticMethodID: areBlePermissionsGranted,
# isPowered, startAdvertising, startScan, ...). Nothing on the Kotlin side
# references those methods, so a minified consuming app strips/renames them
# and every JNI lookup fails at runtime. Notably blew maps a failed
# areBlePermissionsGranted lookup to "permissions not granted", which kills
# the embedded server before the client-server listener binds (infinite
# "Starting Neutrino" spinner on release builds).
-keep class org.jakebot.blew.** { *; }

# UniFFI bindings + the NativeBle JNI bootstrap: resolved reflectively by JNA
# (uniffi) and by native symbol name (Java_io_element_neutrino_NativeBle_*).
-keep class io.element.neutrino.** { *; }
