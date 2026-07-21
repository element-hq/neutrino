# Consumer R8/ProGuard rules shipped inside the neutrino bindings AAR.

# UniFFI bindings: resolved reflectively by JNA, so a minified consuming app
# must not strip or rename them.
-keep class io.element.neutrino.** { *; }
