# BlueFalcon 3.x only emits connection/service updates for peripherals in
# AndroidEngine._peripherals. MeshCore may register a retrievePeripheral()
# instance into that set via reflection when reconnecting by saved address.
-keepclassmembers class dev.bluefalcon.engine.android.AndroidEngine {
    *** _peripherals;
}
