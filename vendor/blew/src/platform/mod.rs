#[cfg(target_os = "android")]
pub mod android;
#[cfg(target_vendor = "apple")]
pub mod apple;
#[cfg(target_os = "linux")]
pub mod linux;

#[cfg(target_vendor = "apple")]
pub(crate) use apple::AppleCentral as PlatformCentral;
#[cfg(target_vendor = "apple")]
pub(crate) use apple::ApplePeripheral as PlatformPeripheral;

#[cfg(target_os = "linux")]
pub(crate) use linux::LinuxCentral as PlatformCentral;
#[cfg(target_os = "linux")]
pub(crate) use linux::LinuxPeripheral as PlatformPeripheral;

#[cfg(target_os = "android")]
pub(crate) use android::AndroidCentral as PlatformCentral;
#[cfg(target_os = "android")]
pub(crate) use android::AndroidPeripheral as PlatformPeripheral;

#[cfg(not(any(target_vendor = "apple", target_os = "linux", target_os = "android")))]
compile_error!("blew does not support this target platform (supported: Apple, Linux, Android)");
