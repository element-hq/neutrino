//! Shared helpers for Apple platform backends.

use objc2::rc::Retained;
use objc2_core_bluetooth::CBUUID;
use objc2_foundation::NSString;
use uuid::Uuid;

use crate::types::DeviceId;

/// Wraps `Retained<T>` to assert `Send + Sync`.
///
/// # Safety
/// CoreBluetooth is documented thread-safe on macOS 10.15+ / iOS 13+.
pub(crate) struct ObjcSend<T: objc2::Message>(pub(crate) Retained<T>);

// SAFETY: CoreBluetooth is thread-safe on macOS 10.15+ / iOS 13+.
unsafe impl<T: objc2::Message> Send for ObjcSend<T> {}
unsafe impl<T: objc2::Message> Sync for ObjcSend<T> {}

impl<T: objc2::Message> std::ops::Deref for ObjcSend<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

/// Retain an ObjC reference, wrapping in `ObjcSend`.
///
/// # Safety
/// The pointer must be non-null and point to a valid ObjC object.
pub(crate) unsafe fn retain_send<T: objc2::Message>(obj: &T) -> ObjcSend<T> {
    ObjcSend(unsafe { Retained::retain(std::ptr::from_ref::<T>(obj).cast_mut()).expect("retain") })
}

pub(crate) fn uuid_to_cbuuid(uuid: Uuid) -> Retained<CBUUID> {
    unsafe { CBUUID::UUIDWithString(&NSString::from_str(&uuid.to_string())) }
}

pub(crate) fn cbuuid_to_uuid(cb: &CBUUID) -> Option<Uuid> {
    unsafe { cb.UUIDString() }.to_string().parse().ok()
}

pub(crate) fn peripheral_device_id(p: &objc2_core_bluetooth::CBPeripheral) -> DeviceId {
    DeviceId(unsafe { p.identifier().UUIDString() }.to_string())
}

pub(crate) fn central_device_id(c: &objc2_core_bluetooth::CBCentral) -> DeviceId {
    DeviceId(unsafe { c.identifier().UUIDString() }.to_string())
}
