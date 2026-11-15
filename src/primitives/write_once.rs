#![allow(unused)]

use std::{
    cell::UnsafeCell,
    convert::Infallible,
    mem::MaybeUninit,
    sync::atomic::{AtomicU8, Ordering, fence},
};

/// A synchronization primitive which can nominally be written to only once.
/// This primitive can also be used in statics.
pub struct WriteOnce<T> {
    data: UnsafeCell<MaybeUninit<T>>,
    synchronizer: AtomicU8,
}

impl<T> Drop for WriteOnce<T> {
    fn drop(&mut self) {
        unsafe {
            (&mut *self.data.get()).assume_init_drop();
        };
    }
}

unsafe impl<T: Send> Send for WriteOnce<T> {}
unsafe impl<T: Send + Sync> Sync for WriteOnce<T> {}

impl<T> WriteOnce<T> {
    /// Data is initialized.
    const UNINIT: u8 = 1;
    /// Data is uninitialized.
    const INIT: u8 = 2;
    /// Some thread has a mutable reference to the underlynig data.
    const MUT: u8 = 3;
    /// Some thread has an immutable reference to the underlynig data.
    const IMMUT: u8 = 4;
    /// A state to prevent concurrent writes and thus UB.
    /// This is used inside [`get_or_init`].
    const BLOCK: u8 = 5;

    pub const fn new() -> Self {
        Self {
            data: UnsafeCell::new(MaybeUninit::uninit()),
            synchronizer: AtomicU8::new(Self::UNINIT),
        }
    }

    pub const fn new_init(init_value: T) -> Self {
        Self {
            data: UnsafeCell::new(MaybeUninit::new(init_value)),
            synchronizer: AtomicU8::new(Self::UNINIT),
        }
    }

    pub fn get(&self) -> Option<&T> {
        if self.synchronizer.load(Ordering::Relaxed) == Self::INIT {
            fence(Ordering::Acquire);
            // SAFETY: With happens before established, we could be sure that we
            // are calling this function on a `[MaybeUninit<T>]` that is initialized.
            // Also, this doesn't break any aliasing rules associated with `UnsafeCell`
            return Some(unsafe { (*self.data.get()).assume_init_ref() });
        }
        None
    }

    pub fn get_or_init<F>(&self, f: F) -> Result<&T, Infallible>
    where
        F: FnOnce() -> T,
    {
        loop {
            match self.get() {
                Some(value) => {
                    return Ok(value);
                }
                None => {
                    if self
                        .synchronizer
                        .compare_exchange(
                            Self::UNINIT,
                            Self::BLOCK,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        // SAFETY: The CAS above guarantees that only one thread will win the race to reach here.
                        unsafe {
                            (self.data.get()).write(MaybeUninit::new(f()));
                        };
                        self.synchronizer.store(Self::INIT, Ordering::Release);
                        // SAFETY: we have intialized above.
                        return Ok(unsafe { (*self.data.get()).assume_init_ref() });
                    }
                }
            }
        }
    }
}

// Guards would be required to support APIs like `get_mut`, etc.
pub struct GetGuard<'get, T> {
    once: &'get WriteOnce<T>,
}

#[cfg(test)]
mod tests {
    use super::WriteOnce;

    #[test]
    fn smoke() {
        static ONCE: WriteOnce<usize> = WriteOnce::new();
        std::thread::scope(|s| {
            s.spawn(|| assert!(ONCE.get_or_init(|| 1).is_ok()));
            s.spawn(|| assert!(ONCE.get_or_init(|| 2).is_ok()));
            s.spawn(|| assert!(ONCE.get_or_init(|| 3).is_ok()));
        });
    }

    #[test]
    fn only_gets_first_setted_value() {}

    #[test]
    fn stress() {
        static ONCE: WriteOnce<usize> = WriteOnce::new();
        std::thread::scope(|s| {
            for i in 0..100 {
                s.spawn(move || assert!(ONCE.get_or_init(|| i).is_ok()));
            }
        });
    }
}
