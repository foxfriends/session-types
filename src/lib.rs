//! `session_types`
//!
//! This is an implementation of *session types* in Rust.
//!
//! The channels in Rusts standard library are useful for a great many things,
//! but they're restricted to a single type. Session types allows one to use a
//! single channel for transferring values of different types, depending on the
//! context in which it is used. Specifically, a session typed channel always
//! carry a *protocol*, which dictates how communication is to take place.
//!
//! For example, imagine that two threads, `A` and `B` want to communicate with
//! the following pattern:
//!
//!  1. `A` sends an integer to `B`.
//!  2. `B` sends a boolean to `A` depending on the integer received.
//!
//! With session types, this could be done by sharing a single channel. From
//! `A`'s point of view, it would have the type `int ! (bool ? eps)` where `t ! r`
//! is the protocol "send something of type `t` then proceed with
//! protocol `r`", the protocol `t ? r` is "receive something of type `t` then proceed
//! with protocol `r`, and `eps` is a special marker indicating the end of a
//! communication session.
//!
//! Our session type library allows the user to create channels that adhere to a
//! specified protocol. For example, a channel like the above would have the type
//! `Chan<(), Send<i64, Recv<bool, Eps>>>`, and the full program could look like this:
//!
//! ```
//! extern crate session_types;
//! use session_types::*;
//!
//! type Server = Recv<i64, Send<bool, Eps>>;
//! type Client = Send<i64, Recv<bool, Eps>>;
//!
//! fn srv(c: Chan<(), Server>) {
//!     let (c, n) = c.recv();
//!     if n % 2 == 0 {
//!         c.send(true).close()
//!     } else {
//!         c.send(false).close()
//!     }
//! }
//!
//! fn cli(c: Chan<(), Client>) {
//!     let n = 42;
//!     let c = c.send(n);
//!     let (c, b) = c.recv();
//!
//!     if b {
//!         println!("{} is even", n);
//!     } else {
//!         println!("{} is odd", n);
//!     }
//!
//!     c.close();
//! }
//!
//! fn main() {
//!     connect(srv, cli);
//! }
//! ```
use std::marker;
use std::thread::spawn;
use std::marker::PhantomData;

use futures_util::StreamExt as _;
use ipc_channel::ipc::{channel, IpcSender as Sender, IpcReceiver as Receiver};
use ipc_channel::asynch::IpcStream as Stream;

pub use Branch::*;

#[derive(serde::Serialize, serde::Deserialize)]
struct Packet {
    content: String,
}

/// A session typed channel. `P` is the protocol and `E` is the environment,
/// containing potential recursion targets
#[must_use]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Chan<E, P>(Sender<Packet>, Receiver<Packet>, PhantomData<(E, P)>);

impl<E, P> Chan<E, P> {
    pub fn to_async(self) -> ChanAsync<E, P> {
        ChanAsync(self.0, self.1.to_stream(), PhantomData)
    }
}

pub struct ChanAsync<E, P>(Sender<Packet>, Stream<Packet>, PhantomData<(E, P)>);

unsafe impl<E: marker::Send, P: marker::Send> marker::Send for Chan<E, P> {}

unsafe fn write_chan<A: serde::Serialize, E, P>(&Chan(ref tx, _, _): &Chan<E, P>, x: A) {
    let content = serde_json::to_string(&x).unwrap();
    tx.send(Packet { content }).unwrap()
}

unsafe fn write_chan_async<A: serde::Serialize, E, P>(&ChanAsync(ref tx, _, _): &ChanAsync<E, P>, x: A) {
    let content = serde_json::to_string(&x).unwrap();
    tx.send(Packet { content }).unwrap()
}

unsafe fn read_chan<A, E, P>(&Chan(_, ref rx, _): &Chan<E, P>) -> A
where for<'de> A: serde::Deserialize<'de> {
    loop {
        let message = rx.recv();
        let Packet { content } = match message {
            Err(..) => continue,
            Ok(packet) => packet,
        };
        return match serde_json::from_str(&content[..]) {
            Err(..) => continue,
            Ok(response) => response,
        }
    }
}

async unsafe fn read_chan_async<A, E, P>(&mut ChanAsync(_, ref mut rx, _): &mut ChanAsync<E, P>) -> A
where for<'de> A: serde::Deserialize<'de> {
    loop {
        let message = rx.next().await.unwrap();
        let Packet { content } = match message {
            Err(..) => continue,
            Ok(packet) => packet,
        };
        return match serde_json::from_str(&content[..]) {
            Err(..) => continue,
            Ok(response) => response,
        }
    }
}

/// Peano numbers: Zero
#[allow(missing_copy_implementations)]
pub struct Z;

/// Peano numbers: Increment
pub struct S<N>(PhantomData<N>);

/// End of communication session (epsilon)
#[allow(missing_copy_implementations)]
pub struct Eps;

/// Receive `A`, then `P`
pub struct Recv<A, P>(PhantomData<(A, P)>);

/// Send `A`, then `P`
pub struct Send<A, P>(PhantomData<(A, P)>);

/// Active choice between `P` and `Q`
pub struct Choose<P, Q>(PhantomData<(P, Q)>);

/// Passive choice (offer) between `P` and `Q`
pub struct Offer<P, Q>(PhantomData<(P, Q)>);

/// Enter a recursive environment
pub struct Rec<P>(PhantomData<P>);

/// Recurse. N indicates how many layers of the recursive environment we recurse
/// out of.
pub struct Var<N>(PhantomData<N>);

/// The HasDual trait defines the dual relationship between protocols.
///
/// Any valid protocol has a corresponding dual.
///
/// This trait is sealed and cannot be implemented outside of session-types
pub trait HasDual: private::Sealed {
    type Dual;
}

impl HasDual for Eps {
    type Dual = Eps;
}

impl<A, P: HasDual> HasDual for Send<A, P> {
    type Dual = Recv<A, P::Dual>;
}

impl<A, P: HasDual> HasDual for Recv<A, P> {
    type Dual = Send<A, P::Dual>;
}

impl<P: HasDual, Q: HasDual> HasDual for Choose<P, Q> {
    type Dual = Offer<P::Dual, Q::Dual>;
}

impl<P: HasDual, Q: HasDual> HasDual for Offer<P, Q> {
    type Dual = Choose<P::Dual, Q::Dual>;
}

impl HasDual for Var<Z> {
    type Dual = Var<Z>;
}

impl<N> HasDual for Var<S<N>> {
    type Dual = Var<S<N>>;
}

impl<P: HasDual> HasDual for Rec<P> {
    type Dual = Rec<P::Dual>;
}

pub enum Branch<L, R> {
    Left(L),
    Right(R),
}

impl<E> Chan<E, Eps> {
    /// Close a channel. Should always be used at the end of your program.
    pub fn close(self) { }
}

impl<E> ChanAsync<E, Eps> {
    /// Close a channel. Should always be used at the end of your program.
    pub fn close(self) { }
}

impl<E, P> Chan<E, P> {
    unsafe fn cast<E2, P2>(self) -> Chan<E2, P2> {
        Chan(self.0, self.1, PhantomData)
    }
}

impl<E, P> ChanAsync<E, P> {
    unsafe fn cast<E2, P2>(self) -> ChanAsync<E2, P2> {
        ChanAsync(self.0, self.1, PhantomData)
    }
}

impl<E, P, A: serde::Serialize> Chan<E, Send<A, P>> {
    /// Send a value of type `A` over the channel. Returns a channel with
    /// protocol `P`
    #[must_use]
    pub fn send(self, v: A) -> Chan<E, P> {
        unsafe {
            write_chan(&self, v);
            self.cast()
        }
    }
}

impl<E, P, A: serde::Serialize> ChanAsync<E, Send<A, P>> {
    /// Send a value of type `A` over the channel. Returns a channel with
    /// protocol `P`
    #[must_use]
    pub fn send(self, v: A) -> ChanAsync<E, P> {
        unsafe {
            write_chan_async(&self, v);
            self.cast()
        }
    }
}

impl<E, P, A> Chan<E, Recv<A, P>>
where for<'de> A: serde::Deserialize<'de> {
    /// Receives a value of type `A` from the channel. Returns a tuple
    /// containing the resulting channel and the received value.
    #[must_use]
    pub fn recv(self) -> (Chan<E, P>, A) {
        unsafe {
            let v = read_chan(&self);
            (self.cast(), v)
        }
    }
}

impl<E, P, A> ChanAsync<E, Recv<A, P>>
where for<'de> A: serde::Deserialize<'de> {
    /// Receives a value of type `A` from the channel. Returns a tuple
    /// containing the resulting channel and the received value.
    #[must_use]
    pub async fn recv(mut self) -> (ChanAsync<E, P>, A) {
        unsafe {
            let v = read_chan_async(&mut self).await;
            (self.cast(), v)
        }
    }
}

impl<E, P, Q> Chan<E, Choose<P, Q>> {
    /// Perform an active choice, selecting protocol `P`.
    #[must_use]
    pub fn sel1(self) -> Chan<E, P> {
        unsafe {
            write_chan(&self, true);
            self.cast()
        }
    }

    /// Perform an active choice, selecting protocol `Q`.
    #[must_use]
    pub fn sel2(self) -> Chan<E, Q> {
        unsafe {
            write_chan(&self, false);
            self.cast()
        }
    }
}

impl<E, P, Q> ChanAsync<E, Choose<P, Q>> {
    /// Perform an active choice, selecting protocol `P`.
    #[must_use]
    pub fn sel1(self) -> ChanAsync<E, P> {
        unsafe {
            write_chan_async(&self, true);
            self.cast()
        }
    }

    /// Perform an active choice, selecting protocol `Q`.
    #[must_use]
    pub fn sel2(self) -> ChanAsync<E, Q> {
        unsafe {
            write_chan_async(&self, false);
            self.cast()
        }
    }
}

/// Convenience function. This is identical to `.sel2()`
impl<Z, A, B> Chan<Z, Choose<A, B>> {
    #[must_use]
    pub fn skip(self) -> Chan<Z, B> {
        self.sel2()
    }
}

/// Convenience function. This is identical to `.sel2()`
impl<Z, A, B> ChanAsync<Z, Choose<A, B>> {
    #[must_use]
    pub fn skip(self) -> ChanAsync<Z, B> {
        self.sel2()
    }
}

/// Convenience function. This is identical to `.sel2().sel2()`
impl<Z, A, B, C> Chan<Z, Choose<A, Choose<B, C>>> {
    #[must_use]
    pub fn skip2(self) -> Chan<Z, C> {
        self.sel2().sel2()
    }
}

/// Convenience function. This is identical to `.sel2().sel2()`
impl<Z, A, B, C> ChanAsync<Z, Choose<A, Choose<B, C>>> {
    #[must_use]
    pub fn skip2(self) -> ChanAsync<Z, C> {
        self.sel2().sel2()
    }
}

/// Convenience function. This is identical to `.sel2().sel2().sel2()`
impl<Z, A, B, C, D> Chan<Z, Choose<A, Choose<B, Choose<C, D>>>> {
    #[must_use]
    pub fn skip3(self) -> Chan<Z, D> {
        self.sel2().sel2().sel2()
    }
}

/// Convenience function. This is identical to `.sel2().sel2().sel2()`
impl<Z, A, B, C, D> ChanAsync<Z, Choose<A, Choose<B, Choose<C, D>>>> {
    #[must_use]
    pub fn skip3(self) -> ChanAsync<Z, D> {
        self.sel2().sel2().sel2()
    }
}

/// Convenience function. This is identical to `.sel2().sel2().sel2().sel2()`
impl<Z, A, B, C, D, E> Chan<Z, Choose<A, Choose<B, Choose<C, Choose<D, E>>>>> {
    #[must_use]
    pub fn skip4(self) -> Chan<Z, E> {
        self.sel2().sel2().sel2().sel2()
    }
}

/// Convenience function. This is identical to `.sel2().sel2().sel2().sel2()`
impl<Z, A, B, C, D, E> ChanAsync<Z, Choose<A, Choose<B, Choose<C, Choose<D, E>>>>> {
    #[must_use]
    pub fn skip4(self) -> ChanAsync<Z, E> {
        self.sel2().sel2().sel2().sel2()
    }
}

/// Convenience function. This is identical to `.sel2().sel2().sel2().sel2().sel2()`
impl<Z, A, B, C, D, E, F> Chan<Z, Choose<A, Choose<B, Choose<C, Choose<D, Choose<E, F>>>>>> {
    #[must_use]
    pub fn skip5(self) -> Chan<Z, F> {
        self.sel2().sel2().sel2().sel2().sel2()
    }
}

/// Convenience function. This is identical to `.sel2().sel2().sel2().sel2().sel2()`
impl<Z, A, B, C, D, E, F> ChanAsync<Z, Choose<A, Choose<B, Choose<C, Choose<D, Choose<E, F>>>>>> {
    #[must_use]
    pub fn skip5(self) -> ChanAsync<Z, F> {
        self.sel2().sel2().sel2().sel2().sel2()
    }
}

/// Convenience function.
impl<Z, A, B, C, D, E, F, G>
    Chan<Z, Choose<A, Choose<B, Choose<C, Choose<D, Choose<E, Choose<F, G>>>>>>> {
    #[must_use]
    pub fn skip6(self) -> Chan<Z, G> {
        self.sel2().sel2().sel2().sel2().sel2().sel2()
    }
}

/// Convenience function.
impl<Z, A, B, C, D, E, F, G>
    ChanAsync<Z, Choose<A, Choose<B, Choose<C, Choose<D, Choose<E, Choose<F, G>>>>>>> {
    #[must_use]
    pub fn skip6(self) -> ChanAsync<Z, G> {
        self.sel2().sel2().sel2().sel2().sel2().sel2()
    }
}

/// Convenience function.
impl<Z, A, B, C, D, E, F, G, H>
    Chan<Z, Choose<A, Choose<B, Choose<C, Choose<D, Choose<E, Choose<F, Choose<G, H>>>>>>>> {
    #[must_use]
    pub fn skip7(self) -> Chan<Z, H> {
        self.sel2().sel2().sel2().sel2().sel2().sel2().sel2()
    }
}

/// Convenience function.
impl<Z, A, B, C, D, E, F, G, H>
    ChanAsync<Z, Choose<A, Choose<B, Choose<C, Choose<D, Choose<E, Choose<F, Choose<G, H>>>>>>>> {
    #[must_use]
    pub fn skip7(self) -> ChanAsync<Z, H> {
        self.sel2().sel2().sel2().sel2().sel2().sel2().sel2()
    }
}

impl<E, P, Q> Chan<E, Offer<P, Q>> {
    /// Passive choice. This allows the other end of the channel to select one
    /// of two options for continuing the protocol: either `P` or `Q`.
    #[must_use]
    pub fn offer(self) -> Branch<Chan<E, P>, Chan<E, Q>> {
        unsafe {
            let b = read_chan(&self);
            if b {
                Left(self.cast())
            } else {
                Right(self.cast())
            }
        }
    }
}

impl<E, P, Q> ChanAsync<E, Offer<P, Q>> {
    /// Passive choice. This allows the other end of the channel to select one
    /// of two options for continuing the protocol: either `P` or `Q`.
    #[must_use]
    pub async fn offer(mut self) -> Branch<ChanAsync<E, P>, ChanAsync<E, Q>> {
        unsafe {
            let b = read_chan_async(&mut self).await;
            if b {
                Left(self.cast())
            } else {
                Right(self.cast())
            }
        }
    }
}

impl<E, P> Chan<E, Rec<P>> {
    /// Enter a recursive environment, putting the current environment on the
    /// top of the environment stack.
    #[must_use]
    pub fn enter(self) -> Chan<(P, E), P> {
        unsafe { self.cast() }
    }
}

impl<E, P> ChanAsync<E, Rec<P>> {
    /// Enter a recursive environment, putting the current environment on the
    /// top of the environment stack.
    #[must_use]
    pub fn enter(self) -> ChanAsync<(P, E), P> {
        unsafe { self.cast() }
    }
}

impl<E, P> Chan<(P, E), Var<Z>> {
    /// Recurse to the environment on the top of the environment stack.
    #[must_use]
    pub fn zero(self) -> Chan<(P, E), P> {
        unsafe { self.cast() }
    }
}

impl<E, P> ChanAsync<(P, E), Var<Z>> {
    /// Recurse to the environment on the top of the environment stack.
    #[must_use]
    pub fn zero(self) -> ChanAsync<(P, E), P> {
        unsafe { self.cast() }
    }
}

impl<E, P, N> Chan<(P, E), Var<S<N>>> {
    /// Pop the top environment from the environment stack.
    #[must_use]
    pub fn succ(self) -> Chan<E, Var<N>> {
        unsafe { self.cast() }
    }
}

impl<E, P, N> ChanAsync<(P, E), Var<S<N>>> {
    /// Pop the top environment from the environment stack.
    #[must_use]
    pub fn succ(self) -> ChanAsync<E, Var<N>> {
        unsafe { self.cast() }
    }
}

#[must_use]
pub fn session_channel<P: HasDual>() -> std::io::Result<(Chan<(), P>, Chan<(), P::Dual>)> {
    let (tx1, rx1) = channel()?;
    let (tx2, rx2) = channel()?;

    let c1 = Chan(tx1, rx2, PhantomData);
    let c2 = Chan(tx2, rx1, PhantomData);

    Ok((c1, c2))
}

/// Connect two functions using a session typed channel.
pub fn connect<F1, F2, P>(srv: F1, cli: F2) -> std::io::Result<()>
where
    F1: Fn(Chan<(), P>) + marker::Send + 'static,
    F2: Fn(Chan<(), P::Dual>) + marker::Send,
    P: HasDual + marker::Send + 'static,
    P::Dual: HasDual + marker::Send + 'static
{
    let (c1, c2) = session_channel()?;
    let t = spawn(move || srv(c1));
    cli(c2);
    t.join().unwrap();
    Ok(())
}

mod private {
    use super::*;
    pub trait Sealed {}

    // Impl for all exported protocol types
    impl Sealed for Eps {}
    impl<A, P> Sealed for Send<A, P> {}
    impl<A, P> Sealed for Recv<A, P> {}
    impl<P, Q> Sealed for Choose<P, Q> {}
    impl<P, Q> Sealed for Offer<P, Q> {}
    impl<Z> Sealed for Var<Z> {}
    impl<P> Sealed for Rec<P> {}
}

/// This macro is convenient for server-like protocols of the form:
///
/// `Offer<A, Offer<B, Offer<C, ... >>>`
///
/// # Examples
///
/// Assume we have a protocol `Offer<Recv<u64, Eps>, Offer<Recv<String, Eps>,Eps>>>`
/// we can use the `offer!` macro as follows:
///
/// ```rust
/// extern crate session_types;
/// use session_types::*;
/// use std::thread::spawn;
///
/// fn srv(c: Chan<(), Offer<Recv<u64, Eps>, Offer<Recv<String, Eps>, Eps>>>) {
///     offer! { c,
///         Number => {
///             let (c, n) = c.recv();
///             assert_eq!(42, n);
///             c.close();
///         },
///         String => {
///             c.recv().0.close();
///         },
///         Quit => {
///             c.close();
///         }
///     }
/// }
///
/// fn cli(c: Chan<(), Choose<Send<u64, Eps>, Choose<Send<String, Eps>, Eps>>>) {
///     c.sel1().send(42).close();
/// }
///
/// fn main() {
///     let (s, c) = session_channel();
///     spawn(move|| cli(c));
///     srv(s);
/// }
/// ```
///
/// The identifiers on the left-hand side of the arrows have no semantic
/// meaning, they only provide a meaningful name for the reader.
#[macro_export]
macro_rules! offer {
    ($id:ident, $branch:ident => $code:block $($t:tt)+) => (
        match $id.offer() {
            $crate::Left($id) => $code,
            $crate::Right($id) => offer!{ $id, $($t)+ }
        }
    );
    ($id:ident, $branch:ident => $code:expr, $($t:tt)+) => (
        match $id.offer() {
            $crate::Left($id) => $code,
            $crate::Right($id) => offer!{ $id, $($t)+ }
        }
    );
    ($id:ident, $branch:ident => $code:expr $(,)?) => ($code)
}

#[macro_export]
macro_rules! offer_async {
    ($id:ident, $branch:ident => $code:block $($t:tt)+) => (
        match $id.offer().await {
            $crate::Left($id) => $code,
            $crate::Right($id) => offer_async!{ $id, $($t)+ }
        }
    );
    ($id:ident, $branch:ident => $code:expr, $($t:tt)+) => (
        match $id.offer().await {
            $crate::Left($id) => $code,
            $crate::Right($id) => offer_async!{ $id, $($t)+ }
        }
    );
    ($id:ident, $branch:ident => $code:expr $(,)?) => ($code)
}

#[macro_export]
macro_rules! choose {
    ($choice:ty, $($t:tt)+) => (Choose<$choice, choose!($($t)+)>);
    ($choice:ty $(,)?) => ($choice)
}
