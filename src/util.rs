use super::{Chan, ChanAsync};

pub type RootChan<P> = Chan<(), P>;
pub type LoopChan<P> = Chan<(P, ()), P>;

pub type RootChanAsync<P> = ChanAsync<(), P>;
pub type LoopChanAsync<P> = ChanAsync<(P, ()), P>;
