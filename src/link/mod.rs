/// A simple pull based link.  It is pull based in the sense that packets are only fetched on the input
/// when a packet is requested from the output. This link does not have the abilty store packets internally,
/// so all packets that enter either immediatly leave or are dropped, as dictated by the processor. Both sides of
/// this link are on the same thread, hence the label synchronous.
mod process;
pub(crate) use self::process::Process;

/// Input packets are placed into an intermediate channel that are pulled from the output asynchronously.
/// Asynchronous in that a packets may enter and leave this link asynchronously to each other.  This link is
/// useful for creating queues in the router, buffering, and creating `Task` boundries that can be processed on
/// different threads, or even different cores. Before packets are placed into the queue to be output, they are run
/// through the processor defined process function, often performing some sort of transformation.
mod queue;
pub(crate) use self::queue::{Queue, QueueStream};

/// Uses processor defined classifications to sort input into different streams, a good example would
/// be a flow that splits IPv4 and IPv6 packets, asynchronous. Packets are either dispatched to a
/// particular stream, or dropped.
mod classify;
pub(crate) use self::classify::Classify;

/// Fairly combines all inputs into a single output, asynchronous.
mod join;
pub(crate) use self::join::Join;

/// Copies all input to each of its outputs, asynchronous.
mod fork;

mod utils;
