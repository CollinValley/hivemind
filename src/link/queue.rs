use crate::link::utils::task_park::*;
use crate::{Link, PacketStream, Runnable, IntoLink};
use crossbeam::atomic::AtomicCell;
use crossbeam::crossbeam_channel;
use crossbeam::crossbeam_channel::{Receiver, Sender, TryRecvError};
use futures::prelude::*;
use futures::ready;
use futures::task::{Context, Poll};
use std::pin::Pin;
use std::sync::Arc;

/// A link used to create queues, buffers, or Task boundries. Packets may be
/// transformed with a Processor prior to being enqueued.
pub struct Queue<Packet: Send + Sized> {
    runnables: Vec<Runnable>,
    stream: PacketStream<Packet>,
}

impl<Packet: Send + Sized + 'static> Queue<Packet> {
    pub fn new(input: PacketStream<Packet>, cap: Option<usize>) -> Self {
        let (sender, reciever) = match cap {
            None => crossbeam_channel::unbounded::<Option<Packet>>(),
            Some(capacity) => crossbeam_channel::bounded::<Option<Packet>>(capacity),
        };

        let task_park: Arc<AtomicCell<TaskParkState>> =
            Arc::new(AtomicCell::new(TaskParkState::Empty));

        let runnable = QueueRunnable::new(input, sender, Arc::clone(&task_park));
        let stream = QueueStream::new(reciever, task_park);

        Queue {
            runnables: vec![Box::new(runnable)],
            stream: Box::new(stream),
        }
    }
}

impl<Packet: Send + Sized + 'static> IntoLink<Packet> for Queue<Packet> {
    fn into_link(self) -> Link<Packet> {
        Link {
            runnables: self.runnables,
            streams: vec![self.stream],
        }
    }
}

/// TODO: Fixup docs
/// The QueueIngressor is responsible for polling its input stream,
/// processing them using the `processor`s process function, and pushing the
/// output packet onto the to_egressor queue. It does work in batches, so it
/// will continue to pull packets as long as it can make forward progess,
/// after which it will return NotReady to sleep. This is handed to, and is
/// polled by the runtime.
pub struct QueueRunnable<Packet: Sized> {
    input_stream: PacketStream<Packet>,
    to_egressor: Sender<Option<Packet>>,
    task_park: Arc<AtomicCell<TaskParkState>>,
}

impl<Packet: Sized> QueueRunnable<Packet> {
    fn new(
        input_stream: PacketStream<Packet>,
        to_egressor: Sender<Option<Packet>>,
        task_park: Arc<AtomicCell<TaskParkState>>,
    ) -> Self {
        QueueRunnable {
            input_stream,
            to_egressor,
            task_park,
        }
    }
}

impl<Packet: Send + Sized> Unpin for QueueRunnable<Packet> {}

impl<Packet: Send + Sized> Future for QueueRunnable<Packet> {
    type Output = ();

    /// Implement Poll for Future for QueueIngressor
    ///
    /// This function continues to process
    /// packets off it's input queue until it reaches a point where it can not
    /// make forward progress. There are several cases:
    /// ###
    /// #1 The to_egressor queue is full, we wake the Egressor that we need
    /// awaking when there is work to do, and go to sleep by returning `Async::NotReady`.
    ///
    /// #2 The input_stream returns a NotReady, we sleep, with the assumption
    /// that whomever produced the NotReady will awaken the task in the Future.
    ///
    /// #3 We get a Ready(None), in which case we push a None onto the to_Egressor
    /// queue and then return Ready(()), which means we enter tear-down, since there
    /// is no further work to complete.
    ///
    /// #4 If our upstream `PacketStream` has a packet for us, we pass it to our `processor`
    /// for `process`ing. Most of the time, it will yield a `Some(output_packet)` that has
    /// been transformed in some way. We pass that on to our egress channel and wake
    /// our `Egressor` that it has work to do, and continue polling our upstream `PacketStream`.
    ///
    /// #5 `processor`s may also choose to "drop" packets by returning `None`, so we do nothing
    /// and poll our upstream `PacketStream` again.
    ///
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        loop {
            if self.to_egressor.is_full() {
                park_and_wake(&self.task_park, cx.waker().clone());
                return Poll::Pending;
            }
            let packet = ready!(Pin::new(&mut self.input_stream).poll_next(cx));
            match packet {
                None => {
                    self.to_egressor.try_send(None).expect(
                        "QueueIngressor::Poll::Ready(None) try_send to_egressor shouldn't fail",
                    );
                    die_and_wake(&self.task_park);
                    return Poll::Ready(());
                }
                Some(packet) => {
                    self.to_egressor
                        .try_send(Some(packet))
                        .expect("QueueIngressor::Poll::Ready(Some(val)) try_send to_egressor shouldn't fail");
                    unpark_and_wake(&self.task_park);
                }
            }
        }
    }
}

/// TODO: Docs fixup
/// The Egressor side of the QueueLink is responsible to converting the
/// output queue of processed packets, which is a crossbeam channel, to a
/// Stream that can be polled for packets. It ends up being owned by the
/// processor which is polling for packets.
pub struct QueueStream<Packet: Sized> {
    from_ingressor: Receiver<Option<Packet>>,
    task_park: Arc<AtomicCell<TaskParkState>>,
}

impl<Packet: Sized> QueueStream<Packet> {
    pub fn new(
        from_ingressor: Receiver<Option<Packet>>,
        task_park: Arc<AtomicCell<TaskParkState>>,
    ) -> Self {
        QueueStream {
            from_ingressor,
            task_park,
        }
    }
}

impl<Packet: Sized> Unpin for QueueStream<Packet> {}

impl<Packet: Sized> Stream for QueueStream<Packet> {
    type Item = Packet;

    /// Implement Poll for Stream for QueueEgressor
    ///
    /// This function, tries to retrieve a packet off the `from_ingressor`
    /// channel, there are four cases:
    /// ###
    /// #1 Ok(Some(Packet)): Got a packet. If the Ingressor needs (likely due to
    /// an until now full channel) to be awoken, wake them. Return the Async::Ready(Option(Packet))
    ///
    /// #2 Ok(None): this means that the Ingressor is in tear-down, and we
    /// will no longer be receivig packets. Return Async::Ready(None) to forward propagate teardown
    ///
    /// #3 Err(TryRecvError::Empty): Packet queue is empty, await the Ingressor to awaken us with more
    /// work, by returning Async::NotReady to signal to runtime to sleep this task.
    ///
    /// #4 Err(TryRecvError::Disconnected): Ingressor is in teardown and has dropped its side of the
    /// from_ingressor channel; we will no longer receive packets. Return Async::Ready(None) to forward
    /// propagate teardown.
    /// ###
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.from_ingressor.try_recv() {
            Ok(Some(packet)) => {
                unpark_and_wake(&self.task_park);
                Poll::Ready(Some(packet))
            }
            Ok(None) => {
                die_and_wake(&self.task_park);
                Poll::Ready(None)
            }
            Err(TryRecvError::Empty) => {
                park_and_wake(&self.task_park, cx.waker().clone());
                Poll::Pending
            }
            Err(TryRecvError::Disconnected) => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::link::Process;
    use crate::utils::test::processor::Identity;
    use crate::utils::test::harness::{initialize_runtime, test_link};
    use crate::utils::test::packet_generators::{immediate_stream, PacketIntervalGenerator};
    use core::time;
    use rand::{thread_rng, Rng};

    #[test]
    fn queue() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link = Queue::new(immediate_stream(packets.clone()), Some(10))
                .into_link();

            test_link(link, None).await
        });
        assert_eq!(results[0], packets);
    }

    #[test]
    fn long_stream() {
        let mut rng = thread_rng();
        let stream_len = rng.gen_range(2000, 4000);

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link =
                Queue::new(immediate_stream(0..stream_len), Some(10)).into_link();

            test_link(link, None).await
        });
        assert_eq!(results[0].len(), stream_len);
    }

    #[test]
    fn small_channel() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link =
                Queue::new(immediate_stream(packets.clone()), Some(1)).into_link();

            test_link(link, None).await
        });
        assert_eq!(results[0], packets);
    }

    #[test]
    fn empty_stream() {
        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let packets: Vec<i32> = vec![];
            let link = Queue::new(immediate_stream(packets.clone()), Some(10))
                .into_link();

            test_link(link, None).await
        });
        assert_eq!(results[0], []);
    }

    #[test]
    fn two_links() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let (mut runnables, mut streams0) = Queue::new(immediate_stream(packets.clone()), Some(10))
                .into_link().take();

            let (mut runnables0, streams1) = Queue::new(streams0.remove(0), None).into_link().take();
            runnables.append(&mut runnables0);

            let link = Link::new(runnables, streams1);
            test_link(link, None).await
        });
        assert_eq!(results[0], packets);
    }

    #[test]
    fn series_of_process_and_queue_links() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let (mut runnables0, mut streams0) = Process::new(immediate_stream(packets.clone()), Identity::new())
                .into_link().take();

            let (mut runnables1, mut streams1) =
                Queue::new(streams0.remove(0), Some(10)).into_link().take();

            let (mut runnables2, mut streams2) =
                Process::new(streams1.remove(0), Identity::new()).into_link().take();

            let (mut runnables3, streams3) =
                Queue::new(streams2.remove(0), Some(10)).into_link().take();
            runnables0.append(&mut runnables1);
            runnables0.append(&mut runnables2);
            runnables0.append(&mut runnables3);

            let link = Link::new(runnables0, streams3);
            test_link(link, None).await
        });
        assert_eq!(results[0], packets);
    }

    #[test]
    fn wait_between_packets() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let input = PacketIntervalGenerator::new(
                time::Duration::from_millis(10),
                packets.clone().into_iter(),
            );

            let link = Queue::new(Box::new(input), None).into_link();
            test_link(link, None).await
        });
        assert_eq!(results[0], packets);
    }
}
