use crate::link::utils::task_park::*;
use crate::Classifier;
use crate::{link::QueueStream, IntoLink, Link, PacketStream};
use crossbeam::atomic::AtomicCell;
use crossbeam::crossbeam_channel;
use crossbeam::crossbeam_channel::{Receiver, Sender};
use futures::prelude::*;
use futures::ready;
use futures::task::{Context, Poll};
use std::pin::Pin;
use std::sync::Arc;
use tokio::stream::Stream;

pub type Dispatcher<'a, Class> = dyn Fn(Class) -> Option<usize> + Send + Sync + 'a;

pub struct Classify<C: Classifier + 'static> {
    runnable: ClassifyRunnable<'static, C>,
    streams: Vec<PacketStream<C::Packet>>,
}

impl<C: Classifier + 'static> Classify<C> {
    pub fn new(
        input: PacketStream<C::Packet>,
        classifier: C,
        dispatcher: Box<Dispatcher<'static, C::Class>>,
        num_streams: usize,
        cap: Option<usize>,
    ) -> Self {
        let mut senders: Vec<Sender<Option<C::Packet>>> = Vec::new();
        let mut streams: Vec<PacketStream<C::Packet>> = Vec::new();
        let mut receivers: Vec<Receiver<Option<C::Packet>>> = Vec::new();
        let mut task_parks: Vec<Arc<AtomicCell<TaskParkState>>> = Vec::new();

        for _ in 0..num_streams {
            let (sender, receiver) = match cap {
                None => crossbeam_channel::unbounded::<Option<C::Packet>>(),
                Some(capacity) => crossbeam_channel::bounded::<Option<C::Packet>>(capacity),
            };
            let task_park = Arc::new(AtomicCell::new(TaskParkState::Empty));
            let stream = QueueStream::new(receiver.clone(), Arc::clone(&task_park));

            senders.push(sender);
            streams.push(Box::new(stream));
            receivers.push(receiver);
            task_parks.push(task_park);
        }
        let runnable = ClassifyRunnable::new(input, dispatcher, senders, classifier, task_parks);

        Classify { runnable, streams }
    }
}

impl<C: Classifier + Send + 'static> IntoLink<C::Packet> for Classify<C> {
    fn into_link(self) -> Link<C::Packet> {
        Link {
            runnables: vec![Box::new(self.runnable)],
            streams: self.streams,
        }
    }
}

pub struct ClassifyRunnable<'a, C: Classifier> {
    input_stream: PacketStream<C::Packet>,
    dispatcher: Box<Dispatcher<'a, C::Class>>,
    to_egressors: Vec<Sender<Option<C::Packet>>>,
    classifier: C,
    task_parks: Vec<Arc<AtomicCell<TaskParkState>>>,
}

impl<'a, C: Classifier> Unpin for ClassifyRunnable<'a, C> {}

impl<'a, C: Classifier> ClassifyRunnable<'a, C> {
    fn new(
        input_stream: PacketStream<C::Packet>,
        dispatcher: Box<Dispatcher<'a, C::Class>>,
        to_egressors: Vec<Sender<Option<C::Packet>>>,
        classifier: C,
        task_parks: Vec<Arc<AtomicCell<TaskParkState>>>,
    ) -> Self {
        ClassifyRunnable {
            input_stream,
            dispatcher,
            to_egressors,
            classifier,
            task_parks,
        }
    }
}

impl<'a, C: Classifier> Future for ClassifyRunnable<'a, C> {
    type Output = ();

    /// Same logic as QueueEgressor, except if any of the channels are full we
    /// await that channel to clear before processing a new packet. This is somewhat
    /// inefficient, but seems acceptable for now since we want to yield compute to
    /// that egressor, as there is a backup in its queue.
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let ingressor = Pin::into_inner(self);
        loop {
            for (port, to_egressor) in ingressor.to_egressors.iter().enumerate() {
                if to_egressor.is_full() {
                    park_and_wake(&ingressor.task_parks[port], cx.waker().clone());
                    return Poll::Pending;
                }
            }

            //TODO: Standardize in_stream, input_stream, and stream to one name
            let packet_option: Option<C::Packet> =
                ready!(Pin::new(&mut ingressor.input_stream).poll_next(cx));

            match packet_option {
                None => {
                    for to_egressor in ingressor.to_egressors.iter() {
                        to_egressor
                            .try_send(None)
                            .expect("ClassifyIngressor::Drop: try_send to_egressor shouldn't fail");
                    }
                    for task_park in ingressor.task_parks.iter() {
                        die_and_wake(&task_park);
                    }
                    return Poll::Ready(());
                }
                Some(packet) => {
                    let class = ingressor.classifier.classify(&packet);
                    // If we get Some(port) back, send the packet; else we drop it.
                    if let Some(port) = (ingressor.dispatcher)(class) {
                        if port >= ingressor.to_egressors.len() {
                            panic!("Tried to access invalid port: {}", port);
                        }
                        if let Err(err) = ingressor.to_egressors[port].try_send(Some(packet)) {
                            panic!(
                                "Error in to_egressors[{}] sender, have nowhere to put packet: {:?}",
                                port, err
                            );
                        }
                        unpark_and_wake(&ingressor.task_parks[port]);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::test::classifier::{even_link, fizz_buzz_link};
    use crate::utils::test::harness::{initialize_runtime, test_link};
    use crate::utils::test::packet_generators::{immediate_stream, PacketIntervalGenerator};
    use crate::Link;
    use core::time;

    #[test]
    fn even_odd() {
        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let packet_generator = immediate_stream(vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9]);

            test_link(even_link(packet_generator), None).await
        });
        assert_eq!(results[0], vec![0, 2, 420, 4, 6, 8]);
        assert_eq!(results[1], vec![1, 1337, 3, 5, 7, 9]);
    }

    #[test]
    fn even_odd_wait_between_packets() {
        let packets = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let packet_generator =
                PacketIntervalGenerator::new(time::Duration::from_millis(10), packets.into_iter());

            test_link(even_link(Box::new(packet_generator)), None).await
        });
        assert_eq!(results[0], vec![0, 2, 420, 4, 6, 8]);
        assert_eq!(results[1], vec![1, 1337, 3, 5, 7, 9]);
    }

    #[test]
    fn only_odd() {
        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let packet_generator = immediate_stream(vec![1, 1337, 3, 5, 7, 9]);

            test_link(even_link(packet_generator), None).await
        });
        assert_eq!(results[0], []);
        assert_eq!(results[1], vec![1, 1337, 3, 5, 7, 9]);
    }

    #[test]
    fn even_odd_long_stream() {
        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let packet_generator = immediate_stream(0..2000);

            test_link(even_link(packet_generator), None).await
        });
        assert_eq!(results[0].len(), 1000);
        assert_eq!(results[1].len(), 1000);
    }

    #[test]
    fn fizz_buzz() {
        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let packet_generator = immediate_stream(0..=30);
            test_link(fizz_buzz_link(packet_generator), None).await
        });

        let expected_fizz_buzz = vec![0, 15, 30];
        assert_eq!(results[0], expected_fizz_buzz);

        let expected_fizz = vec![3, 6, 9, 12, 18, 21, 24, 27];
        assert_eq!(results[1], expected_fizz);

        let expected_buzz = vec![5, 10, 20, 25];
        assert_eq!(results[2], expected_buzz);

        let expected_other = vec![1, 2, 4, 7, 8, 11, 13, 14, 16, 17, 19, 22, 23, 26, 28, 29];
        assert_eq!(results[3], expected_other);
    }

    #[test]
    fn fizz_buzz_to_even_odd() {
        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let packet_generator = immediate_stream(0..=30);

            let (mut fb_runnables, mut fb_egressors) = fizz_buzz_link(packet_generator).take();

            let (mut eo_runnables, eo_egressors) = even_link(fb_egressors.pop().unwrap()).take();

            fb_runnables.append(&mut eo_runnables);

            test_link(Link::new(fb_runnables, eo_egressors), None).await
        });
        assert_eq!(results[0], vec![2, 4, 8, 14, 16, 22, 26, 28]);
        assert_eq!(results[1], vec![1, 7, 11, 13, 17, 19, 23, 29]);
    }
}
