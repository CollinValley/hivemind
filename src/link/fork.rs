use crate::link::utils::task_park::*;
use crate::{link::QueueStream, HStream, Link};
use crossbeam::atomic::AtomicCell;
use crossbeam::crossbeam_channel;
use crossbeam::crossbeam_channel::{Receiver, Sender};
use futures::prelude::*;
use futures::ready;
use futures::task::{Context, Poll};
use std::pin::Pin;
use std::sync::Arc;

impl<Packet: Clone + Send + 'static> Link<Packet> {
    pub(crate) fn do_fork(input: HStream<Packet>, count: usize, capacity: Option<usize>) -> Self {
        let mut to_egressors: Vec<Sender<Option<Packet>>> = Vec::new();
        let mut egressors: Vec<HStream<Packet>> = Vec::new();

        let mut from_ingressors: Vec<Receiver<Option<Packet>>> = Vec::new();

        let mut task_parks: Vec<Arc<AtomicCell<TaskParkState>>> = Vec::new();

        for _ in 0..count {
            let (to_egressor, from_ingressor) = match capacity {
                None => crossbeam_channel::unbounded::<Option<Packet>>(),
                Some(capacity) => crossbeam_channel::bounded::<Option<Packet>>(capacity),
            };
            let task_park = Arc::new(AtomicCell::new(TaskParkState::Empty));

            let egressor = QueueStream::new(from_ingressor.clone(), Arc::clone(&task_park));

            to_egressors.push(to_egressor);
            egressors.push(Box::new(egressor));
            from_ingressors.push(from_ingressor);
            task_parks.push(task_park);
        }

        let ingressor = ForkRunnable::new(input, to_egressors, task_parks);

        Link {
            runnables: vec![Box::new(ingressor)],
            streams: egressors,
        }
    }
}

pub struct ForkRunnable<P> {
    input_stream: HStream<P>,
    to_egressors: Vec<Sender<Option<P>>>,
    task_parks: Vec<Arc<AtomicCell<TaskParkState>>>,
}

impl<P> ForkRunnable<P> {
    fn new(
        input_stream: HStream<P>,
        to_egressors: Vec<Sender<Option<P>>>,
        task_parks: Vec<Arc<AtomicCell<TaskParkState>>>,
    ) -> Self {
        ForkRunnable {
            input_stream,
            to_egressors,
            task_parks,
        }
    }
}

impl<P: Send + Clone> Future for ForkRunnable<P> {
    type Output = ();

    // If any of the channels are full, we await that channel to clear before processing a new packet.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        loop {
            for (port, to_egressor) in self.to_egressors.iter().enumerate() {
                if to_egressor.is_full() {
                    park_and_wake(&self.task_parks[port], cx.waker().clone());
                    return Poll::Pending;
                }
            }
            let packet_option: Option<P> = ready!(Pin::new(&mut self.input_stream).poll_next(cx));

            match packet_option {
                None => {
                    for to_egressor in self.to_egressors.iter() {
                        if let Err(err) = to_egressor.try_send(None) {
                            panic!("Ingressor: Drop: try_send to egressor, fail?: {:?}", err);
                        }
                    }
                    for task_park in self.task_parks.iter() {
                        die_and_wake(&task_park);
                    }
                    return Poll::Ready(());
                }
                Some(packet) => {
                    assert!(self.to_egressors.len() == self.task_parks.len());
                    for port in 0..self.to_egressors.len() {
                        if let Err(err) = self.to_egressors[port].try_send(Some(packet.clone())) {
                            panic!(
                                "Error in to_egressors[{}] sender, have nowhere to put packet: {:?}",
                                port, err
                            );
                        }
                        unpark_and_wake(&self.task_parks[port]);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test::harness::{initialize_runtime, test_link};
    use crate::utils::test::packet_generators::immediate_stream;

    #[test]
    fn no_input() {
        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link = Link::<i32>::do_fork(immediate_stream(vec![]), 1, None);

            test_link(link, None).await
        });
        assert!(results[0].is_empty());
    }

    #[test]
    fn one_way() {
        let packets = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link = Link::<i32>::do_fork(immediate_stream(packets.clone()), 1, None);

            test_link(link, None).await
        });
        assert_eq!(results[0], packets);
    }

    #[test]
    fn two_way() {
        let packets = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link = Link::<i32>::do_fork(immediate_stream(packets.clone()), 2, None);

            test_link(link, None).await
        });

        assert_eq!(results[0], packets.clone());
        assert_eq!(results[1], packets);
    }

    #[test]
    fn three_way() {
        let packets = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link = Link::<i32>::do_fork(immediate_stream(packets.clone()), 3, None);

            test_link(link, None).await
        });

        assert_eq!(results[0], packets.clone());
        assert_eq!(results[1], packets.clone());
        assert_eq!(results[2], packets);
    }
}
