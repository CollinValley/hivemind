use crate::link::utils::task_park::*;
use crate::{IntoLink, Link, PacketStream, Runnable};
use crossbeam::atomic::AtomicCell;
use crossbeam::crossbeam_channel;
use crossbeam::crossbeam_channel::{Receiver, Sender};
use futures::prelude::*;
use futures::ready;
use futures::task::{Context, Poll};
use std::pin::Pin;
use std::sync::Arc;

pub struct Join<Packet: Send + Sized> {
    runnables: Vec<Runnable>,
    stream: PacketStream<Packet>,
}

impl<Packet: Send + Sized + 'static> Join<Packet> {
    pub fn new(input: Vec<PacketStream<Packet>>, cap: Option<usize>) -> Self {
        assert!(
            !input.is_empty(),
            format!(
                "number of in_streams: {}, must be greater than 0",
                input.len()
            )
        );

        let mut runnables: Vec<Runnable> = Vec::new();
        let mut from_ingressors: Vec<Receiver<Option<Packet>>> = Vec::new();
        let mut task_parks: Vec<Arc<AtomicCell<TaskParkState>>> = Vec::new();
        let num_inputs = input.len();

        for stream in input {
            let (to_egressor, from_ingressor) = match cap {
                None => crossbeam_channel::unbounded::<Option<Packet>>(),
                Some(capacity) => crossbeam_channel::bounded::<Option<Packet>>(capacity),
            };
            let task_park = Arc::new(AtomicCell::new(TaskParkState::Empty));

            let runnable = JoinRunnable::new(stream, to_egressor, Arc::clone(&task_park));
            runnables.push(Box::new(runnable));
            from_ingressors.push(from_ingressor);
            task_parks.push(task_park);
        }

        let stream = JoinStream::new(from_ingressors, task_parks, num_inputs);
        Join {
            runnables: runnables,
            stream: Box::new(stream),
        }
    }
}

impl<Packet: Send + Sized + 'static> IntoLink<Packet> for Join<Packet> {
    fn into_link(self) -> Link<Packet> {
        Link {
            runnables: self.runnables,
            streams: vec![self.stream],
        }
    }
}

pub struct JoinRunnable<Packet: Sized> {
    input: PacketStream<Packet>,
    to_egressor: Sender<Option<Packet>>,
    task_park: Arc<AtomicCell<TaskParkState>>,
}

impl<Packet: Sized> Unpin for JoinRunnable<Packet> {}

impl<Packet: Sized> JoinRunnable<Packet> {
    fn new(
        input: PacketStream<Packet>,
        to_egressor: Sender<Option<Packet>>,
        task_park: Arc<AtomicCell<TaskParkState>>,
    ) -> Self {
        JoinRunnable {
            input,
            to_egressor,
            task_park,
        }
    }
}

impl<Packet: Sized> Future for JoinRunnable<Packet> {
    type Output = ();

    /// Implement Poll for Future for JoinRunnable
    ///
    /// Note that this function works a bit different, it continues to process
    /// packets off it's input queue until it reaches a point where it can not
    /// make forward progress. There are three cases:
    /// ###
    /// #1 The to_egressor queue is full, we wake the egressor that we need
    /// awaking when there is work to do, and go to sleep.
    ///
    /// #2 The input_stream returns a NotReady, we sleep, with the assumption
    /// that whomever produced the NotReady will awaken the task in the Future.
    ///
    /// #3 We get a Ready(None), in which case we push a None onto the to_egressor
    /// queue and then return Ready(()), which means we enter tear-down, since there
    /// is no futher work to complete.
    /// ###
    /// By Sleep, we mean we return a NotReady to the runtime which will sleep the task.
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let ingressor = Pin::into_inner(self);
        loop {
            if ingressor.to_egressor.is_full() {
                park_and_wake(&ingressor.task_park, cx.waker().clone()); //TODO: Change task park to cx based
                return Poll::Pending;
            }
            let input_packet_option: Option<Packet> =
                ready!(Pin::new(&mut ingressor.input).poll_next(cx));

            match input_packet_option {
                None => {
                    ingressor.to_egressor.try_send(None).expect(
                        "JoinIngressor::Poll::Ready(None) try_send to_egressor shouldn't fail",
                    );
                    die_and_wake(&ingressor.task_park);
                    return Poll::Ready(());
                }
                Some(packet) => {
                    ingressor.to_egressor.try_send(Some(packet)).expect(
                        "JoinIngressor::Poll:Ready(Some(Val)) try_send to_egressor shouldn't fail",
                    );
                    unpark_and_wake(&ingressor.task_park);
                }
            }
        }
    }
}

#[allow(dead_code)]
pub struct JoinStream<Packet: Sized> {
    from_ingressors: Vec<Receiver<Option<Packet>>>,
    task_parks: Vec<Arc<AtomicCell<TaskParkState>>>,
    ingressors_alive: usize,
    next_pull_ingressor: usize,
}

impl<Packet: Sized> JoinStream<Packet> {
    fn new(
        from_ingressors: Vec<Receiver<Option<Packet>>>,
        task_parks: Vec<Arc<AtomicCell<TaskParkState>>>,
        ingressors_alive: usize,
    ) -> Self {
        let next_pull_ingressor = 0;
        JoinStream {
            from_ingressors,
            task_parks,
            ingressors_alive,
            next_pull_ingressor,
        }
    }
}

impl<Packet: Sized> Unpin for JoinStream<Packet> {}

impl<Packet: Sized> Drop for JoinStream<Packet> {
    fn drop(&mut self) {}
}

impl<Packet: Sized> Stream for JoinStream<Packet> {
    type Item = Packet;

    /// Iterate over all the channels, pull the first packet that is available.
    /// This starts at the next index after the last successful recv
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        //rotate_slice exists in 1.22 nightly experimental
        let egressor = Pin::into_inner(self);
        let rotated_iter = egressor
            .from_ingressors
            .iter()
            .enumerate()
            .cycle()
            .skip(egressor.next_pull_ingressor)
            .take(egressor.from_ingressors.len());
        for (port, from_ingressor) in rotated_iter {
            match from_ingressor.try_recv() {
                Ok(Some(packet)) => {
                    unpark_and_wake(&egressor.task_parks[port]);
                    egressor.next_pull_ingressor = port + 1;
                    return Poll::Ready(Some(packet));
                }
                Ok(None) => {
                    //Got a none from a consumer that has shutdown
                    egressor.ingressors_alive -= 1;
                    if egressor.ingressors_alive == 0 {
                        for task_park in egressor.task_parks.iter() {
                            die_and_wake(&task_park);
                        }
                        return Poll::Ready(None);
                    }
                }
                Err(_) => {
                    //On an error go to next channel.
                }
            }
        }

        // We could not get a packet from any of our ingressors, this means we will park our task in a
        // common location, and then hand out Arcs to all the ingressors to the common location. The first
        // one to access the egressor task will awaken us, so we can continue providing packets.
        let mut parked_egressor_task = false;
        let egressor_task = Arc::new(AtomicCell::new(Some(cx.waker().clone())));
        for task_park in egressor.task_parks.iter() {
            if indirect_park_and_wake(&task_park, Arc::clone(&egressor_task)) {
                parked_egressor_task = true;
            }
        }
        //we were unable to park task, so we must self wake, presumably all the ingressors are dead.
        if !parked_egressor_task {
            cx.waker().clone().wake();
        }
        Poll::Pending
    }
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use super::*;
    use crate::utils::test::packet_generators::{immediate_stream, PacketIntervalGenerator};
    use core::time;
    use rand::{thread_rng, Rng};

    use crate::utils::test::harness::{initialize_runtime, test_link};

    #[test]
    fn join() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9, 11];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let inputs = vec![
                immediate_stream(packets.clone()),
                immediate_stream(packets.clone()),
            ];
            let link = Join::new(inputs, None).into_link();

            test_link(link, None).await
        });
        assert_eq!(results[0].len(), packets.len() * 2);
    }

    #[test]
    fn multiple_ingressor_calls_works() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9, 11];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let inputs = vec![
                immediate_stream(packets.clone()),
                immediate_stream(packets.clone()),
            ];
            let link = Join::new(inputs, None).into_link();

            test_link(link, None).await
        });
        assert_eq!(results[0].len(), packets.len() * 2);
    }

    #[test]
    fn several_long_streams() {
        let mut rng = thread_rng();
        let stream_len = rng.gen_range(2000, 3000);
        let num_streams = rng.gen_range(5, 10);

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async move {
            let mut inputs: Vec<PacketStream<usize>> = Vec::new();
            for _ in 0..num_streams {
                inputs.push(immediate_stream(0..stream_len));
            }

            let link = Join::new(inputs, None).into_link();

            test_link(link, None).await
        });
        assert_eq!(results[0].len(), stream_len * num_streams);
    }

    #[test]
    fn wait_between_packets() {
        let mut runtime = initialize_runtime();
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9, 11];
        let results = runtime.block_on(async {
            let packet_generator0 = PacketIntervalGenerator::new(
                time::Duration::from_millis(10),
                packets.clone().into_iter(),
            );
            let packet_generator1 = PacketIntervalGenerator::new(
                time::Duration::from_millis(10),
                packets.clone().into_iter(),
            );

            let inputs: Vec<PacketStream<i32>> =
                vec![Box::new(packet_generator0), Box::new(packet_generator1)];

            let link = Join::new(inputs, None).into_link();

            test_link(link, None).await
        });
        assert_eq!(results[0].len(), packets.len() * 2);
    }

    #[test]
    fn small_channel() {
        let mut runtime = initialize_runtime();
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9, 11];
        let results = runtime.block_on(async {
            let inputs = vec![
                immediate_stream::<Vec<i32>>(packets.clone()),
                immediate_stream::<Vec<i32>>(packets.clone()),
            ];

            let link = Join::new(inputs, None).into_link();

            test_link(link, None).await
        });
        assert_eq!(results[0].len(), packets.len() * 2);
    }

    #[test]
    fn empty_stream() {
        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let inputs = vec![
                immediate_stream::<Vec<i32>>(vec![]),
                immediate_stream::<Vec<i32>>(vec![]),
            ];

            let link = Join::new(inputs, None).into_link();

            test_link(link, None).await
        });
        assert_eq!(results[0], []);
    }
}
