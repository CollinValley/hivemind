use crate::{PacketStream, Processor};
use futures::prelude::*;
use futures::ready;
use futures::task::{Context, Poll};
use std::pin::Pin;

pub(crate) struct ProcessStream<P: Processor> {
    in_stream: PacketStream<P::Input>,
    processor: P,
}

impl<P: Processor> ProcessStream<P> {
    pub fn new(in_stream: PacketStream<P::Input>, processor: P) -> Self {
        ProcessStream {
            in_stream,
            processor,
        }
    }
}

impl<P: Processor> Unpin for ProcessStream<P> {}

impl<P: Processor> Stream for ProcessStream<P> {
    type Item = P::Output;

    // Intro to `Stream`s:
    // 3 cases: `Poll::Ready(Some)`, `Poll::Ready(None)`, `Poll::Pending`
    //
    // `Poll::Ready(Some)`: We have a packet ready to process from the upstream processor.
    // It's passed to our core's process function for... processing
    //
    // `Poll::Ready(None)`: The input_stream doesn't have anymore input. Semantically,
    // it's like an iterator has exhausted it's input. We should return `Poll::Ready(None)`
    // to signify to our downstream components that there's no more input to process.
    // Our Processors should rarely return `Poll::Ready(None)` since it will effectively
    // kill the Stream chain.
    //
    // `Poll::Pending`: There is more input for us to process, but we can't make any more
    // progress right now. The contract for Streams asks us to register with a Reactor so we
    // will be woken up again by an Executor, but we will be relying on Tokio to do that for us.
    // This case is handled by the `try_ready!` macro, which will automatically return
    // `Ok(Async::NotReady)` if the input stream gives us NotReady.
    //
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            match ready!(Pin::new(&mut self.in_stream).poll_next(cx)) {
                None => return Poll::Ready(None),
                Some(input_packet) => {
                    // if `processor.process` returns None, do nothing, loop around and try polling again.
                    if let Some(output_packet) = self.processor.process(input_packet) {
                        return Poll::Ready(Some(output_packet));
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
    use crate::utils::test::packet_generators::{immediate_stream, PacketIntervalGenerator};
    use crate::utils::test::processor::{Drop, Identity, TransformFrom};
    use crate::Link;
    use core::time;

    #[test]
    fn process() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            // Gnarly declaration becuase ProcessStream is just a stream, without
            // runnables, but still needs to build a link. User would just call
            // my_link.process(my_processor);
            // These tests are designed to only cover ProcessStream
            let link = Link::new(
                vec![],
                vec![Box::new(ProcessStream::new(
                    immediate_stream(packets.clone()),
                    Identity::new(),
                ))],
            );

            test_link(link, None).await
        });
        assert_eq!(results[0], packets);
    }

    #[test]
    fn wait_between_packets() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let packet_generator = PacketIntervalGenerator::new(
                time::Duration::from_millis(10),
                packets.clone().into_iter(),
            );

            let link = Link::new(
                vec![],
                vec![Box::new(ProcessStream::new(
                    Box::new(packet_generator),
                    Identity::new(),
                ))],
            );

            test_link(link, None).await
        });
        assert_eq!(results[0], packets);
    }

    #[test]
    fn type_transform() {
        let packets = "route-rs".chars();

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let packet_generator = immediate_stream(packets.clone());

            let link = Link::new(
                vec![],
                vec![Box::new(ProcessStream::new(
                    Box::new(packet_generator),
                    TransformFrom::<char, u32>::new(),
                ))],
            );

            test_link(link, None).await
        });
        let expected_output: Vec<u32> = packets.map(|p| p.into()).collect();
        assert_eq!(results[0], expected_output);
    }

    #[test]
    fn drop() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link = Link::new(
                vec![],
                vec![Box::new(ProcessStream::new(
                    immediate_stream(packets),
                    Drop::new(),
                ))],
            );

            test_link(link, None).await
        });
        assert_eq!(results[0], []);
    }
}
