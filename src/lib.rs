
pub mod link;

pub mod utils;


pub trait Processor {
    type Input: Send + Clone;
    type Output: Send + Clone;

    fn process(&mut self, packet: Self::Input) -> Option<Self::Output>;
}

pub trait Classifier {
    type Packet: Send + Clone + 'static;
    type Class: Sized;

    fn classify(&self, packet: &Self::Packet) -> Self::Class;
}

pub type Dispatcher<'a, Class> = dyn Fn(Class) -> Option<usize> + Send + Sync + 'a;

pub type PacketStream<T> = Box<dyn futures::Stream<Item = T> + Send + Unpin>;

pub type Runnable = Box<dyn futures::Future<Output = ()> + Send + Unpin>;

pub trait IntoLink<Output: Send> {
    fn into_link(self) -> Link<Output>;
}

use crate::link::{Classify, Fork, Join, Process, Queue};

pub struct Link<Packet: Send + Sized> {
    runnables: Vec<Runnable>,
    streams: Vec<PacketStream<Packet>>,
}

#[allow(dead_code)]
impl<Packet: Send + Sized + Clone + 'static> Link<Packet> {
    pub fn new(runnables: Vec<Runnable>, streams: Vec<PacketStream<Packet>>) -> Self {
        Link { runnables, streams }
    }

    pub fn take(self) -> (Vec<Runnable>, Vec<PacketStream<Packet>>) {
        (self.runnables, self.streams)
    }

    //Insert a queue link with a specific cap.
    pub fn queue(mut self, cap: Option<usize>) -> Self {
        assert!(
            self.streams.len() == 1,
            "Link has more than one stream, needs to be joined."
        );
        let (mut runnables, streams) = Queue::new(self.streams.remove(0), cap).into_link().take();
        self.runnables.append(&mut runnables);
        Link::new(self.runnables, streams)
    }

    //Combines all streams into one stream
    pub fn join(mut self, cap: Option<usize>) -> Self {
        let (mut runnables, streams) = Join::new(self.streams, cap).into_link().take();
        self.runnables.append(&mut runnables);
        Link::new(self.runnables, streams)
    }

    //Split stream to n copies of each stream
    pub fn fork(mut self, count: usize, cap: Option<usize>) -> Self {
        assert!(
            self.streams.len() == 1,
            "Link has more than one stream, needs to be joined."
        );
        let (mut runnables, streams) =
            Fork::new(self.streams.remove(0), count, cap).into_link().take();
        self.runnables.append(&mut runnables);
        Link::new(self.runnables, streams)
    }
}

trait ProcessFn<P: Processor + Send + 'static> {
    fn process(self, processor: P) -> Link<P::Output>;
}

impl<P: Processor + Send + 'static> ProcessFn<P> for Link<P::Input> {
    fn process(mut self, p: P) -> Link<P::Output> {
        assert!(
            self.streams.len() == 1,
            "Link has more than one stream, needs to be joined."
        );
        //Basically declare a process link here, use our current stream as input, and
        // then convert it to new stream with a Process link, use this to create a new
        // PrimativeLink we return. Carry forward any runnables.
        // Is error to try to call on PrimativeLink with more than one stream? Maybe, or
        // we could just run the process on all the streams.
        let (mut runnables, streams) = Process::new(self.streams.remove(0), p).into_link().take();
        self.runnables.append(&mut runnables);
        Link::new(self.runnables, streams)
    }
}

trait ClassifyFn<C: Classifier + Send + 'static> {
    fn classify(
        self,
        classifier: C,
        dispatcher: Box<Dispatcher<'static, C::Class>>,
        num_streams: usize,
        cap: Option<usize>,
    ) -> Link<C::Packet>;
}

impl<C: Classifier + Send + 'static> ClassifyFn<C> for Link<C::Packet> {
    fn classify(
        mut self,
        classifier: C,
        dispatcher: Box<Dispatcher<'static, C::Class>>,
        num_streams: usize,
        cap: Option<usize>,
    ) -> Link<C::Packet> {
        assert!(
            self.streams.len() == 1,
            "Link has more than one stream, needs to be unloaded."
        );
        let (mut runnables0, streams) = Classify::new(
            self.streams.remove(0),
            classifier,
            dispatcher,
            num_streams,
            cap,
        ).into_link().take();
        self.runnables.append(&mut runnables0);
        Link::new(self.runnables, streams)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test::{harness::{initialize_runtime, test_link},
                           processor::Identity,
                           packet_generators::immediate_stream,
                           classifier::Even};

    #[test]
    fn smoke_router() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9 , 10];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link =
                Link::new(vec![], vec![immediate_stream(packets.clone())])
                    .process(Identity::new())
                    .queue(Some(1))
                    .fork(3, Some(10))
                    .join(None)
                    .classify(
                        Even::new(),
                        Box::new(|is_even| if is_even { Some(0) } else { Some(1) }),
                        2,
                        None
                    );
            test_link(link, None).await
        });
        assert_eq!(results[0].len(), 21);
        assert_eq!(results[1].len(), 18);
    }

    #[test]
    fn fork_chain() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link =
                Link::new(vec![], vec![immediate_stream(packets.clone())])
                    .fork(3, Some(10));
            test_link(link, None).await
        });
        assert_eq!(results[0], packets);
        assert_eq!(results[1], packets);
        assert_eq!(results[2], packets);
    }

    #[test]
    fn join_chain() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link =
                Link::new(vec![], vec![immediate_stream(packets.clone())])
                    .fork(3, Some(10))
                    .join(None);
            test_link(link, None).await
        });
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].len(), packets.len()* 3);
    }

    #[test]
    fn classify_chain() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link =
                Link::new(vec![], vec![immediate_stream(packets.clone())])
                    .classify(
                        Even::new(),
                        Box::new(|is_even| if is_even { Some(0) } else { Some(1) }),
                        2,
                        None
                    );
            test_link(link, None).await
        });
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], vec![0, 2, 420, 4, 6, 8]);
        assert_eq!(results[1], vec![1, 1337, 3, 5, 7, 9]);
    }
}
