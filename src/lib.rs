pub mod link;
pub mod utils;
use crate::link::{Classify, Join, Process, Queue};

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

pub struct Link<Packet: Send + Sized> {
    runnables: Vec<Runnable>,
    streams: Vec<PacketStream<Packet>>,
}

#[allow(dead_code)]
impl<Packet: Send + Sized + Clone + 'static> Link<Packet> {
    // Create a new Link
    pub fn new(runnables: Vec<Runnable>, streams: Vec<PacketStream<Packet>>) -> Self {
        Link { runnables, streams }
    }

    // Destructure Link, returning tuple of Runnables and Streams, which could
    // be manually remade into new Links
    pub fn take(self) -> (Vec<Runnable>, Vec<PacketStream<Packet>>) {
        (self.runnables, self.streams)
    }

    // Append a queue of size `cap` to each stream.
    pub fn queue(mut self, cap: Option<usize>) -> Self {
        let mut runnables = vec![];
        let mut streams = vec![];
        for stream in self.streams {
            let (mut q_runnables, mut q_streams) = Queue::new(stream, cap).into_link().take();
            runnables.append(&mut q_runnables);
            streams.append(&mut q_streams);
        }
        self.runnables.append(&mut runnables);
        Link::new(self.runnables, streams)
    }

    // Join all streams in a link into one stream.
    pub fn zip(mut self, cap: Option<usize>) -> Self {
        let (mut runnables, streams) = Join::new(self.streams, cap).into_link().take();
        self.runnables.append(&mut runnables);
        Link::new(self.runnables, streams)
    }

    // Create n copies of each stream, yeilding num_streams * n total streams after
    pub fn fork(mut self, count: usize, cap: Option<usize>) -> Self {
        let mut runnables = vec![];
        let mut streams = vec![];
        for stream in self.streams {
            let (mut f_runnables, mut f_streams) = Link::do_fork(stream, count, cap).take();
            runnables.append(&mut f_runnables);
            streams.append(&mut f_streams);
        }
        self.runnables.append(&mut runnables);
        Link::new(self.runnables, streams)
    }

    // Split link with n streams into n links with 1 stream
    pub fn split(mut self) -> Vec<Self> {
        let mut links = vec![];
        // First Link will carry forward all the runnables
        links.push(Link::new(self.runnables, vec![self.streams.remove(0)]));
        for stream in self.streams {
            links.push(Link::new(vec![], vec![stream]));
        }
        links
    }
}

trait ProcessFn<P: Processor + Clone + Send + 'static> {
    fn process(self, processor: P) -> Link<P::Output>;
}

impl<P: Processor + Send + Clone + 'static> ProcessFn<P> for Link<P::Input> {
    // Append process to each stream in link
    fn process(mut self, p: P) -> Link<P::Output> {
        let mut runnables = vec![];
        let mut streams = vec![];
        for stream in self.streams {
            let (mut p_runnables, mut p_streams) =
                Process::new(stream, p.clone()).into_link().take();
            runnables.append(&mut p_runnables);
            streams.append(&mut p_streams);
        }
        self.runnables.append(&mut runnables);
        Link::new(self.runnables, streams)
    }
}

trait ClassifyFn<C: Classifier + Send + Clone + 'static> {
    fn classify(
        self,
        classifier: C,
        dispatcher: Box<Dispatcher<'static, C::Class>>,
        num_streams: usize,
        cap: Option<usize>,
    ) -> Link<C::Packet>;
}

impl<C: Classifier + Clone + Send + 'static> ClassifyFn<C> for Link<C::Packet> {
    // This doesn't really fully work in parallel...weird interleavedness.
    // Let's fully rework this.
    // Remove dispatcher, class can just return portnum
    // Make class also have a num_outputs() internal function
    // If there are n possible classifications and m streams, return n Links each containing
    // m streams.
    fn classify(
        mut self,
        classifier: C,
        dispatcher: Box<Dispatcher<'static, C::Class>>,
        num_streams: usize,
        cap: Option<usize>,
    ) -> Link<C::Packet> {
        let (mut c_runnables, c_streams) = Classify::new(
            self.streams.remove(0),
            classifier,
            dispatcher,
            num_streams,
            cap,
        )
        .into_link()
        .take();
        self.runnables.append(&mut c_runnables);
        Link::new(self.runnables, c_streams)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test::{
        classifier::Even,
        harness::{initialize_runtime, test_link},
        packet_generators::immediate_stream,
        processor::Identity,
    };

    #[test]
    fn smoke_router() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9, 10];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link = Link::new(vec![], vec![immediate_stream(packets.clone())])
                .process(Identity::new())
                .queue(Some(1))
                .fork(3, Some(10))
                .zip(None)
                .classify(
                    Even::new(),
                    Box::new(|is_even| if is_even { Some(0) } else { Some(1) }),
                    2,
                    None,
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
            let link = Link::new(vec![], vec![immediate_stream(packets.clone())]).fork(3, Some(10));
            test_link(link, None).await
        });
        assert_eq!(results[0], packets);
        assert_eq!(results[1], packets);
        assert_eq!(results[2], packets);
    }

    #[test]
    fn zip_chain() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link = Link::new(vec![], vec![immediate_stream(packets.clone())])
                .fork(3, Some(10))
                .zip(None);
            test_link(link, None).await
        });
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].len(), packets.len() * 3);
    }

    #[test]
    fn classify_chain() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link = Link::new(vec![], vec![immediate_stream(packets.clone())]).classify(
                Even::new(),
                Box::new(|is_even| if is_even { Some(0) } else { Some(1) }),
                2,
                None,
            );
            test_link(link, None).await
        });
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], vec![0, 2, 420, 4, 6, 8]);
        assert_eq!(results[1], vec![1, 1337, 3, 5, 7, 9]);
    }
}
