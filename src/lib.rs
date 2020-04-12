pub mod link;
pub mod utils;
use crate::link::{DoClassify, ProcessStream};

pub trait Processor {
    type Input: Send;
    type Output: Send;

    fn process(&mut self, packet: Self::Input) -> Option<Self::Output>;
}

pub trait Classifier {
    type Packet: Send + 'static;
    const NUM_PORTS: usize;

    fn classify(&self, packet: &Self::Packet) -> Option<usize>;
}

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
impl<Packet: Send + Sized + 'static> Link<Packet> {
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
            let (mut q_runnables, mut q_streams) = Link::do_queue(stream, cap).take();
            runnables.append(&mut q_runnables);
            streams.append(&mut q_streams);
        }
        self.runnables.append(&mut runnables);
        Link::new(self.runnables, streams)
    }

    // Join all streams in a link into one stream.
    pub fn zip(mut self, cap: Option<usize>) -> Self {
        let (mut runnables, streams) = Link::do_join(self.streams, cap).take();
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

    pub fn join(links: Vec<Self>) -> Self {
        let mut runnables = vec![];
        let mut streams = vec![];
        for link in links {
            let (mut r, mut s) = link.take();
            runnables.append(&mut r);
            streams.append(&mut s);
        }
        Link::new(runnables, streams)
    }
}

impl<Packet: Send + Sized + Clone + 'static> Link<Packet> {
    // Create n stream-level copies of this Link
    pub fn fork(mut self, count: usize, cap: Option<usize>) -> Vec<Self> {
        let mut runnables = vec![];
        let mut output_buckets: Vec<Vec<PacketStream<Packet>>> = vec![];

        for input_stream in self.streams {
            let (mut f_runnables, f_streams) = Link::do_fork(input_stream, count, cap).take();
            runnables.append(&mut f_runnables);
            for (i, f_stream) in f_streams.into_iter().enumerate() {
                match output_buckets.get_mut(i) {
                    None => {
                        output_buckets.push(vec![f_stream]);
                    }
                    Some(bucket) => {
                        bucket.push(f_stream);
                    }
                }
            }
        }
        self.runnables.append(&mut runnables);

        let mut links = vec![];
        for streams in output_buckets {
            let mut runnables = vec![];
            runnables.append(&mut self.runnables);
            links.push(Link::new(runnables, streams));
        }
        links
    }
}

pub(crate) trait ProcessFn<P: Processor + Clone + Send + 'static> {
    fn process(self, processor: P) -> Link<P::Output>;
}

impl<P: Processor + Clone + Send + 'static> ProcessFn<P> for Link<P::Input> {
    // Append process to each stream in link
    fn process(self, p: P) -> Link<P::Output> {
        let mut streams: Vec<PacketStream<P::Output>> = vec![];
        for stream in self.streams {
            // Little weird, still, process doesn't create new runnables, so just
            // manipulate the stream directly.
            let p_stream = ProcessStream::new(stream, p.clone());
            streams.push(Box::new(p_stream));
        }
        Link::new(self.runnables, streams)
    }
}

trait ClassifyFn<C: Classifier + Clone + Send + 'static> {
    fn classify(self, classifier: C, cap: Option<usize>) -> Vec<Link<C::Packet>>;
}

impl<C: Classifier + Clone + Send + 'static> ClassifyFn<C> for Link<C::Packet> {
    fn classify(mut self, classifier: C, cap: Option<usize>) -> Vec<Link<C::Packet>> {
        let mut links = vec![];
        //Take and classify each input stream
        for stream in self.streams {
            let (mut runnables, streams) =
                DoClassify::do_classify(stream, classifier.clone(), cap).take();
            runnables.append(&mut self.runnables);
            links.push(Link::new(runnables, streams));
        }
        links
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
    fn process_1_stream() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9, 10];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link =
                Link::new(vec![], vec![immediate_stream(packets.clone())]).process(Identity::new());
            test_link(link, None).await
        });
        assert_eq!(results[0], packets);
    }

    #[test]
    fn process_3_streams() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9, 10];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link = Link::new(
                vec![],
                vec![
                    immediate_stream(packets.clone()),
                    immediate_stream(packets.clone()),
                    immediate_stream(packets.clone()),
                ],
            )
            .process(Identity::new());

            test_link(link, None).await
        });
        assert_eq!(results[0], packets);
        assert_eq!(results[1], packets);
        assert_eq!(results[2], packets);
    }

    #[test]
    fn queue_1_stream() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9, 10];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link =
                Link::new(vec![], vec![immediate_stream(packets.clone())]).queue(Some(10));
            test_link(link, None).await
        });
        assert_eq!(results[0], packets);
    }

    #[test]
    fn queue_3_streams() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9, 10];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link = Link::new(
                vec![],
                vec![
                    immediate_stream(packets.clone()),
                    immediate_stream(packets.clone()),
                    immediate_stream(packets.clone()),
                ],
            )
            .process(Identity::new());

            test_link(link, None).await
        });
        assert_eq!(results[0], packets);
        assert_eq!(results[1], packets);
        assert_eq!(results[2], packets);
    }

    #[test]
    fn join_1_link() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];
        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link = Link::new(vec![], vec![immediate_stream(packets.clone())]);
            test_link(Link::join(vec![link]), None).await
        });
        assert_eq!(results[0], packets);
    }

    #[test]
    fn join_3_links() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];
        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link0 = Link::new(vec![], vec![immediate_stream(packets.clone())]);
            let link1 = Link::new(vec![], vec![immediate_stream(packets.clone())]);
            let link2 = Link::new(vec![], vec![immediate_stream(packets.clone())]);
            let links = vec![link0, link1, link2];
            test_link(Link::join(links), None).await
        });
        assert_eq!(results[0], packets);
        assert_eq!(results[1], packets);
        assert_eq!(results[2], packets);
    }


    #[test]
    // Demonstrate that calling fork(2) on a link containing only one stream produces
    // 2 Links with the same one stream.
    fn fork_1_stream() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let f_links =
                Link::new(vec![], vec![immediate_stream(packets.clone())]).fork(2, Some(10));
            let mut runnables = vec![];
            let mut streams = vec![];
            for link in f_links {
                let (mut r, mut s) = link.take();
                runnables.append(&mut r);
                streams.append(&mut s);
            }
            test_link(Link::new(runnables, streams), None).await
        });
        assert_eq!(results[0], packets);
        assert_eq!(results[1], packets);
    }

    #[test]
    // Demonstrate calling fork(2) on a link containing two streams produces 3 links,
    // each with one of each stream.
    fn fork_2_streams() {
        let packets0: Vec<i32> = vec![0, 1, 2, 420, 1337];
        let packets1: Vec<i32> = vec![6, 7, 8, 9, 10];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let f_links = Link::new(
                vec![],
                vec![
                    immediate_stream(packets0.clone()),
                    immediate_stream(packets1.clone()),
                ],
            )
            .fork(3, Some(10));

            let mut runnables = vec![];
            let mut streams = vec![];
            for link in f_links {
                let (mut r, mut s) = link.take();
                runnables.append(&mut r);
                streams.append(&mut s);
            }
            test_link(Link::new(runnables, streams), None).await
        });
        assert_eq!(results[0], packets0);
        assert_eq!(results[1], packets1);
        assert_eq!(results[2], packets0);
        assert_eq!(results[3], packets1);
    }

    #[test]
    fn zip_1_stream() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link = Link::new(vec![], vec![immediate_stream(packets.clone())]).zip(None);
            test_link(link, None).await
        });
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].len(), packets.len());
    }

    #[test]
    fn zip_3_streams() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let link = Link::new(
                vec![],
                vec![
                    immediate_stream(packets.clone()),
                    immediate_stream(packets.clone()),
                    immediate_stream(packets.clone()),
                ],
            )
            .zip(None);

            test_link(link, None).await
        });
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].len(), packets.len() * 3);
    }

    #[test]
    fn classify_1_stream() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let results = runtime.block_on(async {
            let mut links = Link::new(vec![], vec![immediate_stream(packets.clone())])
                .classify(Even::new(), None);
            test_link(links.remove(0), None).await
        });
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], vec![0, 2, 420, 4, 6, 8]);
        assert_eq!(results[1], vec![1, 1337, 3, 5, 7, 9]);
    }

    #[test]
    fn classify_3_streams() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let (results, num_links) = runtime.block_on(async {
            let links = Link::new(
                vec![],
                vec![
                    immediate_stream(packets.clone()),
                    immediate_stream(packets.clone()),
                    immediate_stream(packets.clone())
                ],
            )
            .classify(Even::new(), None);

            let mut runnables = vec![];
            let mut streams = vec![];
            let num_links = links.len();
            for link in links {
                let (mut r, mut s) = link.take();
                runnables.append(&mut r);
                streams.append(&mut s);
            }
            (test_link(Link::new(runnables, streams), None).await , num_links)
        });
        assert_eq!(num_links, 3);
        assert_eq!(results[0], vec![0, 2, 420, 4, 6, 8]);
        assert_eq!(results[1], vec![1, 1337, 3, 5, 7, 9]);
        assert_eq!(results[2], vec![0, 2, 420, 4, 6, 8]);
        assert_eq!(results[3], vec![1, 1337, 3, 5, 7, 9]);
        assert_eq!(results[4], vec![0, 2, 420, 4, 6, 8]);
        assert_eq!(results[5], vec![1, 1337, 3, 5, 7, 9]);
    }

    #[test]
    // Should correctly do nothing
    fn split_1_stream() {
        let packets: Vec<i32> = vec![0, 1, 2, 420, 1337, 3, 4, 5, 6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let (results, num_links) = runtime.block_on(async {
            let links = Link::new(
                vec![],
                vec![
                    immediate_stream(packets.clone()),
                ],
            )
            .split();

            let mut runnables = vec![];
            let mut streams = vec![];
            let num_links = links.len();
            for link in links {
                let (mut r, mut s) = link.take();
                runnables.append(&mut r);
                streams.append(&mut s);
            }
            (test_link(Link::new(runnables, streams), None).await , num_links)
        });
        assert_eq!(num_links, 1);
        assert_eq!(results[0], packets);
    }

    #[test]
    fn split_3_streams() {
        let packets0: Vec<i32> = vec![0, 1, 2, 420];
        let packets1: Vec<i32> = vec![1337, 3, 4, 5];
        let packets2: Vec<i32> = vec![6, 7, 8, 9];

        let mut runtime = initialize_runtime();
        let (results, num_links) = runtime.block_on(async {
            let links = Link::new(
                vec![],
                vec![
                    immediate_stream(packets0.clone()),
                    immediate_stream(packets1.clone()),
                    immediate_stream(packets2.clone()),
                ],
            )
            .split();

            let mut runnables = vec![];
            let mut streams = vec![];
            let num_links = links.len();
            for link in links {
                let (mut r, mut s) = link.take();
                runnables.append(&mut r);
                streams.append(&mut s);
            }
            (test_link(Link::new(runnables, streams), None).await , num_links)
        });
        assert_eq!(num_links, 3);
        assert_eq!(results[0], packets0);
        assert_eq!(results[1], packets1);
        assert_eq!(results[2], packets2);
    }
}
