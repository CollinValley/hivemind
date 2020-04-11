use crate::Classifier;
use crate::{link::DoClassify, Link, PacketStream};

#[derive(Default, Clone)]
pub struct Even {}

impl Even {
    pub fn new() -> Self {
        Even {}
    }
}

impl Classifier for Even {
    type Packet = i32;
    const NUM_PORTS: usize = 2;

    fn classify(&self, packet: &Self::Packet) -> Option<usize> {
        //hilarious.  I'm too lazy to change this out to a usize packet type
        match packet % 2 {
            0 => Some(0),
            1 => Some(1),
            _ => None,
        }
    }
}

pub fn even_link(stream: PacketStream<i32>) -> Link<i32> {
    DoClassify::do_classify(stream, Even::new(), None)
}
