use crate::Classifier;
use crate::{link::Classify, IntoLink, Link, PacketStream};

#[derive(Default, Clone)]
pub struct Even {}

impl Even {
    pub fn new() -> Self {
        Even {}
    }
}

impl Classifier for Even {
    type Packet = i32;
    type Class = bool;

    fn classify(&self, packet: &Self::Packet) -> Self::Class {
        packet % 2 == 0
    }
}

pub fn even_link(stream: PacketStream<i32>) -> Link<i32> {
    Classify::new(
        stream,
        Even::new(),
        Box::new(|is_even| if is_even { Some(0) } else { Some(1) }),
        2,
        None,
    )
    .into_link()
}
