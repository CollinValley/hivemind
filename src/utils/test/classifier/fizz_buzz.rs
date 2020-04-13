use crate::Classifier;
use crate::{link::DoClassify, Link, PacketStream};

pub enum FizzBuzzVariant {
    FizzBuzz,
    Fizz,
    Buzz,
    None,
}

#[derive(Default)]
pub struct FizzBuzz {}

impl FizzBuzz {
    pub fn new() -> Self {
        FizzBuzz {}
    }
}

impl Classifier for FizzBuzz {
    type Packet = i32;

    fn classify(&mut self, packet: &Self::Packet) -> Option<usize> {
        if packet % 3 == 0 && packet % 5 == 0 {
            Some(0)
        } else if packet % 3 == 0 {
            Some(1)
        } else if packet % 5 == 0 {
            Some(2)
        } else {
            Some(3)
        }
    }

    fn num_ports(&mut self) -> usize { 4 }
}

pub fn fizz_buzz_link(stream: PacketStream<i32>) -> Link<i32> {
    DoClassify::do_classify(stream, FizzBuzz::new(), None)
}
