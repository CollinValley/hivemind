use crate::Classifier;
use crate::{link::Classify, IntoLink, Link, PacketStream};

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
    type Class = FizzBuzzVariant;

    fn classify(&self, packet: &Self::Packet) -> Self::Class {
        if packet % 3 == 0 && packet % 5 == 0 {
            FizzBuzzVariant::FizzBuzz
        } else if packet % 3 == 0 {
            FizzBuzzVariant::Fizz
        } else if packet % 5 == 0 {
            FizzBuzzVariant::Buzz
        } else {
            FizzBuzzVariant::None
        }
    }
}

pub fn fizz_buzz_link(stream: PacketStream<i32>) -> Link<i32> {
    Classify::new(
        stream,
        FizzBuzz::new(),
        Box::new(|fb| match fb {
            FizzBuzzVariant::FizzBuzz => Some(0),
            FizzBuzzVariant::Fizz => Some(1),
            FizzBuzzVariant::Buzz => Some(2),
            FizzBuzzVariant::None => Some(3),
        }),
        4,
        None,
    )
    .into_link()
}