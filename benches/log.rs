#![feature(test)]

extern crate test;

use test::{black_box, Bencher};

#[bench]
fn a(_: &mut Bencher) {}
