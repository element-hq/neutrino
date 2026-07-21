use alloc::vec::Vec;

use coap_message::{
    Code, MinimalWritableMessage, MutableWritableMessage, OptionNumber,
    ReadableMessage, SeekWritableMessage, WithSortedOptions,
};

use crate::{CoapOption, MessageClass, Packet};

impl Code for MessageClass {
    // Conveniently, it already satisfies the requirements
}

// pub only in name: We don't expose this whole module, so all users will know
// is that this is a suitable iterator.
pub struct MessageOptionAdapter<'a> {
    head: Option<(u16, alloc::collections::linked_list::Iter<'a, Vec<u8>>)>,
    // right from Packet::options -- fortunately that doesn't say that it
    // returns an impl Iterator
    raw_iter: alloc::collections::btree_map::Iter<
        'a,
        u16,
        alloc::collections::linked_list::LinkedList<Vec<u8>>,
    >,
}

// pub only in name: We don't expose this whole module, so all users will know
// is that this implements coap_message::MessageOption
pub struct MessageOption<'a> {
    number: u16,
    value: &'a [u8],
}

impl<'a> Iterator for MessageOptionAdapter<'a> {
    type Item = MessageOption<'a>;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        loop {
            if let Some((number, values)) = self.head.as_mut() {
                if let Some(value) = values.next() {
                    return Some(MessageOption {
                        number: *number,
                        value,
                    });
                }
            }
            let (number, values) = self.raw_iter.next()?;
            self.head = Some((*number, values.iter()));
        }
    }
}

impl coap_message::MessageOption for MessageOption<'_> {
    fn number(&self) -> u16 {
        self.number
    }
    fn value(&self) -> &[u8] {
        self.value
    }
}

impl ReadableMessage for Packet {
    type Code = MessageClass;

    type MessageOption<'a> = MessageOption<'a>;
    type OptionsIter<'a> = MessageOptionAdapter<'a>;

    fn code(&self) -> Self::Code {
        self.header.code
    }
    fn payload(&self) -> &[u8] {
        &self.payload
    }
    fn options(&self) -> Self::OptionsIter<'_> {
        MessageOptionAdapter {
            raw_iter: self.options.iter(),
            head: None,
        }
    }
}

impl WithSortedOptions for Packet {}

impl OptionNumber for CoapOption {}

impl MinimalWritableMessage for Packet {
    type Code = MessageClass;
    type OptionNumber = CoapOption;

    fn set_code(&mut self, code: Self::Code) {
        self.header.code = code;
    }

    fn add_option(&mut self, option: Self::OptionNumber, data: &[u8]) {
        self.add_option(option, data.into());
    }

    fn set_payload(&mut self, payload: &[u8]) {
        self.payload = payload.into();
    }
}

impl MutableWritableMessage for Packet {
    fn available_space(&self) -> usize {
        usize::MAX
    }
    fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.payload
    }
    fn payload_mut_with_len(&mut self, len: usize) -> &mut [u8] {
        self.payload.resize(len, 0);
        &mut self.payload
    }
    fn truncate(&mut self, length: usize) {
        self.payload.truncate(length)
    }
    fn mutate_options<F>(&mut self, mut callback: F)
    where
        F: FnMut(Self::OptionNumber, &mut [u8]),
    {
        for (&number, ref mut values) in self.options.iter_mut() {
            for v in values.iter_mut() {
                callback(number.into(), v);
            }
        }
    }
}

impl SeekWritableMessage for Packet {}
