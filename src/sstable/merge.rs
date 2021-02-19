use crate::db::error::Result;
use crate::db::slice::Slice;
use crate::db::Iterator;
use crate::util::cmp::Comparator;
use std::cmp::Ordering;
use std::rc::Rc;

const FORWARD: u8 = 0;
const BACKWARD: u8 = 1;

pub(crate) struct MergingIterator<C: Comparator<Slice>> {
    children: Vec<Box<dyn Iterator>>,
    current: Option<usize>,
    comparator: C,
    direction: u8,
}

impl<C: Comparator<Slice>> MergingIterator<C> {
    pub fn new(comparator: C, children: Vec<Box<dyn Iterator>>) -> Self {
        MergingIterator {
            children,
            current: None,
            comparator,
            direction: FORWARD,
        }
    }

    fn find_smallest(&mut self) {
        let mut smallest: Option<usize> = None;
        self.children.iter().enumerate().for_each(|(pos, child)| {
            if child.valid() {
                if let Some(ref small) = smallest {
                    if self
                        .comparator
                        .compare(&child.key(), &(self.children[*small].key()))
                        == Ordering::Less
                    {
                        smallest = Some(pos);
                    }
                } else {
                    smallest = Some(pos);
                }
            }
        });
        self.current = smallest
    }

    fn find_largest(&mut self) {
        let mut largest: Option<usize> = None;
        self.children.iter().enumerate().for_each(|(pos, child)| {
            if child.valid() {
                if let Some(ref large) = largest {
                    if self
                        .comparator
                        .compare(&child.key(), &(self.children[*large].key()))
                        == Ordering::Greater
                    {
                        largest = Some(pos);
                    }
                } else {
                    largest = Some(pos);
                }
            }
        });
        self.current = largest;
    }
}

impl<C: Comparator<Slice>> Iterator for MergingIterator<C> {
    fn valid(&self) -> bool {
        self.current.is_some()
    }

    fn seek_to_first(&mut self) {
        self.children
            .iter_mut()
            .for_each(|child| child.seek_to_first());
        self.find_smallest();
        self.direction = FORWARD;
    }

    fn seek_to_last(&mut self) {
        self.children
            .iter_mut()
            .for_each(|child| child.seek_to_last());
        self.find_smallest();
        self.direction = BACKWARD;
    }

    fn seek(&mut self, target: Slice) {
        self.children
            .iter_mut()
            .for_each(|child| child.seek(target));
        self.find_smallest();
        self.direction = FORWARD;
    }

    fn next(&mut self) {
        assert!(self.valid());

        // Ensure that all children are positioned after key().
        // If we are moving in the forward direction, it is already
        // true for all of the non-current_ children since current_ is
        // the smallest child and key() == current->key().  Otherwise,
        // we explicitly position the non-current_ children.
        let current = self.current.as_ref().unwrap();
        let current_key = self.key();
        if self.direction == BACKWARD {
            for (pos, child) in self.children.iter_mut().enumerate() {
                if pos != *current {
                    child.seek(current_key);
                    if self.comparator.compare(&current_key, &child.key()) == Ordering::Equal {
                        child.next();
                    }
                }
            }
            self.direction = FORWARD;
        }
        self.children[*current].next();
        self.find_smallest();
    }

    fn prev(&mut self) {
        // Ensure that all children are positioned before key().
        // If we are moving in the reverse direction, it is already
        // true for all of the non-current_ children since current_ is
        // the largest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        assert!(self.valid());
        let current = self.current.as_ref().unwrap();
        let current_key = self.key();
        if self.direction == FORWARD {
            self.children
                .iter_mut()
                .enumerate()
                .for_each(|(pos, child)| {
                    if pos != *current {
                        child.seek(current_key);
                        if child.valid() {
                            child.prev();
                        } else {
                            child.seek_to_last();
                        }
                    }
                });
            self.direction = BACKWARD;
        }
        self.children[*current].prev();
        self.find_largest();
    }

    fn key(&self) -> Slice {
        assert!(self.valid());
        let current = self.current.as_ref().unwrap();
        self.children[*current].key()
    }

    fn value(&self) -> Slice {
        assert!(self.valid());
        let current = self.current.as_ref().unwrap();
        self.children[*current].value()
    }

    fn status(&mut self) -> Result<()> {
        for child in self.children.iter_mut() {
            child.status()?;
        }

        Ok(())
    }
}
