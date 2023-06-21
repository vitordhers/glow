use core::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::num::Wrapping;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::task::{Context, Poll, Waker};

use polars_lazy::prelude::LazyFrame;
use tokio::task::JoinError;

/// Thread-safe observable, intended to be implemented on `&self`
pub trait Observable<'a> {
    type Item;
    /// (item, epoch)
    type Ref: Deref<Target = (Self::Item, usize)> + 'a;

    /// subscriber has to be renewed after the value is updated
    fn renew(self, waker: Waker);
    fn next(self, item: Self::Item);
    fn get_observed(self) -> Self::Ref;
    fn get_epoch(self) -> usize;
}

pub struct ObservableFuture<'a, T: Observable<'a>> {
    observable: T,
    epoch: usize,
    phantom: PhantomData<&'a ()>,
}
impl<'a, T: Clone + Observable<'a>> ObservableFuture<'a, T> {
    fn new(observable: T) -> Self {
        Self {
            observable: observable.clone(),
            epoch: observable.get_epoch(),
            phantom: PhantomData,
        }
    }
}
impl<'a, T: Clone + Observable<'a>> Future for ObservableFuture<'a, T> {
    type Output = T::Ref;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let observable = &self.observable;

        if self.epoch != observable.clone().get_epoch() {
            Poll::Ready(observable.clone().get_observed())
        } else {
            observable.clone().renew(cx.waker().clone());
            Poll::Pending
        }
    }
}

pub fn wait_for_update<'a, T: Clone + Observable<'a>>(observable: T) -> ObservableFuture<'a, T> {
    ObservableFuture::new(observable)
}

// async fn give_number() -> u32 {
//     100
// }

// fn give_number() -> impl Future<Output = u32> {
//     GiveNumberFuture
// }

struct Observed<T> {
    // If T is a primitive, then you can replace RwLock
    // with Atomic.
    // Or if T is `Sync`, then you may remove `RwLock`.
    // However, you still have to think of a way where
    // epoch can be updated atomically with T.
    item: RwLock<(T, usize)>,
    // You can consider replacing Vec with other
    // container with built-in concurrency support.
    // such as crossbeam::queue::SegQueue
    observers: RwLock<Vec<Waker>>,
}



impl<T> Observed<T> {
    fn new(item: T) -> Self {
        Self {
            item: RwLock::new((item, 0)),
            observers: RwLock::new(Vec::new()),
        }
    }
}



impl<'a, T: 'a> Observable<'a> for &'a Observed<T> {
    type Item = T;
    type Ref = RwLockReadGuard<'a, (T, usize)>;

    fn renew(self, waker: Waker) {
        self.observers.write().unwrap().push(waker);
    }

    fn next(self, item: Self::Item) {
        let epoch = Wrapping(self.get_epoch());
        let one = Wrapping(1);
        *self.item.write().unwrap() = (item, (epoch + one).0);
        let observers = std::mem::replace(&mut *self.observers.write().unwrap(), Vec::new());
        for observer in observers {
            observer.wake_by_ref();
        }
    }

    fn get_observed(self) -> Self::Ref {
        self.item.read().unwrap()
    }

    fn get_epoch(self) -> usize {
        self.item.read().unwrap().1
    }
}

#[tokio::test]
async fn test() {
    use tokio::time::{sleep, Duration};

    let observed = Arc::new(Observed::new(LazyFrame::default()));

    let observed_cloned = observed.clone();
    let handle = tokio::spawn(async move {
        let observed = observed_cloned;
        let result = wait_for_update(&*observed).await;
        let payload = &result.0.clone().collect().unwrap();
        println!("{:#?}", payload);
    });

    // let observed_cloned = observed.clone();
    // let handle2 = tokio::spawn(async move {
    //     let observed = observed_cloned;

    //     println!("{:#?}", *wait_for_update(&*observed).await);
    // });

    // sleep for 1s to make sure wait_for_update is executed
    sleep(Duration::from_millis(5000)).await;
    observed.next(LazyFrame::default());

    handle.await.unwrap();
    // handle2.await.unwrap();
}