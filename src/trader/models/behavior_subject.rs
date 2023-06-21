use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

pub struct BehaviorSubject<T: Clone> {
    value: Arc<Mutex<T>>,
    subscribers: Arc<Mutex<Vec<Arc<Mutex<dyn FnMut(T) + Send + 'static>>>>>,
}

impl<T: Clone> BehaviorSubject<T> {
    pub fn new(value: T) -> Self {
        BehaviorSubject {
            value: Arc::new(Mutex::new(value)),
            subscribers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn subscribe<F>(&self, callback: F)
    where
        F: FnMut(T) + Send + 'static,
    {
        let subscriber = Arc::new(Mutex::new(callback));
        self.subscribers.lock().unwrap().push(subscriber.clone());
        let value = self.value.lock().unwrap().clone();
        let mut callback = subscriber.lock().unwrap();
        callback(value);
    }

    pub fn next(&self, value: T) {
        *self.value.lock().unwrap() = value;
        let subscribers = self.subscribers.lock().unwrap();
        for subscriber in subscribers.iter() {
            let value = self.value.lock().unwrap().clone();
            let mut callback = subscriber.lock().unwrap();
            callback(value);
        }
    }

    pub fn value(&self) -> T {
        self.value.lock().unwrap().clone()
    }
}

struct SubscribeFuture<T: Clone> {
    behavior_subject: Arc<BehaviorSubject<T>>,
    completed: bool,
}

impl<T: Clone> Future for SubscribeFuture<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.completed {
            Poll::Pending
        } else {
            self.completed = true;
            Poll::Ready((*self.behavior_subject.value.lock().unwrap()).clone())
        }
    }
}

// fn subscribe<T: Clone>(behavior_subject: Arc<BehaviorSubject<T>>) -> SubscribeFuture<T> {
//     SubscribeFuture {
//         behavior_subject,
//         completed: false,
//     }
// }

#[tokio::test]
async fn test() {
    let behavior_subject = Arc::new(BehaviorSubject::new(0));

    // let initial_value = subscribe(behavior_subject.clone()).await;

    // Print the initial value
    // println!("Initial value: {:?}", initial_value);

    // Subscribe to the behavior subject
    behavior_subject.subscribe(|value| {
        println!("Received value: {:?}", value);
    });

    // Update the behavior subject's value
    behavior_subject.next(42);
}
