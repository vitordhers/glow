use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

#[derive(Clone)]
pub struct AsyncBehaviorSubject<T: Clone> {
    value: Arc<Mutex<T>>,
    subscribers: Arc<
        Mutex<Vec<Arc<Mutex<dyn FnMut(T) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>>>>,
    >,
}

impl<T: Clone> AsyncBehaviorSubject<T> {
    pub fn new(value: T) -> Self {
        AsyncBehaviorSubject {
            value: Arc::new(Mutex::new(value)),
            subscribers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn subscribe<F>(&self, callback: F)
    where
        F: FnMut(T) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static,
    {
        let subscriber = Arc::new(Mutex::new(callback));
        self.subscribers.lock().unwrap().push(subscriber.clone());
        let value = self.value.lock().unwrap().clone();
        let mut callback = subscriber.lock().unwrap();
        let future = callback(value);
        future.await;
    }

    pub fn next(&self, value: T) {
        *self.value.lock().unwrap() = value;
        let subscribers = self.subscribers.lock().unwrap();
        for subscriber in subscribers.iter() {
            let value = self.value.lock().unwrap().clone();
            let mut callback = subscriber.lock().unwrap();
            let future = callback(value);
            tokio::spawn(future);
        }
    }

    pub fn value(&self) -> T {
        self.value.lock().unwrap().clone()
    }
}

struct SubscribeFuture<T: Clone> {
    behavior_subject: Arc<AsyncBehaviorSubject<T>>,
    completed: bool,
}

impl<T: Clone> Future for SubscribeFuture<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.completed {
            Poll::Pending
        } else {
            self.completed = true;
            Poll::Ready((*self.behavior_subject.value.lock().unwrap()).clone())
        }
    }
}

// #[tokio::test]
// async fn test() {
//     let subject = AsyncBehaviorSubject::new(0);

//     // Subscribe to the subject asynchronously
//     let subs = subject.subscribe(|value| {
//         Box::pin(async move {
//             println!("Async subscriber: {}", value);
//             sleep(Duration::from_secs(1)).await;
//             println!("Async subscriber completed");
//         })
//     });

//     subs.await;

//     // Update the subject value
//     subject.next(42);

//     // Wait for a moment to allow async subscribers to execute
//     sleep(Duration::from_secs(2)).await;

//     // Update the subject value again
//     subject.next(99);

//     // Wait for a moment to allow async subscribers to execute
//     sleep(Duration::from_secs(2)).await;
// }
