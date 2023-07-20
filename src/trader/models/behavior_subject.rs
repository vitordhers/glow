use futures_util::Stream;
use futures_util::StreamExt;
use std::{sync::Arc, time::Duration};
use tokio::sync::watch::{channel, Receiver, Sender};
use tokio::time::sleep;
use tokio_stream::wrappers::WatchStream;

#[derive(Clone)]
pub struct BehaviorSubject<T> {
    sender: Arc<Sender<T>>,
    receiver: Receiver<T>,
}

// unsafe impl<T: Send + Sync> Send for BehaviorSubject<T> {}
// unsafe impl<T: Send + Sync> Sync for BehaviorSubject<T> {}

impl<T: 'static + Clone + Send + Sync> BehaviorSubject<T> {
    pub fn new(value: T) -> Self {
        let (sender, receiver) = channel(value);
        Self {
            sender: Arc::new(sender),
            receiver,
        }
    }

    pub fn value(&self) -> T {
        self.receiver.borrow().clone()
    }

    pub fn next(&self, value: T) {
        _ = self.sender.send(value)
    }

    pub fn subscribe(&self) -> WatchStream<T> {
        let rx = self.sender.subscribe();
        WatchStream::new(rx)
    }
}

// #[tokio::test]
// async fn test() {
//     let test = BehaviorSubject::new(0);

//     let mut stream = test.subscribe();

//     tokio::spawn(async move {
//         while let Some(value) = stream.next().await {
//             println!("Got {}", value);
//         }
//     });

//     test.next(1);

//     sleep(Duration::from_secs(1)).await;

//     println!("TEST GET VALUE {}", test.value());

//     test.next(2);
//     sleep(Duration::from_secs(2)).await;
//     test.next(3);
//     test.next(4);
//     sleep(Duration::from_secs(4)).await;
// }
