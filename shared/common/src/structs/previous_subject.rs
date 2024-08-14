use futures_util::Stream;
use futures_util::StreamExt;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch::{channel, Receiver, Ref, Sender};
use tokio_stream::wrappers::WatchStream;

#[derive(Clone)]
pub struct BehaviorSubject<T> {
    sender: Arc<Sender<T>>,
    receiver: Receiver<T>,
    previous_value: Arc<Mutex<Option<T>>>,
}

impl<T: 'static + Clone + Send + Sync> BehaviorSubject<T> {
    pub fn new(value: T) -> Self {
        let (sender, receiver) = channel(value);
        Self {
            sender: Arc::new(sender),
            receiver,
            previous_value: Arc::new(Mutex::new(None)),
        }
    }

    pub fn value(&self) -> T {
        self.receiver.borrow().clone()
    }

    pub fn ref_value(&self) -> Ref<'_, T> {
        self.receiver.borrow()
    }

    pub fn previous_value(&self) -> Option<T> {
        let previous_value = self.previous_value.lock().unwrap();
        previous_value.clone()
    }

    pub fn next(&self, value: T) {
        let mut previous_value = self.previous_value.lock().unwrap();
        *previous_value = Some(self.receiver.borrow().clone());
        _ = self.sender.send(value)
    }

    pub fn subscribe(&self) -> WatchStream<T> {
        let rx = self.sender.subscribe();
        WatchStream::new(rx)
    }
}

#[tokio::test]
async fn test() {
    use std::time::Duration;
    use tokio::time::sleep;
    let test = BehaviorSubject::new(0);
    let test_clone: BehaviorSubject<i32> = test.clone();

    let mut stream = test.subscribe();

    tokio::spawn(async move {
        while let Some(value) = stream.next().await {
            println!(
                "Got {}, and previous value {:?}",
                value,
                test_clone.previous_value()
            );
        }
    });

    test.next(1);

    sleep(Duration::from_secs(1)).await;

    println!("TEST GET VALUE {}", test.value());

    test.next(2);
    sleep(Duration::from_secs(2)).await;
    test.next(3);
    println!("OUT OF CONTEXT PREVIOUS VALUE {:?}", test.previous_value());
    test.next(4);
    sleep(Duration::from_secs(4)).await;
}
