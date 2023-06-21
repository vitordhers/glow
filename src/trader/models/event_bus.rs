use polars::prelude::*;
use tokio::sync::watch::{channel, error::SendError, Receiver, Sender};
use async_trait::async_trait;

// #[derive(Debug)]
struct Module {
    receiver: Receiver<LazyFrame>,
}

impl Module {
    async fn test(&mut self) -> Result<(), SendError<LazyFrame>> {
        let rx = &mut self.receiver;
        println!("STARTED WHILE");
        match rx.changed().await {
            
            Ok(()) => {
                let lf = rx.borrow().clone();
                println!("received = {:?}", lf.collect().unwrap())},
            Err(err) => println!("ERROR {:?}", err),
        }
        println!("WHILE ENDED");
        // tx.send("world")?;

        Ok(())
    }
}

struct AnotherModule {
    emitter: Sender<LazyFrame>,
}

impl AnotherModule {
    pub fn emit(&self) -> Result<(), SendError<LazyFrame>> {
        let s1 = Series::new("Fruit", &["Apple", "Apple", "Pear"]);
        let s2 = Series::new("Color", &["Red", "Yellow", "Green"]);

        let df = DataFrame::new(vec![s1, s2]).unwrap();
        self.emitter.send(df.lazy())
    }
}

// impl<'a> AnotherModule<'static> {
//     pub fn emit(&self) -> Result<(), SendError<&str>> {
//         self.emitter.send("TEST1")
//     }
// }

pub trait Emitter<T> {
    fn send(&self, dte: T) -> Result<(), SendError<T>>;
}

#[async_trait]
pub trait Subscriber<T> {
    async fn subscribe(&mut self) -> Result<(), SendError<T>>;
}

#[tokio::test]
async fn run() {
    println!("RUNN'D");
    let (tx, rx) = channel(LazyFrame::default());

    let mut module = Module { receiver: rx };

    let mut second_module = Module {
        receiver: tx.subscribe(),
    };

    let handle = tokio::spawn(async move {
        let _ = module.test().await;
    });

    println!("BEFORE HANDLE AWAIT");
    let another_module = AnotherModule { emitter: tx };

    let _ = another_module.emit();

    let _ = handle.await.unwrap();

    println!("AFTER HANDLE AWAIT");

    let handle2 = tokio::spawn(async move {
        let _ = second_module.test().await;
    });

    let _ = handle2.await.unwrap();


}
