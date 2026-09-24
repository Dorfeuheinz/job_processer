use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};

type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct JobProcessor {
    sender: Option<mpsc::Sender<Job>>,
    workers: Vec<JoinHandle<()>>,
}

impl JobProcessor {
    pub fn new(n: usize) -> Self {
        assert!(n > 0, "need at least one worker");
        let (sender, receiver) = mpsc::channel::<Job>();
        let receiver = Arc::new(Mutex::new(receiver));

        let workers = (0..n)
            .map(|_| {
                let rx = Arc::clone(&receiver);
                thread::spawn(move || loop {
                    // The guard is a temporary dropped at the end of this statement,
                    // so the lock is released before the job runs.
                    let msg = rx.lock().unwrap().recv();
                    match msg {
                        Ok(job) => job(),
                        Err(_) => break, // all senders dropped and queue drained
                    }
                })
            })
            .collect();

        Self { sender: Some(sender), workers }
    }

    pub fn submit<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender
            .as_ref()
            .expect("sender exists until drop")
            .send(Box::new(job))
            .expect("workers alive");
    }
}

impl Drop for JobProcessor {
    fn drop(&mut self) {
        // Closing the channel makes recv() return Err once queued jobs are drained.
        drop(self.sender.take());
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

fn main() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let counter = Arc::new(AtomicUsize::new(0));
    {
        let pool = JobProcessor::new(4);
        for i in 0..20 {
            let counter = Arc::clone(&counter);
            pool.submit(move || {
                counter.fetch_add(1, Ordering::Relaxed);
                println!("job {i} on {:?}", thread::current().id());
            });
        }
    } // drop: waits for all 20 jobs to finish
    assert_eq!(counter.load(Ordering::Relaxed), 20);
    println!("all jobs done");
}