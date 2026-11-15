mod io;
mod join_handle;
mod primitives;
mod queue;
mod task;

use std::cell::RefCell;
use std::collections::LinkedList;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::Thread;

use crate::primitives::write_once::WriteOnce;
use crate::task::DynTask;

pub struct MultiThreadedRuntime {
    global_queue: Mutex<LinkedList<Arc<dyn DynTask>>>,
    workers: WriteOnce<Box<[Worker]>>,
    parked_workers: Mutex<Vec<usize>>,
}

#[derive(Debug)]
pub struct Worker {
    thread: Thread,
    parker: Mutex<Option<WorkerThreadWaker>>,
    unparker: Condvar,
}

#[derive(Debug)]
enum WorkerThreadWaker {
    Parked,
}

impl MultiThreadedRuntime {
    fn new(n: NonZeroUsize) -> Arc<Self> {
        Arc::new(Self {
            global_queue: Mutex::new(LinkedList::new()),
            workers: WriteOnce::new(),
            parked_workers: Mutex::new(Vec::with_capacity(n.get())),
        })
    }

    pub fn thread_per_cote() -> Arc<Self> {
        let threads = std::thread::available_parallelism().unwrap();
        Self::new(threads)
    }

    fn init_workers(self: Arc<Self>, worker_init: NonZeroUsize) {
        let mut workers = Vec::<Worker>::with_capacity(worker_init.get());
        for i in 0..worker_init.get() {
            let handle = ExecutorHandle {
                worker_index: i,
                shared: self.clone(),
                local_queue: RefCell::new(VecDeque::new()),
            };
            std::thread::scope(|s| {
                s.spawn(|| {
                    while handle.shared.workers.get().is_none() {
                        std::thread::park();
                    }
                });
            });
        }
    }
}

// Each thread will have this present thread locally.
struct ExecutorHandle {
    worker_index: usize,
    shared: Arc<MultiThreadedRuntime>,
    local_queue: RefCell<VecDeque<Arc<dyn DynTask>>>,
}

thread_local! {
    static HANDLE: RefCell<Option<Rc<ExecutorHandle>>> = RefCell::new(None);
}

impl ExecutorHandle {
    fn run_forever(self: Rc<Self>) {
        loop {
            let workers = match self.shared.workers.get() {
                Some(workers) => workers,
                None => continue,
            };
            let worker = &workers[self.worker_index];
            HANDLE.with(|handle| {
                *handle.borrow_mut() = Some(self.clone());
            });
            loop {
                let task = if self.local_queue.borrow().len() > 0 {
                    self.get_local_task()
                } else {
                    self.get_global_task()
                };
                if let Some(task) = task {
                    // task.run();
                }
                // TODO: handle io here!
                //  else if todo!() {
                //     todo!()
                // }
                else {
                    // No work to do, sleep!
                    let mut parking = worker.parker.lock().unwrap();
                    if parking.is_none() {
                        {
                            let mut parked_workers = self.shared.parked_workers.lock().unwrap();
                            parked_workers.push(self.worker_index);
                        }
                        *parking = Some(WorkerThreadWaker::Parked);
                        *worker.unparker.wait(parking).unwrap() = None;
                    }
                }
            }
        }
    }

    fn get_local_task(&self) -> Option<Arc<dyn DynTask>> {
        let mut queue = self.local_queue.borrow_mut();
        queue.pop_front()
    }

    fn get_global_task(&self) -> Option<Arc<dyn DynTask>> {
        let mut global = self.shared.global_queue.lock().unwrap();
        global.pop_front()
    }

    fn schedule_local(&self, task: Arc<dyn DynTask>) {
        let mut queue = self.local_queue.borrow_mut();
        if queue.len() < queue.capacity() {
            queue.push_back(task);
        } else {
            task.schedule_global();
        }
    }
}
