import queue
import threading

"""
    Usage example: 
        1. Initializing with 10 threads
        multi_threading = helpers.threads.MultiThreading(10)
        
        2. Adding parameters list: 
        multi_threading.update_queue(parameters_list)
        
        3. Start multi-threads with function process_parameters
        multi_threading.start_threads(process_parameters)
"""


class ProcessingThread(threading.Thread):
    """Extending the Thread"""
    def __init__(self, threadID, q, queueLock, func):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = "Thread " + str(threadID)
        self.q = q
        self.queueLock = queueLock
        self.empty_queue = 0
        self.inner_func = func


    def run(self):
        """Overwrite the threads.Thread run function to handle our calls"""
        print("Starting " + self.name)
        self.process_data()
        print("Exiting " + self.name)


    def update_empty_flag(self):
        self.empty_queue = 1


    def process_data(self):
        while not self.empty_queue:
            self.queueLock.acquire()
            if not self.q.empty():
                data = self.q.get()
                self.queueLock.release()
                print("%s processing " % (self.name))
                try:
                    self.inner_func(data)
                except:
                    print("Error in thread %s", self.name)
                    self.update_empty_flag()
                    raise
                # self.q.task_done()
            else:
                self.queueLock.release()


class MultiThreading:
    """Multithread management"""
    def __init__(self, max_threads):
        self.threadList = range(max_threads)
        self.queueLock = threading.Lock()
        self.workQueue = queue.Queue(max_threads)
        self.work_to_do = []
        self.threads = []


    def update_queue(self, to_process):
        self.work_to_do = to_process


    def start_threads(self, inn_function):
        # Create new threads
        # inn_function -> function that will be called by each thread
        for thread_id in self.threadList:
            thread = ProcessingThread(thread_id, self.workQueue, self.queueLock, inn_function)
            thread.start()
            self.threads.append(thread)

        # Fill the queue
        self.queueLock.acquire()
        for work in self.work_to_do:
            self.workQueue.put(work)
        self.queueLock.release()

        # Wait for queue to empty
        while not self.workQueue.empty():
            pass

        # If the queue is empty, update the status for each thread
        self.stop_threads()

        # Wait for all threads to complete
        for t in self.threads:
            t.join()
        print("Exiting Main Thread")


    def stop_threads(self):
        # Notify threads it's time to exit
        for thread in self.threads:
            thread.update_empty_flag()