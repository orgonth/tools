"""
---------------
Threads manager
---------------
version:    0.3
author:     Tommy Carozzani, 2017

Multiple threads manager with groups and group-priority.
This module provides a pool-like class (ThreadManager) similar to what you can find in
the builtin multiprocess module, however it uses threads instead of subprocesses.

Simple example
--------------

def my_func(x,y,z):
    ...
    return a,b

tm = ThreadManager()

tm.add_job(my_func, (0,1,2))
tm.add_job(my_func, kwargs={'x': 10, 'y': 15, 'z': 20})
tm.add_job(my_func, (3,4), {'z': 5})

tm.run(2)

Example with a callback function
--------------------------------

The aim is to split a job in two and pass results of the first to the second.
Useful with groups if one part of the job can be run concurrently and not the other (e.g. writing to a file).

def my_other_func(a,b):
    ...

def my_third_func(a,b,c):
    ...

tm.add_job(my_func, (6,7,8), callback = my_other_func)
tm.add_job(my_func, (9,10,11), callback = lambda a,b,c=15: my_third_func(a,b,c))

tm.run(2)

Example with groups
-------------------

g1 = tm.add_group()
g2 = tm.add_group(1)            # group with maximum 1 simultaneous task
g3 = tm.add_group(priority=0)   # high-priority group

g1.add_job(my_func, (12,13,14))
tm.add_job(my_func, (15,16,17), group=g2)
g3.add_job(my_func, (18,19,20), callback=my_other_func, callback_group=g2)

tm.run(2)
"""

import threading
import queue
import time

_DEBUG = 1


def _dprint(msg, level=1):
    if _DEBUG>=level:
        print('[multi] '+msg)


class NoSlotsAvailable(Exception):
    """Exception raised by group if no slots are available"""
    def __init__(self, msg):
        self.msg = msg
        
    def __str__(self):
        return self.msg


class MultipleErrors(Exception):
    """Exception holding one or more exceptions"""
    def __init__(self, err_list):
        self.err_list = err_list
        
    def __str__(self):
        msg = ""
        for e in self.err_list:
            msg += '\n'+str(e)
        return msg


class Counter:
    """Thread-safe counter"""
    def __init__(self, value=0):
        """Arguments:
           value -- initial value of the counter
        """
        self.lock = threading.Lock()
        self.counter = value

    def acquire(self):
        """Lock the counter (wait if necessary)"""
        self.lock.acquire()

    def release(self):
        """Release the counter"""
        self.lock.release()

    def increase(self, value=1):
        """Thread-safe increase of the counter
           Arguments:
           value -- how much to increase (default 1)
        """
        self.acquire()
        self.counter += value
        self.release()

    def increase_and_notify(self, value, notify_value, event):
        """Thread-safe increase of the counter
           Arguments:
           value -- how much to increase
           notify_value -- if the internal counter is equal to notify_value after the
                           increase, event will be notified
           event -- event to be notified if the counter is equal to notify_value
        """
        self.acquire()
        self.counter += value
        if self.counter == notify_value:
            with event:
                event.notifyAll()
        self.release()

    def decrease(self, value=1):
        """Thread-safe decrease of the counter
           Arguments:
           value -- how much to decrease (default 1)
        """
        self.increase(-value)

    def decrease_and_notify(self, value, notify_value, event):
        """Thread-safe decrease of the counter
           Arguments:
           value -- how much to decrease
           notify_value -- if the internal counter is equal to notify_value after the
                           decrease, event will be notified
           event -- event to be notified if the counter is equal to notify_value
        """
        self.increase_and_notify(-value, notify_value, event)


class Group:
    """Group of awaiting jobs (tasks)"""
    def __init__(self, manager, max_threads, priority):
        """Arguments:
           manager -- thread manager owning the group
           max_threads -- max number of simultaneous tasks allowed for this group
           priority -- priority of the group (big number = low priority)
        """
        assert (max_threads != 0)
        self.jobs = queue.Queue(-1)
        self.max_threads = max_threads
        self.nb_slots = Counter(max_threads)
        self.priority = priority
        self._tm = manager

    def _add_job(self, job, event=None):
        """Internal function called by the thread manager

           Arguments:
           job -- tasks that will be added to the queue
           event -- if not None (default), event will be notified if there are available slots.
                    In other terms, notification means there is at least one job in this group
                    that could be treated.
        """
        self.jobs.put(job)
        if event is not None:
            self.nb_slots.acquire()
            if self.nb_slots.counter != 0:
                with event:
                    event.notifyAll()
            self.nb_slots.release()

    def add_job(self, func, args=(), kwargs={},
                callback=None, callback_group=None):
        """Adds a new job in the queue (same effect as ThreadManager.add_job)

           Arguments:
           func -- function to be called
           args -- tuple of arguments for func
           kwargs -- dictionary of arguments for func
           callback -- a function that will be called (under the form of another job) when
                       func ends. Results of func will be passed as parameters of callback.
                       Default None.
           callback_group -- group to which callback job will belong to (default None =
                       default group, which has low priority)
        """
        self._tm.add_job(func, args, kwargs, self, callback, callback_group)

    def get_job_nowait(self):
        """Returns:
           A job to be treated

           Raises:
           queue.Empty if no more jobs
           NoSlotsAvailable if max_threads is reached
        """
        self.nb_slots.acquire()
        if self.nb_slots.counter != 0:
            try:
                job = self.jobs.get_nowait()
                self.nb_slots.counter -= 1
            except queue.Empty as e:
                raise e
            finally:
                self.nb_slots.release()
        else:
            self.nb_slots.release()
            raise NoSlotsAvailable('Max nb of threads ({}) reached for this group'.format(self.max_threads))

        return job

    def task_done(self, event=None):
        """Marks a previous job obtained through get_job_nowait as complete.
           Effectively increases the number of available slots.

           Arguments:
           event -- if not None (default), event will be notified if there are remaining jobs.
                    In other terms, notification means there is at least one job in this group
                    that could be treated.

           Raises:
           Exception if no job to mark as complete
        """
        self.nb_slots.acquire()
        if self.nb_slots.counter >= self.max_threads:
            self.nb_slots.release()
            raise Exception('task_done called too many times')
        else:
            self.nb_slots.counter += 1
            if event is not None and not self.jobs.empty():
                with event:
                    event.notifyAll()
            self.nb_slots.release()


class ThreadManager:
    """Manages a collection of jobs (in groups) and threads"""
    def __init__(self):
        self.job_or_idle = threading.Condition()
        self.groups = []
        self.add_group(-1, 2**17)
        self.nb_jobs_total = Counter(0)     # total jobs added

    def add_group(self, max_threads=-1, priority=2**16):
        """Creates a new group of jobs

           Arguments:
           max_threads -- max number of simultaneous tasks allowed for this group (-1 = no limit = default)
           priority -- priority of the group (big number = low priority = default)

           Returns:
           The new group
        """
        g = Group(self, max_threads, priority)
        g.id = len(self.groups)
        self.groups.append(g)
        return g

    def add_job(self, func, args=(), kwargs={}, group=None,
                callback=None, callback_group=None):
        """Adds a new job in the queue

           Arguments:
           func -- function to be called
           args -- tuple of arguments for func
           kwargs -- dictionnary of arguments for func
           group -- group to which the job will belong to (default None = default group,
                    which has low priority)
           callback -- a function that will be called (under the form of another job) when
                       func ends. Results of func will be passed as parameters of callback.
                       Default None.
           callback_group -- group to which callback job will belong to (default None =
                       default group, which has low priority)
        """
        if group is None:
            group = self.groups[0]

        if group not in self.groups:
            self.groups.append(group)

        job = threading.Thread(target=self._f_wrap,
                               args=(func, args, kwargs, group, callback, callback_group)
                               )
        job.given_func_name = func.__name__
        job.given_args = args
        job.given_kwargs = kwargs
        job.given_id = self.nb_jobs_total.counter
        group._add_job(job, event=self.job_or_idle)
        self.nb_jobs_total.increase()

    def _f_wrap(self, func, args, kwargs, group, callback, callback_group):
        """Convenience function wrapper for threads

           Arguments:
           func -- function to be called
           args -- tuple of arguments for func
           kwargs -- dictionnary of arguments for func
           group -- group to which the job will belong to
           callback -- a function that will be called (under the form of another job) when
                       func ends. Results of func will be passed as parameters of callback.
           callback_group -- group to which callback job will belong to
        """
        self.running_threads.increase()

        res = None

        try:
            res = func(*args, **kwargs)
            if callback is not None:
                self.add_job(callback, args=res, group=callback_group)
        except BaseException as e:
            self.run_error.set()
            self.errors_lock.acquire()
            self.errors.append(e)
            self.errors_lock.release()
            
        self.jobs_available.release()

        group.task_done(self.job_or_idle)

        self.running_threads.decrease_and_notify(1, 0, self.job_or_idle)
        
        return res

    def run(self, nb_threads):
        """Run threads until there are no more jobs to process

           Arguments:
           nb_threads -- number of simultaneous threads to run jobs on

           Raises:
           MultipleErrors if one or more job raises an error
        """
        if self.nb_jobs_total.counter == 0:
            return
        
        assert(nb_threads > 0)
        self.nb_threads = nb_threads
        self.threads = []
        for i in range(nb_threads):
            self.threads.append((None, None))     # (job,group)

        self.jobs_available = threading.Semaphore(nb_threads)
        self.running_threads = Counter()

        self.run_error = threading.Event()
        self.run_error.clear()
        self.errors = []
        self.errors_lock = threading.Lock()

        splitter = threading.Thread(target=self._give_jobs)

        splitter.start()

        splitter.join()

    def _get_slot(self, loop=False):
        """Get an available thread slot

           Arguments:
           loop -- if False (default), raises an error if no available slot.
                   If True, loop until there is one available (cpu intensive)

           Returns:
           Available slot number
        """
        while True:
            for i in range(self.nb_threads):
                t, g = self.threads[i]
                if t is None or not t.is_alive():
                    return i
            if not loop:
                raise Exception('no slot available')
            time.sleep(0.01)

    def _get_job_nowait(self):
        """Get an available job to process (non blocking)

           Returns:
           An available job from the highest priority group,
           or raise exception

           Raises:
           queue.Empty if no pending jobs
           NoSlotsAvailable if no slot in any group
           Exception if blocking job
        """
        job = None
        err_q_empty = None
        err_no_slots = None
        for g in sorted(self.groups, key=lambda x: x.priority):
            try:
                job = g.get_job_nowait()
                if job is not None:
                    break
            except queue.Empty as e:
                job = None
                err_q_empty = e
            except NoSlotsAvailable as e:
                job = None
                err_no_slots = e

        if job is None:
            if err_no_slots is not None:
                raise err_no_slots
            elif err_q_empty is not None:
                raise err_q_empty
            else:
                raise Exception('empty (None) job')

        return job, g

    def _give_jobs(self):
        """Distributes and start jobs among threads"""
        while True:
            _dprint('wait for a slot...',2)
            self.jobs_available.acquire()
            self.running_threads.acquire()

            i = self._get_slot(True)
            t, g = self.threads[i]
            if t is not None:
                t.join()

            if self.run_error.is_set():
                break
            
            _dprint('[got a slot: {}] wait for a job...'.format(i),2)
            try:
                job, group = self._get_job_nowait()
            except:
                if self.running_threads.counter == 0:
                    self.running_threads.release()
                    _dprint('no more job 1', 2)
                    break

                with self.job_or_idle:
                    self.running_threads.release()
                    self.job_or_idle.wait()
                try:
                    job, group = self._get_job_nowait()
                except queue.Empty:
                    _dprint('no more job 2', 2)
                    break
            else:
                self.running_threads.release()

            if self.run_error.is_set():
                break

            _print_types = (str, bool, int, float)
            _dprint('sending job #{} (group {}) to slot {} >>> {}{}{}'
                    .format(job.given_id, group.id, i,
                            job.given_func_name,
                            tuple([x for x in job.given_args if type(x) in _print_types]),
                            [f'{x}: {job.given_kwargs[x]}' for x in job.given_kwargs
                             if type(job.given_kwargs[x]) in _print_types]
                            ))
            self.threads[i] = (job, group)
            job.start()

        for i in range(self.nb_threads):
            t, g = self.threads[i]
            if t is not None and t.is_alive():
                t.join()
                
        if self.run_error.is_set():
            _dprint('there were {} errors'.format(len(self.errors)))
            self.run_error.clear()
            raise MultipleErrors(self.errors)

        _dprint('done')
