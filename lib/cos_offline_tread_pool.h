#ifndef CONCURRENT_THREADPOOL_H
#define CONCURRENT_THREADPOOL_H

#include <atomic>
#include <thread>
#include <mutex>
#include <array>
#include <list>
#include <functional>
#include <condition_variable>

/**
 *  Simple ThreadPool
 *  Creates `ThreadCount` threads in the initialization
 *
 *  Gets new jobs from a queue.
 *
 *  The default `ThreadCount` is 10.
 *
 *  This class requires your compiler supports C++11 features.
 */
namespace Storage {
        template<unsigned ThreadCount = 10>
        class ThreadPool {

            std::array<std::thread, ThreadCount> threads;
            std::list<std::function<void(void)>> queue;

            std::atomic_int jobs_left;
            std::atomic_bool bailout;
            std::atomic_bool finished;
            std::condition_variable job_available_var;
            std::condition_variable wait_var;
            std::mutex wait_mutex;
            std::mutex queue_mutex;

            /**
             *  Take the next job from the queue and run it.
             *  Notify the main thread that a job has completed.
             */
            void Task() {
                while (!bailout) {
                    next_job()();
                    wait_var.notify_one();
                }
            }

            /**
             *  Get the next job; pop the first item from the queue,
             *  otherwise wait for a signal from the main thread.
             */
            std::function<void()> next_job() {
                std::function<void(void)> res = [] {};
                std::unique_lock<std::mutex> job_lock(queue_mutex);

                // Wait for a job if we don't have any.
                job_available_var.wait(job_lock, [this]() -> bool { return queue.size() || bailout; });

                // Get job from the queue
                if (!bailout) {
                    res = queue.front();
                    queue.pop_front();
                    --jobs_left;
                }
                return res;
            }

        public:
            ThreadPool()
                    : jobs_left(0), bailout(false), finished(false) {
                for (unsigned i = 0; i < ThreadCount; ++i)
                    threads[i] = std::thread([this] { this->Task(); });
            }

            /**
             *  JoinAll on destruction
             */
            ~ThreadPool() {
                JoinAll();
            }

            /**
             *  Get the number of threads in this pool
             */
            inline unsigned Size() const {
                return ThreadCount;
            }

            /**
             *  Get the number of jobs in the queue.
             */
            inline unsigned JobsRemaining() {
                std::lock_guard<std::mutex> guard(queue_mutex);
                return static_cast<unsigned>(queue.size());
            }

            /**
             *  Add a new job to the pool. If there is no job in the queue,
             *  a thread is woken up to take the job. If all threads are busy,
             *  the job is added to the end of the queue.
             */
            void AddJob(std::function<void(void)> job) {
                std::lock_guard<std::mutex> guard(queue_mutex);
                queue.emplace_back(job);
                ++jobs_left;
                job_available_var.notify_one();
            }

            /**
             *  Join with all threads. Block until all threads have completed.
             *  Params: WaitForAll: If true, will wait for the queue to empty
             *  before joining with threads. If false, will complete
             *  current jobs, then inform the threads to exit.
             *  The queue will be empty after this call, and the threads will
             *  be done. After invoking `ThreadPool::JoinAll`, the pool can no
             *  longer be used. If you need the pool to exist past completion
             *  of jobs, look to use `ThreadPool::WaitAll`.
             */
            void JoinAll(bool WaitForAll = true) {
                if (!finished) {
                    if (WaitForAll) {
                        WaitAll();
                    }

                    // note that we're done, and wake up any thread that's
                    // waiting for a new job
                    bailout = true;
                    job_available_var.notify_all();

                    for (auto &x : threads)
                        if (x.joinable())
                            x.join();
                    finished = true;
                }
            }

            /**
             *  Wait for the pool to be empty before continuing.
             *  This doesn't call `std::thread::join` which only waits until
             *  all jobs have finished execution.
             */
            void WaitAll() {
                if (jobs_left > 0) {
                    std::unique_lock<std::mutex> lk(wait_mutex);
                    wait_var.wait(lk, [this] { return this->jobs_left == 0; });
                    lk.unlock();
                }
            }
        };

    } // namespace concurrent
#endif //CONCURRENT_THREADPOOL_H
