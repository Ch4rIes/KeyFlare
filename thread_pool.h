//
// Created by Charles_Z on 2023-09-16.
//

#ifndef KEYFLARE_THREADPOOL_H
#define KEYFLARE_THREADPOOL_H

#endif //KEYFLARE_THREADPOOL_H

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include <vector>
#include <deque>


struct Work {
    void (*f)(void *) = NULL;
    void *arg = NULL;
};

struct TheadPool {
    std::vector<pthread_t> threads;
    pthread_cond_t not_empty;
    std::deque<Work> queue;
    pthread_mutex_t mu;
};

void thread_pool_init(TheadPool *tp, size_t num_threads);
void thread_pool_queue(TheadPool *tp, void (*f)(void *), void *arg);