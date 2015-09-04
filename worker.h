#ifndef WORKER_H
#define WORKER_H

#include <pthread.h>
#include <malloc.h>
#include <event2/event.h>
#include <sys/time.h>
#include <errno.h>
#include "config.h"
#include "conn.h"
#include "request.h"
#include "response.h"
#include "generate.h"

#include "mt19937p.h"

#define QUEUE_SIZE 1000000
#define INCR_FIX_QUEUE_SIZE 1000

struct worker 
{
	pthread_t thread;

	struct config* config;
	struct event_base* event_base;
	struct conn** connections;
	int nConnections;
	int cpu_num;
	struct timeval last_write_time;
	int interarrival_time;

	//Circular queue
	struct request* request_queue[QUEUE_SIZE];
	int head;
	int tail;
	int n_requests;
	int current_request_id;

	struct request* incr_fix_queue[INCR_FIX_QUEUE_SIZE];
	int incr_fix_queue_head;
	int incr_fix_queue_tail;
	struct mt19937p myMT19937p;
	int warmup_key;
	int warmup_key_check;	
	int received_warmup_keys;
};

void createWorkers(struct config* config);

#endif
