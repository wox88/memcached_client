// client - a memcached load tester
// David Meisner 2010 (meisner@umich.edu)
#ifndef CONFIG_H
#define CONFIG_H

#include <stdint.h>
#include "generate.h"
#include "pthread.h"

#define TCP_MODE 0
#define UDP_MODE 1
#define MAX_SERVERS 4

//#define FLEXUS
#ifdef FLEXUS
#include "magic2_breakpoint.h"
#endif

struct config 
{
	int protocol_mode;			//TCP or UDP

	int n_cpus;					//numbers of CPU available
	int n_keys;					//numbers of keys generated

	int n_workers;				//numbers of work threads
	int n_servers;				//numbers of      memcacehd     servers  
	struct worker** workers;	//pointer to data of all workers
	int n_connections_total;	//

	int run_time;
	int stats_time;
	int naggles;
	int multiget_size;

	//
	char* server_ip_address[MAX_SERVERS];
	int server_port[MAX_SERVERS];
	//
	char* server_file;
	char* input_file;
	char* output_file;
	
	int server_memory;
	int keysToPreload;			//the number of items to preload,equal to min(trace length, mc server volume)
	int scaling_factor;			

	float get_frac;				// the fraction of get operation
	float multiget_frac;		// the fraction of multiget operation
	float incr_frac;			// the fraction of increase operation

	struct key_list* key_list;
	//if the followed are used, they should point to an array include 10000 integer,indicates cdf distribution
	struct int_dist* key_pop_dist;		// uniform distribution default, changed when -N option specified
	struct int_dist* value_size_dist;	// uniform distribution default, changed when -d option specified
	struct int_dist* multiget_dist;		// uniform distribution default, changed when -L option specified
	struct int_dist* interarrival_dist;	// not used when rps<=0
	struct dep_dist* dep_dist;

	int arrival_distribution_type;	//
	//int received_warmup_keys;		//never used
	int rps;						//request per second
	int fixed_size;					//use fixed value size
	int zynga;						//currently not uesd
	int random_seed;				//currently not uesd
	int pre_load;					//warm up ther server or not
	int bad_multiget;				//when set,the socket send buffer only contain one request, then send;
									//else ,the socket send buffer contain multi requests, then send.

	uint32_t current_request_uid;
};

#endif //CONFIG_H
