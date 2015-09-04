#ifndef GENERATE_H
#define GENERATE_H

#include <malloc.h>
#include <math.h> 

#include "config.h"
#include "worker.h"
#include "util.h"

#define MAX_KEY_SIZE 250
#define CDF_VALUES 10000
#define ALPHA 0.915

#define ARRIVAL_CONSTANT 0
#define ARRIVAL_EXPONENTIAL 1

struct config;
struct worker;

struct int_dist 
{
	int cdf_y[CDF_VALUES];
};

struct dep_entry 
{
	double cdf;
	int size;
	char key[251];
};

struct dep_dist 
{
	struct dep_entry** dep_entries;
	int n_entries;
};

struct key_list 
{
	char** keys;
	int n_keys;
};

int getIntQuantile(struct int_dist* dist);

struct dep_entry* getRandomDepEntry(struct dep_dist* dep_dist, struct worker* worker);

struct request* generateRequest(struct config* config, struct worker* worker);
double harmonicSum(int size, double alpha);

//
struct dep_dist* loadDepFile(struct config* config);
struct dep_dist* loadAndScaleDepFile(struct config* config);

//
struct int_dist* createUniformDistribution(int min, int max);
struct int_dist* createConstantDistribution(int constant);
struct int_dist* createExponentialDistribution(int meanInterarrival);

struct int_dist* loadDistributionFile(char* filename);
struct key_list* generateKeys(struct config* config);

#endif
