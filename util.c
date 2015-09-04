// Misc helper functions
#include "util.h"
#include "mt.h"
#include "worker.h"
#include "mt19937p.h"

double round(double d) 
{
	return floor(d + 0.5);
}

void writeBlock(int fd, void* buffer, int writeSize) 
{
#if DEBUG_READ_WRITE
	printf("Going to write %d bytes\n", writeSize);
	printf("Writing:\n");

	for(int i = 0; i < writeSize; i++){
		printf("%8x ", *((unsigned char*)buffer+i));
		if(i % 4 == 3 && i != 0){
			printf("\n");
		}
	}
	printf("\n");
#endif

	int nWriteTotal = 0;
	int nWrite = 0;

	while(nWriteTotal != writeSize){
		nWrite = write(fd, buffer + nWriteTotal, writeSize - nWriteTotal);
		if(nWrite < 0) {
			printf("Write error: %d\n", nWrite);
			perror("Write error");
			exit(-1);
		}
		nWriteTotal += nWrite;
	}

#if DEBUG_READ_WRITE
	printf("Done with write block\n");
#endif
}

void readBlock(int fd, void* buffer, int readSize) 
{
#if DEBUG_READ_WRITE
	printf("Going to read %d bytes\n", readSize);
#endif

	int nReadTotal = 0;
	int nRead = 0;
	while(nReadTotal != readSize){
		nRead = read(fd, buffer + nReadTotal, readSize - nReadTotal);
		if(nRead < 0) {
			perror("Read error");
			exit(-1);
		}
		nReadTotal += nRead;
	}

#if DEBUG_READ_WRITE
	printf("Readed:\n");

	for(int i = 0; i < readSize; i++){
		printf("%8x ", *((unsigned char*)buffer+i));
		if(i % 4 == 3 && i != 0){
			printf("\n");
		}
	}
	printf("----\n");
	printf("Done with read block\n");
#endif
}

// Let's us replace the  random function
int randomFunction()
{
	//int rand_int =  genrand_int32();
	//  printf("rand int %d\n", rand_int);
	return rand();
}

int parRandomFunction(struct worker* worker)
{
	//printf("rand int %d\n", rand_int);
	//int rand_int = rand();
	// printf("Done\n");
	return genrand(&(worker->myMT19937p));
}

// Creates a string of length size
char* randomString(int size)
{
	static const char text[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

	char* randomString = malloc(sizeof(char) * size);
	for(int i = 0; i < size - 1; i++ ){
		randomString[i] = text[randomFunction() % (sizeof(text) - 1)];
	}
	randomString[size-1] = '\0';

	return randomString;
}

//Translate a hostname to an IP address
char* nslookup(char* hostname)
{
	struct hostent* host_info = 0 ;

	for(int attempt=0 ; (host_info==0) && (attempt<3) ; ++attempt ) {
		host_info = gethostbyname(hostname) ;
	}

	char* ip_address;
	if(host_info){
		struct in_addr * address = (struct in_addr * )host_info->h_addr;
		ip_address = inet_ntoa(*address);

		printf("host: %s\n", host_info->h_name);
		printf("address: %s\n",ip_address);

	} 
	else {
		printf("DNS error\n");
		exit(-1);
	}

	//pretty sure i should free something here... 
	return ip_address;
}

void timestampTest() 
{
	printf("--- Timestamp test ---\n");

	struct stat res;
	int nIterations = 100000;
	for(int i = 0; i < nIterations; i++) {
		struct timeval timestamp1,timestamp2;
		gettimeofday(&timestamp1, NULL);
		gettimeofday(&timestamp2, NULL);
		double diff = (timestamp2.tv_sec - timestamp1.tv_sec) + 1e-6*(timestamp2.tv_usec - timestamp1.tv_usec);
		addSample(&res, diff);
	}

	printf("Timestamp resolution %.9f s\n", getAvg(&res));
}

void lockTest()
{
	pthread_mutex_t lock=PTHREAD_MUTEX_INITIALIZER;

	int nIterations = 100000;
	struct timeval timestamp1,timestamp2;
	gettimeofday(&timestamp1, NULL);
	for(int i = 0; i < nIterations; i++){
		pthread_mutex_lock(&lock);  
		pthread_mutex_unlock(&lock);  
	}
	gettimeofday(&timestamp2, NULL);

	double diff = (timestamp2.tv_sec - timestamp1.tv_sec) + 1e-6*(timestamp2.tv_usec - timestamp1.tv_usec);
	double time = diff/(float)nIterations;
	
	printf("Lock/unlock time %.9f \n", time);
}

void timingTests() 
{
	timestampTest();
	lockTest();
}
