#ifndef CONN_H
#define CONN_H

#include <net/if.h>
#include <sys/ioctl.h>
#include <arpa/inet.h>
#include <malloc.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include "config.h"

#define MEMCACHED_PORT 11211

struct conn 
{
	int sock;
	int port;	
	int uid;
	int protocol;
};

//int openTcpSocket(const char* ipAddress, int port);
//int openUdpSocket(const char* ipAddress, int port);

struct conn* createConnection(const char* ip_address, int port, int protocol, int naggles);

#endif
