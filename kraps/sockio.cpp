#include <sys/ioctl.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/utsname.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <netdb.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include "sockio.h"
 
#ifndef USE_EPOLL
#define USE_EPOLL 1
#endif

#if USE_EPOLL
#include <sys/epoll.h>
#endif

char const* Socket::unixSocketDir = "/tmp/";

static void setGlobalSocketOptions(int sd)
{
    int optval = 1;
    setsockopt(sd, IPPROTO_TCP, TCP_NODELAY, (char const*)&optval, sizeof(int));
}

const char* SocketError::what() const throw()
{
    char* error = new char[strlen(msg) + 32];
    sprintf(error, "%s: %d\n", msg, errno);
    return error;
}

bool Socket::isLocalHost(char const* address) 
{
    struct utsname localHost;
    uname(&localHost);
    size_t localHostNodeNameLen = strlen(localHost.nodename);
    return strncmp(address, "localhost:", 10) == 0
        || (strncmp(address, localHost.nodename, localHostNodeNameLen) == 0 &&
            (address[localHostNodeNameLen] == ':' || address[localHostNodeNameLen] == '.'));
}
    
Socket* Socket::createGlobal(int port, size_t listenQueueSize)
{
    struct sockaddr_in sock; 
    sock.sin_family = AF_INET;
    sock.sin_addr.s_addr = htonl(INADDR_ANY);
    sock.sin_port = htons(port);
    int sd = socket(AF_INET, SOCK_STREAM, 0);
    if (sd < 0) { 
        throw SocketError("Failed to create global socket");
    }       
    int on = 1;
    setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, (char*)&on, sizeof on);

    if (bind(sd, (sockaddr*)&sock, sizeof(sock)) < 0) {
        throw SocketError("Failed to bind global socket");
    }
    if (listen(sd, listenQueueSize) < 0) {
        throw SocketError("Failed to listen global socket");
    }
    setGlobalSocketOptions(sd);
    return new Socket(sd, false);
}

Socket* Socket::createLocal(int port, size_t listenQueueSize)
{
    struct sockaddr sock;
    sock.sa_family = AF_UNIX;
    size_t len = ((char*)sock.sa_data - (char*)&sock) + sprintf(sock.sa_data, "%sp%u", unixSocketDir, port);
    unlink(sock.sa_data); /* remove file if existed */
    int sd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sd < 0) {
        throw SocketError("Failed to create local socket");
    }
    if (bind(sd, &sock, len) < 0) {
        throw SocketError("Failed to bind local socket");
    }
    if (listen(sd, listenQueueSize) < 0) {
        throw SocketError("Failed to listen local socket");
    }
    return new Socket(sd, true);
}

static bool getAddrsByName(const char *hostname, unsigned* addrs, size_t* n_addrs)
{
    struct sockaddr_in sin;
    struct hostent* hp;
    size_t i;
    
    sin.sin_addr.s_addr = inet_addr(hostname);
    if (sin.sin_addr.s_addr != INADDR_NONE) {
        memcpy(&addrs[0], &sin.sin_addr.s_addr, sizeof(sin.sin_addr.s_addr));
        *n_addrs = 1;
        return true;
    }

    hp = gethostbyname(hostname);
    if (hp == NULL || hp->h_addrtype != AF_INET) { 
        return false;
    }
    for (i = 0; hp->h_addr_list[i] != NULL && i < *n_addrs; i++) { 
        memcpy(&addrs[i], hp->h_addr_list[i], sizeof(addrs[i]));
    }
    *n_addrs = i;
    return true;
}

Socket* Socket::connect(char const* address, size_t maxAttempts)
{
    char* sep = (char*)strchr(address, ':');
    if (sep == NULL) { 
        throw SocketError("Port is not specified");
    }
    int port = atoi(sep+1);
    int rc = 0;
    int sd;
    while (1) {
        bool isLocal;
        if (isLocalHost(address)) { 
            struct sockaddr sock; 
            isLocal = true;
            sock.sa_family = AF_UNIX;
            sd = socket(AF_UNIX, SOCK_STREAM, 0); 
            if (sd < 0) { 
                throw SocketError("Failed to create local socket");
            }    
            size_t len = ((char*)sock.sa_data - (char*)&sock) + sprintf(sock.sa_data, "%sp%u", unixSocketDir, port);
            do { 
                rc = ::connect(sd, &sock, len);
            } while (rc < 0 && errno == EINTR);            
        } else { 
            struct sockaddr_in sock_inet;
            unsigned addrs[128];
            size_t n_addrs = sizeof(addrs) / sizeof(addrs[0]);
            isLocal = false;
            sock_inet.sin_family = AF_INET;  
            sock_inet.sin_port = htons(port);
            size_t hostLen = sep - address;
            char* host = new char[hostLen+1];
            memcpy(host, address, hostLen);
            host[hostLen] = '\0';
            if (!getAddrsByName(host, addrs, &n_addrs)) {
                throw SocketError("Failed to resolve addresses");
            }
            delete[] host;

            sd = socket(AF_INET, SOCK_STREAM, 0);
            if (sd < 0) { 
                throw SocketError("Failed to create global socket");
            }       
            for (size_t i = 0; i < n_addrs; ++i) {
                memcpy(&sock_inet.sin_addr, &addrs[i], sizeof sock_inet.sin_addr);
                do { 
                    rc = ::connect(sd, (struct sockaddr*)&sock_inet, sizeof(sock_inet));
                } while (rc < 0 && errno == EINTR);

                if (rc >= 0 || errno == EINPROGRESS) { 
                    if (rc >= 0) {
                        setGlobalSocketOptions(sd);
                    }
                    break;
                }
            }
        }
        if (rc < 0) { 
            ::close(sd);
            if (errno == EINPROGRESS) {
                throw SocketError("Failed to connect socket");
            }
            if (errno == ENOENT || errno == ECONNREFUSED) {
                if (maxAttempts-- != 0) {
                    sleep(1);
                    continue;
                }
            }
            throw SocketError("Connection can not be establish");
        } else { 
            return new Socket(sd, isLocal);
        }
    }
}

void Socket::read(void* buf, size_t size)
{
    size_t offs = 0;
    while (offs < size) { 
        int rc = recv(sd, (char*)buf + offs, size - offs, 0);
        if (rc <= 0) { 
            if (errno == EINTR) { 
                continue;
            }
            throw SocketError("Failed to read data from socket");
        }
        offs += rc;
    }
}
        
void Socket::write(void const* buf, size_t size)
{
    size_t offs = 0;
    while (offs < size) { 
        int rc = send(sd, (char const*)buf + offs, size - offs, 0);
        if (rc <= 0) { 
            if (errno == EINTR) { 
                continue;
            }
            throw SocketError("Failed to write data to socket");
        }
        offs += rc;
    }
}
        
Socket* Socket::accept()
{
    int ns = ::accept(sd, NULL, NULL);
    if (ns < 0) { 
        throw SocketError("Failed to accept socket");
    }
    setGlobalSocketOptions(ns);
    return new Socket(ns, localSocket);
}


Socket* Socket::select(size_t nSockets, Socket** sockets)
{
    static size_t rr = 0;
	int rc;
    while (true) { 
#if USE_EPOLL
        struct epoll_event event;
		event.events = EPOLLIN;
		int epollfd = epoll_create(1);
		assert(epollfd >= 0);			
        for (size_t i = 0; i < nSockets; i++) { 
            if (sockets[i] != NULL) {
				event.data.ptr = (void*)sockets[i];
				rc = epoll_ctl(epollfd, EPOLL_CTL_ADD, sockets[i]->sd, &event);
				assert(rc == 0);
			}
		}
        rc = epoll_wait(epollfd, &event, 1, -1);        
		close(epollfd);
        if (rc < 0) { 
			throw SocketError("Failed to select socket");
        } else if (rc != 0) {                         
			return (Socket*)event.data.ptr;
		}
#else
        fd_set events;
        FD_ZERO(&events);
        int max_sd = 0;
        for (size_t i = 0; i < nSockets; i++) { 
            if (sockets[i] != NULL) {
                int sd = sockets[i]->sd;
                if (sd > max_sd) { 
                    max_sd = sd;
                }
                FD_SET(sd, &events);
            }
        }
        int rc = ::select(max_sd+1, &events, NULL, NULL, NULL);
        if (rc < 0) { 
            if (errno != EINTR) {
                throw SocketError("Failed to select socket");
            }
        } else {                         
            for (size_t i = 0, j = rr; i < nSockets; i++) {
                j = (j + 1) % nSockets;  // round robin
                if (sockets[j] && FD_ISSET(sockets[j]->sd, &events)) { 
                    rr = j;
                    return sockets[j];
                }
            }
        }
#endif
    }
}

Socket::~Socket()
{
    ::close(sd);
}
