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
#include "sockio.h"
#include "exception.h"
 
char const* Socket::unixSocketDir = "/tmp/";

const char* SocketError::what()const throw()
{
    char* error = new char[strlen(msg) + 32];
    sprintf(error, "%s: %d\n", msg, errno);
    return error;
}

Socket* Socket::createGlobal(int port)
{
    struct sockaddr_in sock_inet;
    u.sock.sa_family = AF_INET;
    u.sock_inet.sin_addr.s_addr = htonl(INADDR_ANY);
    u.sock_inet.sin_port = htons(port);
    int len = sizeof(u.sock_inet);
    int sd = socket(u.sock.sa_family, SOCK_STREAM, 0);
    if (sd < 0) { 
        throw SocketException("Failed to create local socket");
    }       
    int on = 1;
    setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, (char*)&on, sizeof on);

    if (bind(sd, &u.sock, len) < 0) {
        throw SocketException("Failed to bind socket");
    }    
    return new Socket(sd);
}

Socket* Socket::createLocal(int port)
{
    struct sockaddr sock;
    u.sock.sa_family = AF_UNIX;
    int len = offsetof(struct sockaddr, sa_data) + sprintf(u.sock.sa_data, "%sp%u", unixSocketDir, port);
    unlink(u.sock.sa_data); /* remove file if existed */
    int sd = socket(u.sock.sa_family, SOCK_STREAM, 0);
    if (sd < 0) { 
        throw SocketException("Failed to create global socket");
    }       
    if (bind(sd, &u.sock, len) < 0) {
        throw SocketException("Failed to bind socket");
    }    
    return new Socket(sd);
}

Socket* Socket::connect(char const* address, int maxAttempts)
{
    char const* sep = strchr(address, ':');
    union { 
        struct sockaddr sock;
        struct sockaddr_in sock_inet;
    } u;
    if (sep == NULL) { 
        throw SocketException("Port is not specified");
    }
    *(char*)sep = '\0';
    int port = atoi(sep+1);
    int rc;
    while (1) { 
        if (strcmp(address, "localhost") == 0) { 
            struct sockaddr sock;
            int len = offsetof(struct sockaddr, sa_data) + sprintf(u.sock.sa_data, "%sp%u", unixSocketDir, port);
            do { 
                rc = connect(s->handle, &sock, len);
            } while (rc < 0 && ERRNO_INTR());            
        } else { 
            struct sockaddr_in sock_inet;
            uint4 addrs[128];
            int   n_addrs = sizeof(addrs) / sizeof(addrs[0]);

#if defined(_ECOS)
            sock_inet.sin_len = sizeof(struct sockaddr_in);
#endif
            sock_inet.sin_family = AF_INET;  
            sock_inet.sin_port = htons(port);
            
            for (i = 0; i < n_addrs; ++i) {
                memcpy(&sock_inet.sin_addr, &addrs[i], sizeof sock_inet.sin_addr);
                do { 
                    rc = connect(s->handle, (struct sockaddr*)&sock_inet, sizeof(sock_inet));
                } while (rc < 0 && ERRNO_INTR());

                if (rc >= 0 || errno == EINPROGRESS) { 
                    break;
                }
            }
        }
        if (rc < 0) { 
            if (errno == EINPROGRESS) {
                throw SocketError("Failed to connect socket");
            }
            if (errno == ENOENT || errno == ECONNREFUSED) {
                if (--maxAttempts > 0) {
                    sleep(1);
                    continue;
                }
            }
            throw SocketError("Connection can not be establish");
        } else { 
            return new Socket(rc);
        }
    }
}

void Socket::read(void* buf, size_t size)
{
    size_t offs = 0;
    while (offs < size) { 
        int rc = recv(sd, (char*)buf + offs, size - offs, 0);
        if (rc < 0) { 
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
        if (rc < 0) { 
            if (errno == EINTR) { 
                continue;
            }
            throw SocketError("Failed to read data from socket");
        }
        offs += rc;
    }
}
        
Socket* Socket::accept()
{
    struct sockaddr_in new_addr;
    socklen_t addrlen = sizeof(new_addr);
    int ns = accept(sd, (struct sockaddr*) &new_addr, &addrlen);
    if (ns < 0) { 
        throw SocketError("Failed to accept socket");
    }
    return new Socket(ns);
}


Socket* Socket::select(size_t nSockets, Socket** sockets)
{
    while (true) { 
        fd_set events;
        FD_ZERO(&events);
        int max_sd = 0;
        for (size_t i = 0; i < nSockets; i++) { 
            int sd = sockets[i]->sd;
            if (sd > max_sd) { 
                max_sd = sd;
            }
            FD_SET(sd, &events);
        }
        int rc = select(max_sd+1, &events, NULL, NULL, NULL);
        if (rc < 0) { 
            if (rc != EINTR) {
                throw SocketError("Failed to select socket");
            }
        } else { 
            for (size_t i = 0; i < nSockets; i++) { 
                if (FD_ISSET(sockets[i]->sd)) { 
                    return sockets[i];
                }
            }
        }
    }
}

Socket::~Socket()
{
    ::close(sd);
}
