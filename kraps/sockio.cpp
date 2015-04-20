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
 
char const* Socket::unixSocketDir = "/tmp/";

const char* SocketError::what()const throw()
{
    char* error = new char[strlen(msg) + 32];
    sprintf(error, "%s: %d\n", msg, errno);
    return error;
}

Socket* Socket::createGlobal(int port)
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
        throw SocketError("Failed to bind socket");
    }    
    return new Socket(sd);
}

Socket* Socket::createLocal(int port)
{
    struct sockaddr sock;
    sock.sa_family = AF_UNIX;
    int len = ((char*)sock.sa_data - (char*)&sock) + sprintf(sock.sa_data, "%sp%u", unixSocketDir, port);
    unlink(sock.sa_data); /* remove file if existed */
    int sd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sd < 0) { 
        throw SocketError("Failed to create local socket");
    }       
    if (bind(sd, &sock, len) < 0) {
        throw SocketError("Failed to bind socket");
    }    
    return new Socket(sd);
}

Socket* Socket::connect(char const* address, size_t maxAttempts)
{
    char const* sep = strchr(address, ':');
    if (sep == NULL) { 
        throw SocketError("Port is not specified");
    }
    *(char*)sep = '\0';
    int port = atoi(sep+1);
    int rc;
    int sd;
    while (1) { 
        if (strcmp(address, "localhost") == 0) { 
            struct sockaddr sock; 
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
            sock_inet.sin_family = AF_INET;  
            sock_inet.sin_port = htons(port);
            
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
                    break;
                }
            }
        }
        if (rc < 0) { 
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
            return new Socket(sd);
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
    int ns = ::accept(sd, (struct sockaddr*) &new_addr, &addrlen);
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
        int rc = ::select(max_sd+1, &events, NULL, NULL, NULL);
        if (rc < 0) { 
            if (rc != EINTR) {
                throw SocketError("Failed to select socket");
            }
        } else { 
            for (size_t i = 0; i < nSockets; i++) { 
                if (FD_ISSET(sockets[i]->sd, &events)) { 
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
