#include <exception>

class SocketError : public std::exception
{
public:
    SocketError(char const* error) : msg(error) {}
    const char* what()const throw();
private:
    char const* msg;
};


class Socket 
{
public:
    static Socket* createLocal(int port);
    static Socket* createGlobal(int port);
    static Socket* connect(char const* address, size_t maxAttempts = 10);
    Socket* accept();
    void read(void* buf, size_t size);    
    void write(void const* buf, size_t size);
    static Socket* select(size_t nSockets, Socket** sockets);
    ~Socket();

    static char const* unixSocketDir;
private:
    Socket(int fd) : sd(fd) {}
    int const sd;
};

