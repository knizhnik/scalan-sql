#include <exception.h>

class SocketError : public std::exception
{
public:
    class SocketError(cjar const* error) : msg(error) {}
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
    ~Socket();

    static char const* unixSocketDir;
private:
    int sd;
};

