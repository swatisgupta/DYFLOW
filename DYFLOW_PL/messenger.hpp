#ifndef MESSENGER_H
#define MESSENGER_H

#include <zmq.hpp>
#include <iostream>

#ifndef NODEBUG
#define DEBUG(msg) { std::cout<< msg << std::endl << std::flush; }
#endif

enum SOCKET_TYPE {
    CLIENT,
    SERVER,
    PUB,
    SUB
};

enum SENDER_TYPE {
    INTERM,
    ARBITRATOR,
    WRITER,
    READER,
    RDWR
};


#define RES_OK "OK"
#define RES_WAIT "WAIT"
#define RES_SKIP "SKIP_STEP"
#define RES_COMP "COMPRESS_VAR"
#define RES_DUMP "DUMP_STEP"

class Messenger {

    public:
    Messenger(SOCKET_TYPE, SENDER_TYPE, const std::string);
    bool send_msg(std::string);
    std::string receive_msg();
    void set_stream(std::string);
    void decode_tag(SENDER_TYPE &, int &, std::string &);
    std::string set_msg(std::string);
    std::string decode_msg (std::string); 
    void close();

    private:
    SOCKET_TYPE socket_t; 
    SENDER_TYPE sender_t;
    std::string stream;
    int seq;  
    std::string tag;

    zmq::context_t context;

    std::string ip_address;
    zmq::socket_t *socket;  
    void create_socket();
};

#endif
