#include "messenger.hpp"
#include <unistd.h>
#include<iostream>
#include <nlohmann/json.hpp>
#include<unistd.h>

#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

using json = nlohmann::json;

int main(int argc, char** argv) {
    
    char name[128];
    gethostname(name, 128);
    struct hostent *he;
    he = gethostbyname(name);
    auto nm1 = inet_ntoa(*(struct in_addr*)he->h_addr); 
    std::string nm(nm1);

    std::string ip("tcp://" + nm + ":49152"); 
   
    std::cout << ip;
    Messenger msngr(CLIENT, ARBITRATOR, ip);

    std::string stream(argv[1]);
    std::string reader(argv[2]);
    std::string writer(argv[3]);
    msngr.set_stream(stream);

    DEBUG("This is Arbitrator")

    std::string msg1 = "{ \"reg_stream\":\"" + stream + "\" }"; 

    auto msg2 = "{\"reg_readers\": [ \"" + reader + "\"]}";

    auto msg3 = "{\"reg_writer\" : \""+ writer + "\"}";

    msngr.send_msg(msg1);
    msngr.receive_msg();

    msngr.send_msg(msg2);
    msngr.receive_msg();

    msngr.send_msg(msg3);
    msngr.receive_msg();
    
    sleep(300);
    auto msg4 = "{\"stop\": \"\"}";
    msngr.send_msg(msg4);
    msngr.receive_msg();

    msngr.close();

    return 0;
} 
