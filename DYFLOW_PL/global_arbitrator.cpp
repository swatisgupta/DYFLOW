#include "stream_messenger.hpp"
#include <unistd.h>
#include <iostream>
#include <nlohmann/json.hpp>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <vector>

using json = nlohmann::json;


void register_readers(StreamMessenger &msngr, std::vector<std::string> readers) {
    
    std::string msg2 = "{\"reg_readers\": ["; 

    for ( int i=0; i < readers.size(); i++ ) {
      if (i < readers.size() - 1) {
         msg2 += "\"" + readers[i] + "\",";
      } else {
         msg2 += "\"" + readers[i] + "\"";
      } 
    }
    msg2 += "]}";

    msngr.send_msg(msg2);
    msngr.receive_msg();
}

void register_writer(StreamMessenger &msngr, std::string writer) {
    auto msg3 = "{\"reg_writer\" : \""+ writer + "\"}";
    msngr.send_msg(msg3);
    msngr.receive_msg();
}

void UnSetcompression(StreamMessenger &msngr, std::string variable) {
    std::string msg1 = "{ \"unset_policy\" : \"COMPRESS\", \"compress_var\":\"" + variable + "\"}"; 
    msngr.send_msg(msg1);
    msngr.receive_msg();
}


void SetCompression(StreamMessenger &msngr, std::string variable, std::string compression_params) {
    std::string msg1 = "{ \"set_policy\" : \"COMPRESS\", \"compress_var\":\"" + variable + "\" , \"compress_params\" : \"" + compression_params +"\"}"; 
    msngr.send_msg(msg1);
    msngr.receive_msg();
}

void UnSetRoundRobin(StreamMessenger &msngr, std::string task="ALL") {
    std::string msg1 = "{ \"unset_policy\" : \"RR\", \"task\":\"" + task + "\"}"; 
    msngr.send_msg(msg1);
    msngr.receive_msg();
}

void SetRoundRobin(StreamMessenger &msngr, std::string reader="ALL") {
    std::string msg1 = "{ \"set_policy\" : \"RR\", \"task\": \"" + reader + "\"}"; 
    msngr.send_msg(msg1);
    msngr.receive_msg();
}

void UnSetSkip(StreamMessenger &msngr, std::string reader) {
    std::string msg1 = "{ \"set_policy\" : \"SKIP_STEPS_FREQ\", \"task\": \"" + reader + "\"}"; 
    msngr.send_msg(msg1);
    msngr.receive_msg();
}

void SkipN(StreamMessenger &msngr, std::string reader, int nsteps) {
    std::string msg1 = "{ \"set_policy\" : \"SKIP_STEPS\", \"task\": \"" + reader + "\", \"nsteps\":\"" +  std::to_string(nsteps) +  "\"}"; 
    msngr.send_msg(msg1);
    msngr.receive_msg();
}

void SkipFreq(StreamMessenger &msngr, std::string reader, int nsteps) {
    std::string msg1 = "{ \"set_policy\" : \"SKIP_STEPS_FREQ\", \"task\": \"" + reader + "\", \"freq\":\"" +  std::to_string(nsteps) +  "\"}"; 
    msngr.send_msg(msg1);
    msngr.receive_msg();
}

void HoldSteps(StreamMessenger &msngr,  std::string reader) {
    std::string msg1 = "{ \"set_policy\" : \"RESTART\", \"task\": \"" + reader + "\"}"; 
    msngr.send_msg(msg1);
    msngr.receive_msg();
}

void UnSetCheckpoint(StreamMessenger &msngr) {
    std::string msg1 = "{ \"unset_policy\" : \"CHECKPOINT_FREQ\"}"; 
    msngr.send_msg(msg1);
    msngr.receive_msg();
}

void CheckpointFreq(StreamMessenger &msngr,  int nsteps) {
    std::string msg1 = "{ \"set_policy\" : \"CHECKPOINT_FREQ\", \"freq\": \"" + std::to_string(nsteps) +  "\"}"; 
    msngr.send_msg(msg1);
    msngr.receive_msg();
}

void CheckpointM(StreamMessenger &msngr,  int nsteps) {
    std::string msg1 = "{ \"set_policy\" : \"CHECKPOINT\", \"nsteps\": \"" + std::to_string(nsteps) +  "\"}"; 
    msngr.send_msg(msg1);
    msngr.receive_msg();
}

void testCompression(StreamMessenger &msngr) {
    sleep(10);
    SetCompression(msngr, "atoms", "ZFP,rate,9,");
    sleep(1500);
}

void testCheckpoint(StreamMessenger &msngr) {
    sleep(10);
    CheckpointFreq(msngr, 2);
    sleep(300);
    UnSetCheckpoint(msngr);
    sleep(200);
    CheckpointM(msngr, 5);
}

void testHoldSteps(StreamMessenger &msngr, std::string reader) {
   HoldSteps(msngr, reader);
}

void testRR(StreamMessenger &msngr) {
   sleep(10);
   SetRoundRobin(msngr);
   sleep(300);
   UnSetRoundRobin(msngr);
}

void testSkipSteps(StreamMessenger &msngr, std::string reader) {
    sleep(10);
    SkipFreq(msngr, reader,2);
    sleep(300);
    UnSetSkip(msngr, reader);
    sleep(200);
    SkipN(msngr, reader, 5);
}


int main(int argc, char** argv) {
    
    char name[128];
    gethostname(name, 128);
    struct hostent *he;
    he = gethostbyname(name);
    auto nm1 = inet_ntoa(*(struct in_addr*)he->h_addr); 
    std::string nm(nm1);

    std::vector<std::string> readers;
    //std::string ip("tcp://" + nm + ":49152"); 
    std::string ip("tcp://10.103.128.13:4567");   
    std::cout << ip;
    StreamMessenger msngr(CLIENT, ARBITRATOR, ip);

    std::string stream(argv[1]);

    std::string reader(argv[2]);
    std::string reader1(argv[3]);
    std::string reader2(argv[4]);

    std::string writer(argv[5]);

    msngr.set_stream(stream);

    DEBUG("This is Arbitrator")

    std::string msg1 = "{ \"reg_stream\":\"" + stream + "\" }"; 
    msngr.send_msg(msg1);
    msngr.receive_msg();

    readers.push_back(reader);
    readers.push_back(reader1);
    readers.push_back(reader2);

    register_writer(msngr, writer);
    register_readers(msngr, readers);

    std::string msg4 = "{\"stop\": \"\"}";
    msngr.send_msg(msg4);
    msngr.receive_msg();
    msngr.close();

    return 0;
} 
