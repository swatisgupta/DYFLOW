#include "stream_messenger.hpp"
#include "info_manager.hpp"
#include <nlohmann/json.hpp>
#include <unistd.h>
#include <iostream>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <fstream>
// for convenience
using json = nlohmann::json;

bool done = false;
std::string my_ip;
std::string stream_file = ""; 
InfoManager* datam; // = new InfoManager(""); 




void apply_arbitrator_policy(json j_msg, std::string stream) {
                if ( j_msg.contains("stop") )  { 
                    done = true;
                    if (!stream_file.compare("")) {
                        remove(stream_file.c_str()); 
                        stream_file = ""; 
                    }
                    return;
                } 
                if ( j_msg.contains("reg_stream") )  {   
                    if ( datam == nullptr ) {
                       datam = new InfoManager(j_msg.value("reg_stream", ""));
                       if (!stream_file.compare("")) {
                           remove(stream_file.c_str()); 
                       }
                       stream_file = "../" + j_msg.value("reg_stream", "") + ".interm"; 
                       std::ofstream myfile;
                       myfile.open(stream_file); //, std::ios::trunc); 
 		       if (myfile.is_open()) {
                           DEBUG("creating : " + stream_file)
                           myfile << my_ip;
  			   myfile.close();
                       }
                       DEBUG("stream registered : " + j_msg.value("reg_stream", ""))
                    }   
                }   

                bool check = (datam != nullptr && !stream.compare(datam->get_stream()));
                std::string val = check ? "true" : "false";
                DEBUG("Comparison :" +  val)
                DEBUG("Input for Stream :" +  stream)
                DEBUG("Expected for Stream :" +  datam->get_stream())
                if ( j_msg.contains("reg_readers") ) {   
                    if ( check ) { 
                       auto r_array = j_msg.at("reg_readers"); 
                       for (auto& r : r_array.items()) {
                           DEBUG(" Value: ")
                           DEBUG(r.value())
                           datam->register_reader(r.value());
                       }
                    }   
                }   

                if ( j_msg.contains("reg_writer") )  {
                    if ( check ) { 
                       datam->register_writer(j_msg.value("reg_writer", ""));
                       DEBUG("writer to register : " + j_msg.value("reg_writer", ""))
                    }   
                }   

                if ( j_msg.contains("set_policy") )  {
                    if ( check ) { 
                        std::string policy_cmd{j_msg.at("set_policy")};
                        if (policy_cmd == "COMPRESS") {
                            std::string compress_var{j_msg.at("compress_var")};
                            std::string compress_algo{j_msg.at("compress_algo")};
                        }   
                        else {
                            std::string policy_for{j_msg.at(policy_cmd)};
                                                                                                                                          
                            if (policy_cmd == "START") {
                            } else if  (policy_cmd == "RESTART") {
                            } else if  (policy_cmd == "STOP") {
                            } else if (policy_cmd == "CHECKPOINT_FREQ") { 
                            } else if (policy_cmd == "CHECKPOINT") { 
                            } else if (policy_cmd == "SKIP_STEPS_FREQ") {
                            } else if (policy_cmd == "SKIP_STEPS") {
                            }   
                        }
                     }   
                }   

                if ( j_msg.contains("unset_policy") ) {
                    if ( check ) { 
                        std::string policy_cmd{j_msg.at("set_policy")};
                        if (policy_cmd == "COMPRESS") {
                            std::string compress_var{j_msg.at("compress_var")};
                        } else {
                            std::string policy_for{j_msg.at(policy_cmd)};
                            if (policy_cmd == "CHECKPOINT_FREQ") {
                            } else if (policy_cmd == "SKIP_STEPS_FREQ") {
                            }   
                        }
                    }   
                } 
}

std::string handle_conn_requests(json j_msg, std::string stream, int tag_pid) {
                bool check = (datam != nullptr && !stream.compare(datam->get_stream()));
                std::string val = check ? "true" : "false";
                DEBUG("Comparison :" +  val)
                DEBUG("Input for Stream :" +  stream)
                DEBUG("Expected for Stream :" +  datam->get_stream())

                if (!check) {
                    return RES_OK;
                }

                if ( j_msg.contains("reg_conn") ) {   
                       auto reader = j_msg.at("reg_conn"); 
                       if ( reader.contains("pid")) {
                           int pid = atoi(reader.value("pid", "-1" ).c_str());
                           std::string rdr_nm = reader.value("pname", "");
                           DEBUG(" Value: " )
                           DEBUG(reader )
                           if (datam->register_connection(rdr_nm, pid)) { 
                               return RES_OK; 
                           }
                       }   
                } else if ( j_msg.contains("dereg_conn") ) {  
                       auto reader = j_msg.at("dereg_conn"); 
                       if ( reader.contains("pid")) {
                           int pid = atoi(reader.value("pid", "-1" ).c_str()); 
                           std::string pname = reader.value("pname", "");
                           bool allow_close = true;
                           DEBUG(" Value: ")
                           DEBUG(reader)
                           if (datam->is_registered(pname, pid)) {
                           }
                           if (allow_close) {
                               return RES_OK;
                           } else {
                               return RES_WAIT;
                           }
                       }
                } else if ( j_msg.contains("begin_next") ) {
                    auto reader = j_msg.at("begin_next"); 
                       if ( reader.contains("pid")) {
                           int pid = atoi(reader.value("pid", "-1").c_str()); 
                           std::string pname = reader.value("pname", "");
                           bool allow_step = true;
                           DEBUG(" Value: ")
                           DEBUG(reader)
                           if (datam->is_registered(pname, pid)) {
                           }   
                           if (allow_step) {
                               return RES_SKIP;
                           } else {
                               return RES_OK;
                           }
                       }   
                } else if ( j_msg.contains("end_step") ) {
                    
                } else if ( j_msg.contains("put_var") ) {

                } else if ( j_msg.contains("get_var") ) {

                }    
                  
}


int main(int argc, char** argv) {
    
    char name[128];
    gethostname(name, 128);
    
    struct hostent *he;
    he = gethostbyname(name);
    auto nm1 = inet_ntoa(*(struct in_addr*)he->h_addr); 
    //std::string my_ip("tcp:\\localhost:1027"); 
    std::string nm(nm1); 
    my_ip = "tcp://" + nm + ":49152";

   /* Sever that listens to requests from stream connection */
    StreamMessenger msgr_conn(SERVER, INTERM, my_ip);  

   /* Client that receives instructions from arbitrator */ 
    //StreamMessenger msgr_arb = new StreamMessenger(CLIENT, INTERM, arb_ip);

    DEBUG("This is Data Manager")

    SENDER_TYPE client;
    int seq;
    std::string stream("");
 
    while ( !done ) {
        std::string msg = msgr_conn.receive_msg();
        DEBUG("Processing" + msg)
        msgr_conn.decode_tag(client, seq, stream);
        try {
            json j_msg = json::parse(msg);
            DEBUG("Received from " + std::to_string(client));
            if ( client == ARBITRATOR ) {

               apply_arbitrator_policy(j_msg, stream);
               msgr_conn.send_msg(RES_OK);

            }  else if ( client == RDWR ) {
               
              std::string msg = handle_conn_requests(j_msg, stream, seq);

              msgr_conn.send_msg(msg);
            } 
        } 
        catch (json::out_of_range& e) {
        }
        catch (json::type_error& e) {
        } 
    }

    msgr_conn.close();
    return 0;
}

