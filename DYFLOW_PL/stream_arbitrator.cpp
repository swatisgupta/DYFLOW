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

std::fstream logger;
 
std::string apply_arbitrator_policy(json j_msg, std::string stream) {

 
                if ( j_msg.contains("stop") )  { 
                    done = true;
                    if (!stream_file.compare("")) {
                        remove(stream_file.c_str()); 
                        stream_file = ""; 
                    }
                    return "SUCCESS";
                } 

                if ( j_msg.contains("reg_stream") )  {   
                    if ( datam == nullptr ) {
                       datam = new InfoManager(stream); //j_msg.value("reg_stream", ""));
                       if (!stream_file.compare("")) {
                           remove(stream_file.c_str()); 
                       }
                       //stream_file = "../" + j_msg.value("reg_stream", "") + ".interm"; 
                       stream_file = "../" + stream + ".interm"; 
                       std::ofstream myfile;
                       myfile.open(stream_file); //, std::ios::trunc); 
 		       if (myfile.is_open()) {
                           logger << "creating : " << stream_file << std::endl;
                           myfile << my_ip;
  			   myfile.close();
                       }
                       logger << "stream registered : " << stream << std::endl; //j_msg.value("reg_stream", ""))
                    }   
                    return "SUCCESS";
                }   

                bool check = (datam != nullptr && !stream.compare(datam->get_stream()));
                std::string val = check ? "true" : "false";
                std::string ret_msg = "SUCCESS";
                logger << "Comparison :" <<  val << std::endl; 
                logger << "Input for Stream :" <<  stream << std::endl;
                logger << "Expected for Stream :" <<  datam->get_stream() << std::endl;
                if ( !check ) { 
                    return "NOT_READY";
                }

                if ( j_msg.contains("reg_readers") ) {   
                       auto r_array = j_msg.at("reg_readers"); 
                       for (auto& r : r_array.items()) {
                           logger << " Value: " << r.value() << std::endl;
                           datam->register_reader(r.value());
                       }
                }   

                else if ( j_msg.contains("reg_writer") )  {
                       datam->register_writer(j_msg.value("reg_writer", ""));
                       logger << "writer to register : " << j_msg.value("reg_writer", "") << std::endl;

                } else {

                       std::string instruction = "";
                       bool set_policy = true; 
                       std::vector<std::string> params;
                       int policy = -1;
                       std::string task;

                       if ( j_msg.contains("set_policy")) {
                           instruction = "set_policy";
                        } else if ( j_msg.contains("unset_policy")) {
                           instruction = "unset_policy";
                           set_policy = false; 
                        } else {
                           return "INVALID_REQ";
                        }
                        std::string policy_cmd{j_msg.at(instruction)};

                        if (policy_cmd == "COMPRESS") {
                            std::string compress_var{j_msg.at("compress_var")};
                            task = compress_var;
                            policy = NO_COMPRESS;
                            if (set_policy) {
                                policy = COMPRESS;
                                std::string compress_params{j_msg.at("compress_params")};
                                int len = compress_params.length(), pos;
                                std::string delimiter = ",";
                                while ((pos = compress_params.find(delimiter)) != std::string::npos) {
 				   params.push_back(compress_params.substr(0, pos));
    				   compress_params = compress_params.substr(pos + delimiter.length(), len);
			        }
                            }

                        } else if  (policy_cmd == "RESTART") {
                           task = j_msg.at("task");
                           policy = set_policy ? RESTART : -1 ;

                        } else if  (policy_cmd == "RR") {
                           task = j_msg.at("task");
                           policy = set_policy ? DIST_RR : NO_RR; 

                        } else if (policy_cmd == "CHECKPOINT_FREQ") { 
                           std::string freq{j_msg.at("freq")};
                           policy = set_policy ? CHECKPOINT_FREQ : NO_CHECKPOINT; 
		           params.push_back(freq);		
                           
                        } else if (policy_cmd == "CHECKPOINT") { 
                           std::string nsteps{j_msg.at("nsteps")};
                           policy = set_policy ? CHECKPOINT_N : -1;
		           params.push_back(nsteps);		

                        } else if (policy_cmd == "SKIP_STEPS_FREQ") {
                           std::string freq{j_msg.at("freq")};
                           task = j_msg.at("task");
                           policy = set_policy ? SKIP_STEPS_FREQ : NO_SKIP_STEPS; 
		           params.push_back(freq);		


                        } else if (policy_cmd == "SKIP_STEPS") {
                           std::string nsteps{j_msg.at("nsteps")};
                           task = j_msg.at("task");
                           policy = set_policy ? SKIP_STEPS_N : -1;
		           params.push_back(nsteps);		

                        } else if  (policy_cmd == "STOP") {
                           delete datam;
                           datam = nullptr;
                           std::remove(stream_file.c_str());
                           return ret_msg;
                        }   
                        if ( policy != -1 ) {
                           datam->set_policy(task, policy, params);
                        } else { 
                           ret_msg ="INVALID_POLICY";
                        }
                     }   
                     return ret_msg;
}

std::string handle_conn_requests(json j_msg, std::string stream, int tag_pid) {
                bool check = (datam != nullptr && !stream.compare(datam->get_stream()));
                std::string val = check ? "true" : "false";
                logger << "Comparison :" <<  val << std::endl;
                logger << "Input for Stream :" <<  stream << std::endl;
                logger << "Expected for Stream :" <<  datam->get_stream() << std::endl;
                int pid;
                std::string pname;

                if (!check || !j_msg.contains("pid")) {
                    return RES_OK;
                }

                pid = atoi(j_msg.value("pid", "-1" ).c_str());
                pname = j_msg.value("pname", "");

                datam->register_connection(pname, pid); 
                
                logger << "Registered!!!" << std::endl;

                std::string cmd = j_msg.value("cmd", "");
                if ( cmd == "dereg_conn" ) {  
                    bool allow_close = datam->deregister_connection(pname, pid); 
                    logger << "DEREG_CONN!!!" << std::endl;
                    return allow_close ? std::string(RES_OK) : std::string(RES_WAIT);

                } else if ( cmd == "end_step" ) {
                    bool allow_step = datam->end_step(pname, pid);
                    logger << "End Step!!!" << std::endl;
                    return allow_step ? RES_OK : RES_WAIT;

                } else if ( cmd == "begin_step" ) {
                    int allow_step = datam->begin_step(pname, pid);
                    logger << "Begin Step!!!" << std::endl;
                    return (allow_step == 1 ? std::string(RES_OK): allow_step == 0 ? std::string(RES_SKIP) : std::string(RES_WAIT));

                } else if ( cmd == "put_var" ) {
                    logger << "PUT_VAR!!!" << std::endl;
                    std::string varname = j_msg.value("var", "" );
                    std::string compress_params;
                    int allow_step = datam->put_var(pname, pid, varname, compress_params);
                    std::string out = RES_COMP;
                    out += ":" + compress_params;
                    return (allow_step == 0 ? std::string(RES_UNCOMP) : (allow_step == 2 ? std::string(RES_DUMP) : out));

                } else if ( cmd == "get_var" ) {
                    logger << "GET_VAR!!!" << std::endl;
                    std::string varname = j_msg.value("var", "" );
                    int allow_step = datam->get_var(pname, pid, varname);
                    return (allow_step == 1 ? std::string(RES_OK): allow_step == 0 ? std::string(RES_SKIP) : std::string(RES_WAIT));
                }      
                  
                return std::string(RES_OK);
}


int main(int argc, char** argv) {
    
    char name[128];
    /*
    gethostname(name, 128);
    os.spawnvpe    
    struct hostent *he;
    he = gethostbyname(name);
    auto nm1 = inet_ntoa(*(struct in_addr*) he->h_addr); 
    //std::string my_ip("tcp:\\localhost:1027"); 
    //std::string nm(nm1); */ 

    std::string nm(argv[1]);
    my_ip = "tcp://" + nm; // + ":" + port;
    std::cout<< my_ip << std::endl;
   /* Sever that listens to requests from stream connection */
    std::string log_file = "stream_arbitrator_" + nm + ".log";
    logger.open (log_file, std::fstream::out | std::fstream::app);
    StreamMessenger msgr_conn(SERVER, INTERM, my_ip);  

   /* Client that receives instructions from arbitrator */ 
    //StreamMessenger msgr_arb = new StreamMessenger(CLIENT, INTERM, arb_ip);

    logger << "This is Data Manager"  << std::endl;

    SENDER_TYPE client;
    int seq;
    std::string stream("");
 
    while ( !done ) {
        std::string msg = msgr_conn.receive_msg();
        logger << "Processing" << msg << std::endl;
        msgr_conn.decode_tag(client, seq, stream);
        try {
            json j_msg = json::parse(msg);
            logger << "Received from " << std::to_string(client) << std::endl;
            if ( client == ARBITRATOR ) {

               apply_arbitrator_policy(j_msg, stream);
               std::string res = RES_OK; 
               msgr_conn.send_msg(res);

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
    logger.close();
    return 0;
}

