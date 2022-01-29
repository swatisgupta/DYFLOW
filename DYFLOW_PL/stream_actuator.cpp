#include "stream_actuator.hpp"
#include "stream_messenger.hpp"
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include<fstream>

bool StreamActuator::setup_conn() {

    if ( rank != 0 ) {
        msngr_intr = nullptr;
        return true;
    }

    if (msngr_intr == nullptr ) {
       if ( get_ip() ) {
            msngr_intr = new StreamMessenger(CLIENT, sender, ip_address);   
            msngr_intr->set_stream(stream);
            return true;
       } 
    } 
    return false;
}

StreamActuator::StreamActuator(int mrank, MPI_Comm mcomm, std::string str) {
     sender = RDWR;
     pid = getppid();
     program_name = program_invocation_short_name;  
     stream = str;
     msngr_intr = nullptr;  
     rank = mrank;
     comm = mcomm; 
}

StreamActuator::StreamActuator(std::string str): stream(str), msngr_intr(nullptr) {
     sender = RDWR;
     pid = getppid();
     rank = 0;
     comm = MPI_COMM_NULL;
     program_name = program_invocation_short_name; 
}

bool StreamActuator::_open() {
    bool ret = true;
    if ( setup_conn() ) {
         std::string msg = "{\"cmd\" : \"reg_conn\",\"pname\" : \"" + program_name + "\", \"pid\": \"" +  std::to_string(pid) + "\"}";
         msngr_intr->send_msg(msg);
         msg = msngr_intr->receive_msg();

         if ( msg.compare(RES_OK) != 0 ) {
                   ret = false;
         }
    }
    return ret;
}

bool StreamActuator::begin() {
    bool ret = true;
    bool ret_stat = false;

    if ( rank == 0 ) {
        if ( _open() ) {
             while( !ret_stat ) {
                std::string msg = "{\"cmd\" : \"begin_next\", \"pname\": \"" + program_name + "\", \"pid\" : \"" +  std::to_string(pid) + "\"}";
                msngr_intr->send_msg(msg);
                msg = msngr_intr->receive_msg();
                if ( msg.compare(RES_OK) == 0) {
                  ret_stat = true;
                } else if ( msg.compare(RES_SKIP) == 0) {
                  ret_stat = true;
                  ret = false;
                }
             }
        }
    } 
    
#ifdef ADIOS2_USE_MPI   
    if ( comm != MPI_COMM_NULL) { 
        MPI_Bcast(&ret, 1, MPI_C_BOOL, 0, comm);
        MPI_Barrier(comm);
    }
#endif 
    return ret;

}


bool StreamActuator::end() {
    bool ret = true;
    bool ret_stat = false;

    if ( rank == 0 ) { 
        if ( _open() ) { 
             while( !ret_stat ) {
                std::string msg = "{\"cmd\" : \"end_next\", \"pname\": \"" + program_name + "\", \"pid\" : \"" +  std::to_string(pid) + "\"}";
                msngr_intr->send_msg(msg);
                msg = msngr_intr->receive_msg();
                if ( msg.compare(RES_OK) == 0 ) {
                  ret_stat = true;
                }
             }   
        }   
    }   
#ifdef ADIOS2_USE_MPI   
    if ( comm != MPI_COMM_NULL) { 
       MPI_Bcast(&ret, 1, MPI_C_BOOL, 0, comm);
       MPI_Barrier(comm);
    }
#endif
    return ret;

}


int StreamActuator::put(std::string var, std::string & params){
    bool ret = false;
    int ret_val = -1;
    bool compress = false;
    bool dump = false;
    bool ret_status = false;
    int size;

    if ( rank == 0 ) {
        if ( setup_conn() && _open() ) {
             while( !ret_status ) {
               std::string out_msg = "{\"cmd\" : \"put_var\", \"pname\": \"" + program_name + "\", \"pid\" : \"" +  std::to_string(pid) + "\", \"var\" : \"" + var + "\"}";
               msngr_intr->send_msg(out_msg);
               std::string msg = msngr_intr->receive_msg();
               std::size_t found = msg.find(":");
               std::string tag = msg;
               if ( found != std::string::npos ) {
                    tag = msg.substr(0, found);
               }
               std::cout << " Returned msg " << msg << " tag $" << tag <<"$" << std::endl;

               if ( tag == std::string(RES_UNCOMP) ) {
                  ret_status = true;
               } else if ( tag == std::string(RES_COMP) ) {
                  params = msg.substr(found + 1).c_str();
                  size = params.size();
                  ret_status = true;
                  compress = true;              
               } else if (tag == std::string(RES_DUMP) ) {
                  ret_status = true;
                  dump = true; 
               }
            }
            ret = true;
        }   
    } 
#ifdef ADIOS2_USE_MPI   
    if ( comm != MPI_COMM_NULL) { 
       MPI_Bcast(&ret, 1, MPI_C_BOOL, 0, comm);                                                                                
       MPI_Barrier(comm);
    }
#endif
    if ( ret == true ) {
#ifdef ADIOS2_USE_MPI   
         if ( comm != MPI_COMM_NULL) { 
             MPI_Bcast(&compress, 1, MPI_C_BOOL, 0, comm); 
         }
         if ( compress == true ) {
             if ( comm != MPI_COMM_NULL) { 
                  MPI_Bcast(&size, 1, MPI_INT, 0, comm);
             } 
             char *param_cstr = nullptr; 
             if ( rank == 0 ) {
                 param_cstr = (char*) params.c_str();
             } else {
                 param_cstr = (char*) malloc(size); 
             }
             if ( comm != MPI_COMM_NULL) {
                 MPI_Bcast(param_cstr, size, MPI_CHAR, 0, comm);
             }
             //std::string m(params_cstr);
             params = param_cstr;
         }    
         if ( comm != MPI_COMM_NULL) {                                                                            
             MPI_Bcast(&dump, 1, MPI_C_BOOL, 0, comm);                                                                                
             MPI_Barrier(comm);
         }
#endif
         ret_val = 0;
         if (  compress == true )  {
             ret_val = 1;
         } 

         if (dump ==  true) {
             ret_val = 2;
         }
    }         
    return ret_val;                                                                                                             
}


bool StreamActuator::get(std::string var) {
    bool ret = true;
    bool ret_status = false;
    if ( rank == 0 ) {                                                                                                      
        if ( setup_conn() && _open() ) {
             while( !ret_status ) {
               std::string msg = "{ \"cmd\" : \"get_var\", \"pname\": \"" + program_name + "\", \"pid\" : \"" +  std::to_string(pid) + "\", \"var\" : \"" + var + "\"}";
               msngr_intr->send_msg(msg);
               msg = msngr_intr->receive_msg();
               if ( msg.compare(RES_OK) == 0 ) {
                  ret_status = true;
               }
            }  
        }
    }
#ifdef ADIOS2_USE_MPI 
    if ( comm != MPI_COMM_NULL) {  
        MPI_Bcast(&ret, 1, MPI_C_BOOL, 0, comm);
        MPI_Barrier(comm);
    }
#endif
    return ret;
}

bool StreamActuator::open() {
    bool ret = true;

    if ( rank == 0 ) {
        _open();
    } 
#ifdef ADIOS2_USE_MPI   
    if ( comm != MPI_COMM_NULL) { 
       MPI_Bcast(&ret, 1, MPI_C_BOOL, 0, comm);
       MPI_Barrier(comm);
    }
#endif
    return ret;

} 

bool StreamActuator::close() {
    bool ret = true; 
    bool ret_stat = false; 

    if ( rank == 0 ) {
           if ( setup_conn() ) {
              while( !ret_stat ) {  
                  std::string msg = "{\"cmd\" : \"dereg_conn\", \"pname\": \"" + program_name + "\", \"pid\" : \"" +  std::to_string(pid) + "\"}";
                  msngr_intr->send_msg(msg);
                  msg = msngr_intr->receive_msg();
                  if ( msg.compare(RES_OK) == 0 ) {
                      ret_stat = true;
                  } 
              }
           } 
    }
#ifdef ADIOS2_USE_MPI   
    if ( comm != MPI_COMM_NULL) { 
         MPI_Bcast(&ret, 1, MPI_C_BOOL, 0, comm);
         MPI_Barrier(comm);
    } 
#endif
    return ret;
}

bool StreamActuator::get_ip() {
    std::ifstream myfile;
    myfile.open("../"+ stream + ".interm");
    DEBUG("Checking if interm is present")
    if ( myfile.is_open() ) {
       std::getline(myfile, ip_address);
       myfile.close();
       return true;
    } 

    myfile.open("../../"+ stream + ".interm");
    if ( myfile.is_open() ) {
       std::getline(myfile, ip_address);
       myfile.close();
       return true;
    } 
    return false;
}

