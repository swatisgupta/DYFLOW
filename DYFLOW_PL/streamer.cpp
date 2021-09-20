#include "streamer.hpp"
#include "messenger.hpp"
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include<fstream>

bool Streamer::setup_conn() {

    if ( rank != 0 ) {
        msngr_intr = nullptr;
        return true;
    }

    if (msngr_intr == nullptr ) {
       if ( get_ip() ) {
            msngr_intr = new Messenger(CLIENT, sender, ip_address);   
            msngr_intr->set_stream(stream);
            return true;
       } 
    } 
    return false;
}

Streamer::Streamer(int mrank, MPI_Comm mcomm, std::string str) : Streamer(str) {
    rank = mrank;
    comm = mcomm; 
}

Streamer::Streamer(std::string str): stream(str), msngr_intr(nullptr) {
     sender = RDWR;
     pid = getpid();
     program_name = program_invocation_short_name; 
}

bool Streamer::open() {
    bool ret = true;

    if ( rank == 0 ) {
           if ( setup_conn() ) {
               std::string msg = "{ \"reg_conn\" : { \"pname\" : \"" + program_name + "\", \"pid\": \"" +  std::to_string(pid) + "\"}}";
               msngr_intr->send_msg(msg);
               msg = msngr_intr->receive_msg();

               if ( msg.compare("OK") != 0 ) {
                   ret = false;
               }
           } 
    } 
#ifdef ADIOS2_USE_MPI   
    MPI_Bcast(&ret, 1, MPI_C_BOOL, 0, comm);
#endif
    return ret;

} 

bool Streamer::close() {
    bool ret = true; 

    if ( rank == 0 ) {
           if ( setup_conn() ) {
               std::string msg = "{\"dereg_conn\": { \"pname\": \"" + program_name + "\", \"pid\" : \"" +  std::to_string(pid) + "\"}}";
               msngr_intr->send_msg(msg);
               msg = msngr_intr->receive_msg();
               if ( msg.compare("OK")) {
                  ret = false;
               } 
            } 
    }
#ifdef ADIOS2_USE_MPI   
    MPI_Bcast(&ret, 1, MPI_C_BOOL, 0, comm);
#endif
    return ret;
}

bool Streamer::get_ip() {
    std::ifstream myfile;
    myfile.open("../"+ stream + ".interm");
    DEBUG("Checking if interm is present")
    if ( myfile.is_open() ) {
       std::getline(myfile, ip_address);
       myfile.close();
       return true;
    }   
    return false;
}

