#ifndef STREAMER_H
#define STREAMER_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "stream_messenger.hpp"
#include <sys/types.h>
#include <unistd.h>
#include "mpi.h"

class StreamActuator {
   public:
   bool open();

   bool read_var(std::string);
   bool decompress_var(std::string);

   bool write_var(std::string);
   bool compress_var(std::string);
   
   bool begin();

   int put(std::string var, std::string & params);

   bool end();

   bool get(std::string );

   bool close();

   bool get_ip();

   StreamActuator(int, MPI_Comm, std::string);
   StreamActuator(std::string);

   private:
   bool _open();
   StreamMessenger *msngr_intr;
   std::string stream;
 
   std::string program_name; //program_invocation_short_name 
   pid_t pid; //getpid()
   bool reader;
   int rank;
   MPI_Comm comm;

   std::string ip_address; 
   bool compress;
   bool setup_conn(); 
   SENDER_TYPE sender;   
};

#endif
