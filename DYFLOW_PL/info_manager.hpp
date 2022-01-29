#ifndef DATAMANAGER_H
#define DATAMANAGER_H

#include <unordered_map>
#include <vector>
#include "stream_messenger.hpp"
#include "stream_prop.hpp"


enum policy_types {
   RESTART,
   SKIP_STEPS_N,
   SKIP_STEPS_FREQ,
   NO_SKIP_STEPS,
   CHECKPOINT_N,
   CHECKPOINT_FREQ,
   NO_CHECKPOINT,
   COMPRESS,
   NO_COMPRESS,
   DIST_RR,
   NO_RR,
};


class InfoManager {

   private:
   std::string stream;

  
   /* Map used to identify reader from reader task name. This is used to  process request from arbitration 
    *    key: reader task name
    *    value: reader unique ID (port number)  */ 

   /* Map used to identify reader settings from reader ID. This is used to  process request from reader 
    *    key: reader unique ID
    *    value: reader settings */
   std::unordered_map<std::string, StreamProperties*> connections; 

   struct global_properties_t global_prop; 

   int writerId; 

   std::string  writerStr;

   void reset_prop();
   
   public:
   InfoManager(std::string); 
   bool register_reader(std::string); 
   bool register_writer(std::string); 

   bool is_registered(std::string, int); 
   bool register_connection(std::string, int);
   bool deregister_connection(std::string, int);
   int begin_step(std::string, int);
   int end_step(std::string, int);
   int get_var(std::string, int, std::string);
   int put_var(std::string, int, std::string, std::string&);
   void set_policy(std::string stream_v, int policy, std::vector<std::string> &);

   StreamProperties* get_settings_for(int);
   std::string get_stream();
}; 



#endif
