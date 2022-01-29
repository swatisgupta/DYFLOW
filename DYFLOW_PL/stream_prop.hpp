#ifndef STREAM_PROPS_H
#define STREAM_PROPS_H
#include <unordered_map>
#include <list>
#include <vector>

struct global_properties_t {

   std::unordered_map<std::string, std::vector<std::string>>compress_var;

   bool disk_write;

   int checkpoint_cntr;
   bool if_checkpoint;
   int max_checkpoint;
   
   bool if_close;

   bool begin_step;  
   bool end_step;  

   bool write_next;  
   bool read_next;  

   int wait;
   int steps_comp;

   int begin_cntr;
   int end_cntr;

   bool round_robin;
   int turn;
   int nreaders;
}; 

class StreamProperties {

   public:
   StreamProperties(global_properties_t *, int rid, bool); 

   int close();
   void open();

   int begin_step(int);
   bool end_step(int);

   int read_var(int, std::string);
   int write_var(int, std::string, std::string&);
   bool compress_var(std::string, std::vector<std::string> params);
   bool if_checkpoint();
   void register_connection(int pid);
   void deregister_connection(int pid);
   bool is_registered(int pid);
   void set_reconnects(bool);
   void set_noskip(bool);
   void set_skip(int, int);
   void set_RR(bool);

   private:

   struct global_properties_t *global_props;
   std::vector<int> conn_ids;
 
   bool writer;
   int nreaders;

   bool noskip_step;
   int nskip_cntr;
   int max_skip;

   bool reconnects;
   int begin_cntr;
   int end_cntr;

   int global_id;
   bool round_robin;
   int turn;
};

#endif
