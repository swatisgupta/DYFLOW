#include "data_manager.hpp"



DataManager::DataManager(std::string str):stream(str), writerId(-1), writerStr(""), connections() {
} 

bool DataManager::register_reader(std::string str) {
    auto ele = connections.find(str);
    if ( ele == connections.end() ) {
        global_prop.nreaders++;
        StreamProperties* new_conn = new StreamProperties(&global_prop, global_prop.nreaders, false); 
        connections[str] = new_conn;
    }

    DEBUG("Registering" + str)
    DEBUG("Results:")

#ifndef NODEBUG
    for(auto x : connections){
		DEBUG(x.first)
		//DEBUG(x.second)
    }
#endif
    return true;
}

bool DataManager::is_registered(std::string str, int id) {
     auto ele = connections.find(str);
     if ( ele != connections.end() ) {
         return ele->second->is_registered(id); 
     }
     return false;
}

bool DataManager::register_writer(std::string str) {
    auto ele = connections.find(str);
    if ( ele == connections.end() ) {
        StreamProperties* writer = new StreamProperties(&global_prop, 0, true);
        connections[str] = writer;
    }
    return true;
}

bool DataManager::register_connection(std::string str, int id) {

    bool ret = true;
 
    auto ele = connections.find(str);
    if ( ele != connections.end() ) {
        ele->second->register_connection(id);
    }
    return ret; 
}

std::string DataManager::get_stream() {
    return stream;
}
 
bool DataManager::deregister_connection(std::string str, int id) {

    bool ret = false, found = false;
    int pos = -1;
 
    auto ele = connections.find(str);
    if ( ele != connections.end() ) { 
        ele->second->deregister_connection(id);
    }
    return true;
}

void DataManager::reset_prop() {
     global_prop.compress_var.clear();
     global_prop.disk_write = false;
     global_prop.if_close = true;
     global_prop.wait = 0;
     global_prop.nreaders = 0;
     global_prop.begin_cntr = 0;
     global_prop.end_cntr = 0;
}


void DataManager::set_policy(std::string stream_v, int policy, std::vector<std::string>& params ) {
     
     int n;
    
     switch (policy) {

       case RESTART:
                   connections[stream_v]->set_reconnects(true);
                   break;      

       case SKIP_STEPS_N:
                   connections[stream_v]->set_noskip(false);
                   n = atoi(params.front().c_str()); 
                   connections[stream_v]->set_skip(0, n);
                   break;      

       case SKIP_STEPS_FREQ:
                   connections[stream_v]->set_noskip(false);
                   n = atoi(params.front().c_str()); 
                   connections[stream_v]->set_skip(n, n);
                   break;      

       case CHECKPOINT_N:
                   global_prop.if_checkpoint= true;
                   n = atoi(params.front().c_str()); 
                   global_prop.checkpoint_cntr = n;
                   global_prop.max_checkpoint= 0;
                   break;      

       case CHECKPOINT_FREQ:
                   global_prop.if_checkpoint= true;
                   n = atoi(params.front().c_str()); 
                   global_prop.checkpoint_cntr = n;
                   global_prop.max_checkpoint = n;
                   break;      
       case NO_CHECKPOINT:
                   global_prop.if_checkpoint= false;
                   global_prop.checkpoint_cntr = 0;
                   global_prop.max_checkpoint = 0;
                   break;      
       case COMPRESS:
                   global_prop.compress_var[stream_v] = params;
                   break;      
       case NO_COMPRESS:
                   global_prop.compress_var.erase(stream_v);
                   break;      
       case DIST_RR:
                  if(connections.find(stream) != connections.end()) {
                     connections[stream]->set_RR(true); //round robin for specific reader
                  } else {
                     //round robin for all connections
                     global_prop.round_robin = true; //round robin for specific reader
                  }
                  break;
       case NO_RR:
                  if(connections.find(stream) != connections.end()) {
                     //round robin for specific reader
                     connections[stream]->set_RR(false); //round robin for specific reader
                  } else {
                     //round robin for all connections
                     global_prop.round_robin = false; //round robin for specific reader
                  }
                  break;
       default: 
              reset_prop();
     }
}
