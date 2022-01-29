#include "stream_prop.hpp"


StreamProperties::StreamProperties( struct global_properties_t *props, int rid, bool is_writer) : global_props(props), writer(is_writer)  {
    turn = 0;
    round_robin = false;
    global_id = rid;
    nreaders = 0;
}  


void StreamProperties::register_connection(int pid) {
   conn_ids.push_back(pid);
}

bool StreamProperties::is_registered(int pid) {
   for (std::vector<int>::iterator it = conn_ids.begin() ; it != conn_ids.end(); ++it) {
      if ( *it == pid ) { 
           return true;
      }   
    }   
    return false;
}

void StreamProperties::deregister_connection(int pid) {
  std::vector<int>::iterator it;
  for (it = conn_ids.begin() ; it != conn_ids.end(); ++it) {
    if ( *it == pid ) {
          conn_ids.erase(it);
          return;
    } 
  }
  
}

void StreamProperties::open() {
    if ( !writer) {
        nreaders ++;  
        if ( !reconnects) {
            if (nreaders == 1) {
                global_props->nreaders ++;
            }
        } else {
            reconnects = false;
        }
    } 
}

int StreamProperties::close() {
    if (writer) {
       if ( global_props->begin_cntr == global_props->end_cntr) {
           return 1; //global_props->allow_close;
       } else if ( global_props->end_cntr == global_props->nreaders) {
           return 1; //global_props->allow_close;
       }
       return -1;
    }
    nreaders --; 
    if ( !reconnects && nreaders == 0 ) {
        global_props->nreaders --;
    } 

    return 1;   
}


int StreamProperties::begin_step(int id) {
    /* if( writer || noskip_step ) {
        return global_props->begin_step;
    }
    if( nskip_cntr == 0) {
        return true;
    } */

    global_props->begin_cntr++;
    begin_cntr++;
     
    return read_var(id, "");
}

bool StreamProperties::end_step(int id) {
    if (writer) {
       if ( global_props->disk_write && global_props->checkpoint_cntr ) {
           global_props->checkpoint_cntr = global_props->max_checkpoint;
        } else {
           global_props->checkpoint_cntr = 0;
        }  
        return global_props->end_step; 
    }

    if( !noskip_step && nskip_cntr == 0) {
        nskip_cntr = max_skip;
    }  

    global_props->end_cntr++;   

    if ( global_props->round_robin ) {
          global_props->turn = (global_props->turn + 1) % global_props->nreaders;   
    }

    if ( round_robin ) {
        turn = (turn + 1) % conn_ids.size();
    } 

    if ( global_props->end_cntr == global_props->nreaders) {
        global_props->begin_cntr = 0;
        global_props->end_cntr = 0;
    }
   
    return true; 
}




int StreamProperties::read_var(int id, std::string var) {
    if( !noskip_step && nskip_cntr != 0) {
       return 0; //skip
    }
   
    if ( round_robin ) {
       if ( conn_ids[turn] != id ) {
           return 0;
       }
    }

    if ( global_props->round_robin ) {
        if (global_id != global_props->turn) {
            return -1; //try again
        }
    } 
    return 1; //good to go
}

int StreamProperties::write_var(int id, std::string var, std::string &comp_params) {
   if (if_checkpoint()) {
        return 2;
   } else if ( global_props->compress_var.find(var) != global_props->compress_var.end()) {
       std::vector<std::string> params = global_props->compress_var[var];
       std::string cpms;
       for ( auto cntr = params.begin(); cntr != params.end(); cntr ++) {
           cpms += *cntr + ",";
       }
       comp_params = cpms; //.substr(0, cpms.length() - 1);
       return 1;
   } else {
        return 0;
   }
}

bool StreamProperties::compress_var(std::string var, std::vector<std::string> params) {
     global_props->compress_var[var] = params;
     return true;
}


bool StreamProperties::if_checkpoint() {
    if ( global_props->if_checkpoint && global_props->checkpoint_cntr != 0) {
        return true;
    }
    return false;
}

void StreamProperties::set_reconnects(bool rc) {
    reconnects = rc;
}

void StreamProperties::set_noskip(bool nskip) {
    noskip_step = nskip;
}

void  StreamProperties::set_skip(int nskip, int m_skip) {
    max_skip = m_skip;
    nskip_cntr = nskip;
}

void  StreamProperties::set_RR(bool rr) {
    round_robin = rr; 
} 









