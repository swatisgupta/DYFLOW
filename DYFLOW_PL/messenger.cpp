#include "messenger.hpp"
#include <sstream>
#include <bits/stdc++.h>
#include <iostream>

void Messenger::set_stream(std::string str) {
    stream = str;
}


void Messenger::decode_tag(SENDER_TYPE &st, int &seq, std::string &in_stream) {
    std::size_t found = tag.find(":");
    std::string tagx = tag.substr(0, found);
    
    found = tag.find(",");
    std::string nexttag;
    if (found != std::string::npos) {
          st =  (SENDER_TYPE) atoi(tagx.substr(0, found).c_str());
          nexttag =  tagx.substr(found + 1).c_str();
    }
    found = nexttag.find (",");
    if (found != std::string::npos) {
          seq =  atoi(nexttag.substr(0, found).c_str());
          in_stream = nexttag.substr(found + 1).c_str();
        
    }
    DEBUG("After decoding tag :" +  std::to_string(st) + "," + std::to_string(seq) + "," + in_stream) 
}

void Messenger::create_socket ( ) {

    switch(socket_t) {
        case SOCKET_TYPE::SUB: 
             this->socket = new zmq::socket_t (context, zmq::socket_type::sub);
             this->socket->connect(ip_address);
             break;
        case SOCKET_TYPE::CLIENT: 
             this->socket = new zmq::socket_t (context, ZMQ_REQ);
             this->socket->connect(ip_address);
             DEBUG("Connected to : " + ip_address)
             break;
        case SOCKET_TYPE::PUB: 
             this->socket = new zmq::socket_t (context, zmq::socket_type::pub);
             this->socket->bind(ip_address);
             break;
        case SOCKET_TYPE::SERVER: 
             this->socket = new zmq::socket_t (context, ZMQ_REP);
             this->socket->bind(ip_address);
             DEBUG("Listening to : " + ip_address)
             break;
    }   
    
}


Messenger::Messenger(SOCKET_TYPE st, SENDER_TYPE mt, const std::string ip_addr): sender_t(mt), socket_t(st), ip_address(ip_addr), seq(0) {
    create_socket();
}

std::string Messenger::set_msg (std::string request) {
    if ( sender_t != SENDER_TYPE::INTERM ) { 
        seq = (seq + 1) % INT_MAX;
        std::stringstream ss;
        ss << sender_t << "," << seq << "," << stream << ":";
        tag = ss.str(); // (sender_t) + "," + seq + "," +  stream + ":";
    }
    return tag + request; 
}


std::string Messenger::decode_msg (std::string msg) {
    std::size_t found = msg.find (":");
    if (found != std::string::npos) {
          std::string msgtag =  msg.substr(0, found + 1);
          if ( sender_t != SENDER_TYPE::INTERM ) {
              if ( tag.compare(msgtag) ) { 
                   return  nullptr;
              }   
          } else {
             tag = msgtag;
          }   
          return msg.substr (found + 1) ;                                                                                                                              
    }  
    return nullptr; 
}

bool Messenger::send_msg(std::string request) {
    std::string message = set_msg(request); 
    DEBUG("Sending : " +  message)
    socket->send (zmq::buffer(message), zmq::send_flags::dontwait);
    return true;
}

std::string Messenger::receive_msg() {
   zmq::message_t msg;
   socket->recv(msg);
   DEBUG("Received : " + msg.to_string() )
   return decode_msg (msg.to_string());
}

void Messenger::close() {
  socket->close();
  context.close(); 
}
