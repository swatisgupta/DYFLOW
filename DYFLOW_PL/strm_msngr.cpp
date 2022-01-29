#include <pybind11/pybind11.h>
#include "stream_messenger.hpp"

namespace py = pybind11;

PYBIND11_MODULE(strm_msngr, m) {
    py::class_<StreamMessenger> strm(m, "StreamMessenger");

    strm.def(py::init<SOCKET_TYPE, SENDER_TYPE, std::string>())
        .def(py::init()) 
        .def("set_stream", &StreamMessenger::set_stream)
        .def("send_msg", &StreamMessenger::send_msg)
        .def("receive_msg", &StreamMessenger::receive_msg)
        .def("set_msg", &StreamMessenger::set_msg)
        .def("decode_msg", &StreamMessenger::decode_msg)
        .def("decode_tag", &StreamMessenger::decode_tag)
        .def("close", &StreamMessenger::close);


    py::enum_<SOCKET_TYPE>(m, "SOCKET_TYPE")
       .value("CLIENT", SOCKET_TYPE::CLIENT)
       .value("SERVER", SOCKET_TYPE::SERVER)
       .value("PUB", SOCKET_TYPE::PUB)
       .value("SUB", SOCKET_TYPE::SUB)
       .export_values();

    py::enum_<SENDER_TYPE>(m, "SENDER_TYPE")
       .value("INTERM", SENDER_TYPE::INTERM)
       .value("ARBITRATOR", SENDER_TYPE::ARBITRATOR)
       .value("WRITER", SENDER_TYPE::WRITER)
       .value("READER", SENDER_TYPE::READER)
       .value("RDWR", SENDER_TYPE::RDWR)
       .export_values();
  
}
