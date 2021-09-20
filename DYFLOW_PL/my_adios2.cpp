
#include <stdlib.h>
#include <stdio.h>
#include <execinfo.h>
#include <unordered_map>
#include <dlfcn.h>

#include <adios2.h>
#include <string.h>
#include "streamer.hpp"


//std::unordered_map <std::string, Streamer*> streamer_map;  
MPI_Comm comm;

Streamer* conn_streamer (std::string stream) {
  Streamer *istream = nullptr;
  istream = new Streamer(stream);      
  return istream;  
}

#ifdef ADIOS2_USE_MPI
Streamer* conn_streamer ( std::string stream, MPI_Comm comm) {
  Streamer *istream = nullptr;
  int rank;
  MPI_Comm_rank(comm, &rank);
  istream = new Streamer(rank, comm, stream);      
  return istream;  
}
#endif 


adios2::Engine adios2::IO::Open(const std::string &name, const adios2::Mode mode) {

  std::cout<< "Intercepted adios2::IO::Open(const std::string &name, const adios2::Mode mode)" <<std::endl;
  bool reader = false;
  if ( mode == adios2::Mode::Read) {
      reader = true;
  }
#ifdef ADIOS2_USE_MPI
  Streamer* strm = conn_streamer (name, MPI_COMM_WORLD);
#else
  Streamer* strm = conn_streamer (name);
#endif 

  strm->open(); 

  typedef adios2::Engine (adios2::IO::*methodType)(const std::string, const adios2::Mode);

  std::string name1 = name; 
  adios2::Mode md = mode;

  std::cout<< "String:" << name1 << ", Mode:" << md << std::endl;
  static methodType origMethod = nullptr;

  if (origMethod == nullptr)
  {
    //origMethod = *(methodType*)dlsym(RTLD_NEXT, "adios2::IO::Open(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, adios2::Mode)");
    auto origMethodPtr = dlsym(RTLD_NEXT, "_ZN6adios22IO4OpenERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEENS_4ModeE");
    *reinterpret_cast<void**>(&origMethod) = origMethodPtr;
    //memcpy(&origMethod, &tmpPtr, sizeof(&tmpPtr));
  }
 // adios2::Engine *eng1 = &eng; 
  delete strm;

  DEBUG("Registered for " + name);
  return (this->*origMethod)(name, mode);
} 

#if ADIOS2_USE_MPI
adios2::Engine adios2::IO::Open(const std::string &name, const Mode mode, MPI_Comm comm) {

  std::cout<< "Intercepted adios2::IO::Open(const std::string &name, const adios2::Mode mode, MPI_Comm comm)" <<std::endl;

  typedef adios2::Engine (adios2::IO::*methodType)(const std::string, const adios2::Mode, MPI_Comm);

  bool reader = false;
  if ( mode == adios2::Mode::Read) {
      reader = true;
  }

  Streamer* strm = conn_streamer ( name, comm );

  strm->open();

  static methodType origMethod = nullptr;
  
  if (origMethod == nullptr)
  { 
    //origMethod = *(methodType*)dlsym(RTLD_NEXT, "adios2::IO::Open(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, adios2::Mode, ompi_communicator_t*)");
    auto origMethodPtr = dlsym(RTLD_NEXT, "_ZN6adios22IO4OpenERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEENS_4ModeEP19ompi_communicator_t");
    *reinterpret_cast<void**>(&origMethod) = origMethodPtr;
  }

  delete strm;
  return (this->*origMethod)(name, mode, comm);
}
#endif


adios2::StepStatus adios2::Engine::BeginStep() {

  std::cout<< "Intercepted adios2::Engine::BeginStep()" << std::endl;

#ifdef ADIOS2_USE_MPI
  Streamer* strm = conn_streamer (this->Name(), MPI_COMM_WORLD);
#else
  Streamer* strm = conn_streamer (this->Name());
#endif 

  typedef adios2::StepStatus (adios2::Engine::*methodType)();

  static methodType origMethod = nullptr;
  
  if (origMethod == nullptr)
  { 
    auto origMethodPtr = dlsym(RTLD_NEXT, "_ZN6adios26Engine9BeginStepEv");
    *reinterpret_cast<void**>(&origMethod) = origMethodPtr;
  
  }
  return (this->*origMethod)();
}

adios2::StepStatus adios2::Engine::BeginStep(const adios2::StepMode mode, const float timeoutSeconds)  {

  std::cout<< "Intercepted adios2::Engine::BeginStep(const adios2::StepMode mode, const float timeoutSeconds)" <<std::endl;
#ifdef ADIOS2_USE_MPI
  Streamer* strm = conn_streamer (this->Name(), MPI_COMM_WORLD);
#else
  Streamer* strm = conn_streamer (this->Name());
#endif 

  typedef adios2::StepStatus (adios2::Engine::*methodType)(const adios2::StepMode, const float);

  static methodType origMethod = nullptr;
  
  if (origMethod == nullptr)
  { 
    auto origMethodPtr = dlsym(RTLD_NEXT, "_ZN6adios26Engine9BeginStepENS_8StepModeEf");
    *reinterpret_cast<void**>(&origMethod) = origMethodPtr;
  }
  return (this->*origMethod)(mode, timeoutSeconds);
}


void adios2::Engine::EndStep() {

  std::cout<< "Intercepted adios2::Engine::EndStep()" << std::endl;
#ifdef ADIOS2_USE_MPI
  Streamer* strm = conn_streamer (this->Name(), MPI_COMM_WORLD);
#else
  Streamer* strm = conn_streamer (this->Name());
#endif 

  typedef void (adios2::Engine::*methodType)();

  static methodType origMethod = nullptr;
  
  if (origMethod == nullptr)
  { 
    auto origMethodPtr = dlsym(RTLD_NEXT, "_ZN6adios26Engine7EndStepEv");
    *reinterpret_cast<void**>(&origMethod) = origMethodPtr;
  
  }
  delete strm;  
  return (this->*origMethod)();
}


void adios2::Engine::Close(const int transportIndex) {

  std::cout<< "Intercepted adios2::Engine::Close(int)" << std::endl;
#ifdef ADIOS2_USE_MPI
  Streamer* strm = conn_streamer (this->Name(), MPI_COMM_WORLD);
#else
  Streamer* strm = conn_streamer (this->Name());
#endif 

  while (!strm->close());

  typedef void (adios2::Engine::*methodType)(const int);

  static methodType origMethod = nullptr;
  
  if (origMethod == nullptr)
  { 
    auto origMethodPtr = dlsym(RTLD_NEXT, "_ZN6adios26Engine5CloseEi");
    *reinterpret_cast<void**>(&origMethod) = origMethodPtr;
  
  }
  delete strm;  

  return (this->*origMethod)(transportIndex);
}

