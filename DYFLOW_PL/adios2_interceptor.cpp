
#include <stdlib.h>
#include <stdio.h>
#include <execinfo.h>
#include <unordered_map>
#include <dlfcn.h>

#include "adios2/core/IO.h"
#include "adios2/core/Engine.h"
#include "adios2/core/Variable.h"
#include "adios2/helper/adiosCommMPI.h"
#include "adios2.h"
#include <string.h>
#include "stream_actuator.hpp"


//std::unordered_map <std::string, StreamActuator*> streamer_map;  
MPI_Comm comm;

StreamActuator* conn_streamer (std::string stream) {
  StreamActuator *istream = nullptr;
  istream = new StreamActuator(stream);      
  return istream;  
}

#ifdef ADIOS2_USE_MPI
StreamActuator* conn_streamer ( std::string stream, MPI_Comm comm) {
  StreamActuator *istream = nullptr;
  int rank;
  MPI_Comm_rank(comm, &rank);
  istream = new StreamActuator(rank, comm, stream);      
  return istream;  
}
#endif 

template <class T>
void checkpoint_data(int rank, std::string var, int step, const T* data, adios2::Dims ndims, adios2::Box<adios2::Dims> ldims ) { 
    try 
    {   
       std::cout << "Before declare " << std::endl << std::flush; 
   
       #ifdef ADIOS2_USE_MPI 
          adios2::ADIOS adios(MPI_COMM_WORLD, true);
       #else
          adios2::ADIOS adios(true);
       #endif
       adios2::IO io = adios.DeclareIO("Checkpoint");
       std::cout << "After declare " << std::endl << std::flush; 

       adios2::Variable<T> varArray = io.DefineVariable<T>(var, ndims);
       std::cout << "After defineVar " << std::endl << std::flush; 

       std::string fname = var + std::to_string(step) + ".bp";
       adios2::Engine writer = io.Open(fname, adios2::Mode::Write);
       std::cout << "After open " << std::endl << std::flush; 

       varArray.SetSelection(ldims);

       writer.Put<T>(varArray, data);

       writer.Close();
    }   
    catch (std::invalid_argument &e) 
    {   
        if (rank == 0)
        {   
            std::cout << "Invalid argument exception, STOPPING PROGRAM\n";
            std::cout << e.what() << "\n";
        }   
    }   
    catch (std::ios_base::failure &e) 
    {   
        if (rank == 0)
        {   
            std::cout << "System exception, STOPPING PROGRAM\n";
            std::cout << e.what() << "\n";
        }   
    }   
    catch (std::exception &e) 
    {   
        if (rank == 0)
        {   
            std::cout << "Exception, STOPPING PROGRAM\n";
            std::cout << e.what() << "\n";
        }   
    }   
}



adios2::core::Engine& adios2::core::IO::Open(const std::string &name, const adios2::Mode mode) {

  std::cout<< "Intercepted adios2::IO::Open(const std::string &name, const adios2::Mode mode)" <<std::endl;
  bool reader = false;
  if ( mode == adios2::Mode::Read) {
      reader = true;
  }
#ifdef ADIOS2_USE_MPI
  StreamActuator* strm = conn_streamer (name, MPI_COMM_WORLD);
#else
  StreamActuator* strm = conn_streamer (name);
#endif 

  strm->open(); 

  typedef adios2::core::Engine& (adios2::core::IO::*methodType)(const std::string, const adios2::Mode);

  std::cout<< "String:" << name << ", adios2::Mode:" << mode << std::endl;
  static methodType origMethod = nullptr;

  if (origMethod == nullptr)
  {
    auto origMethodPtr = dlsym(RTLD_NEXT, "_ZN6adios24core2IO4OpenERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEENS_4ModeE");
    *reinterpret_cast<void**>(&origMethod) = origMethodPtr;
  }
  delete strm;

  DEBUG("Registered for " + name);
  return (this->*origMethod)(name, mode);
} 

#if ADIOS2_USE_MPI
adios2::core::Engine& adios2::core::IO::Open(const std::string &name, const adios2::Mode mode, adios2::helper::Comm comm) {

  std::cout<< "Intercepted adios2::IO::Open(const std::string &name, const adios2::Mode mode, MPI_Comm comm)" <<std::endl;

  //const char* adios2_dir = std::getenv("ADIOS2_DIR");
  //std::stringstream adios2_lib;
  //adios2_lib << adios2_dir << "/lib64/libadios2_cxx11.so"; 
  //void* adios_handle = dlopen(adios2_lib.str().c_str(), RTLD_NOW);

  typedef adios2::core::Engine& (adios2::core::IO::*methodType)(const std::string, const adios2::Mode, adios2::helper::Comm);

  bool reader = false;
  if ( mode == adios2::Mode::Read) {
      reader = true;
  }

  StreamActuator* strm = conn_streamer ( name, MPI_COMM_WORLD );

  strm->open();

  static methodType origMethod = nullptr;
  
  if (origMethod == nullptr)
  { 
    //origMethod = *(methodType*)dlsym(RTLD_NEXT, "adios2::IO::Open(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, adios2::Mode, ompi_communicator_t*)");
    auto origMethodPtr = dlsym(RTLD_NEXT, "_ZN6adios24core2IO4OpenERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEENS_4ModeENS_6helper4CommE");
    *reinterpret_cast<void**>(&origMethod) = origMethodPtr;
  }

  delete strm;
  return (this->*origMethod)(name, mode, adios2::helper::CommDupMPI(adios2::helper::CommAsMPI(comm)));
}
#endif


adios2::StepStatus adios2::core::Engine::BeginStep() {

  std::cout<< "Intercepted adios2::core::Engine::BeginStep()" << std::endl << std::flush;

#ifdef ADIOS2_USE_MPI
  StreamActuator* strm = conn_streamer (this->m_Name, MPI_COMM_WORLD);
#else
  StreamActuator* strm = conn_streamer (this->m_Name);
#endif 

  typedef adios2::StepStatus (adios2::core::Engine::*methodType)();

  static methodType origMethod = nullptr;
  
  if (origMethod == nullptr)
  { 
    auto origMethodPtr = dlsym(RTLD_NEXT, "_ZN6adios24core6Engine9BeginStepEv");
    *reinterpret_cast<void**>(&origMethod) = origMethodPtr;
  
  }
  return (this->*origMethod)();
}

adios2::StepStatus adios2::core::Engine::BeginStep(const adios2::StepMode mode, const float timeoutSeconds)  {

  std::cout<< "Intercepted adios2::core::Engine::BeginStep(const adios2::StepMode mode, const float timeoutSeconds)" <<std::endl << std::flush;
#ifdef ADIOS2_USE_MPI
  StreamActuator* strm = conn_streamer (this->m_Name, MPI_COMM_WORLD);
#else
  StreamActuator* strm = conn_streamer (this->m_Name);
#endif 

  typedef adios2::StepStatus (adios2::core::Engine::*methodType)(const adios2::StepMode, const float);
  typedef void (adios2::core::Engine::*methodType1)();
  static methodType origMethod = nullptr;
  adios2::StepStatus status;
  bool ret = false;

  while ( ret == false ) { 
      ret = strm->begin();
      if (origMethod == nullptr)
      { 
         auto origMethodPtr = dlsym(RTLD_NEXT, "_ZN6adios24core6Engine9BeginStepENS_8StepModeEf");
         *reinterpret_cast<void**>(&origMethod) = origMethodPtr;
      }
      status = (this->*origMethod)(mode, timeoutSeconds);
      if ( status != adios2::StepStatus::OK ) {
          break;
      } else if ( ret == false )  {

          
         /* methodType origMethodE = nullptr;
         auto origMethodPtrE = dlsym(RTLD_NEXT, "_ZN6adios26Engine7EndStepEv");
         *reinterpret_cast<void**>(&origMethodE) = origMethodPtrE;
         (this->*origMethodE)(); */
     }
  }
  return status; //(this->*origMethod)(mode, timeoutSeconds);
}


void adios2::core::Engine::EndStep() {

  std::cout<< "Intercepted adios2::core::Engine::EndStep()" << std::endl << std::flush;
#ifdef ADIOS2_USE_MPI
  StreamActuator* strm = conn_streamer (this->m_Name, MPI_COMM_WORLD);
#else
  StreamActuator* strm = conn_streamer (this->m_Name);
#endif 

  typedef void (adios2::core::Engine::*methodType)();

  static methodType origMethod = nullptr;
  
  if (origMethod == nullptr)
  { 
    auto origMethodPtr = dlsym(RTLD_NEXT, "_ZN6adios24core6Engine7EndStepEv");
    *reinterpret_cast<void**>(&origMethod) = origMethodPtr;
  
  }
  delete strm;  
  return (this->*origMethod)();
}


void adios2::core::Engine::Close(const int transportIndex) {

  std::cout<< "Intercepted adios2::core::Engine::Close(int)" << std::endl << std::flush;
#ifdef ADIOS2_USE_MPI
  StreamActuator* strm = conn_streamer (this->m_Name, MPI_COMM_WORLD);
#else
  StreamActuator* strm = conn_streamer (this->m_Name);
#endif 

  while (!strm->close());

  typedef void (adios2::core::Engine::*methodType)(const int);

  static methodType origMethod = nullptr;
  
  if (origMethod == nullptr)
  { 
    auto origMethodPtr = dlsym(RTLD_NEXT, "_ZN6adios24core6Engine5CloseEi");
    *reinterpret_cast<void**>(&origMethod) = origMethodPtr;
  
  }
  delete strm;  

  return (this->*origMethod)(transportIndex);
}


template <class T>
  void adios2::core::Engine::Put(adios2::core::Variable<T> &variable, const T *data, const adios2::Mode launch) { 
  std::cout<< "Intercepted adios2::core::Engine::Put(const std::string &variableName, const T &datum, const adios2::Mode launch)" << std::endl << std::flush;
  typedef void (adios2::core::Engine::*methodType)(adios2::core::Variable<T> &, const T *, const adios2::Mode);

  static methodType origMethod = nullptr;
  int rank = 0;
  int step; 
#ifdef ADIOS2_USE_MPI
  StreamActuator* strm = conn_streamer (this->m_Name, MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank); 
#else
  StreamActuator* strm = conn_streamer (this->m_Name);
#endif 
  std::string params;
  //adios2::Params prms(); // = new adios2::Params(); 
  std::string op;
  std::string key;
  std::string value; 
 
  int ret = strm->put(variable.m_Name, params);
  if ( ret == 1 ) {
    variable.RemoveOperations();

    char *token = strtok((char*)params.c_str(), ":");
    if ( token != NULL ) {
        op = token; // new adios2::core::Operation();
    } 
    int i = 1;
    token = strtok(NULL, "-");
    while (token != NULL)
    {
        if ( i % 2 == 0 ) {
            value = token;
        } else {
            key = token;
        }
        token = strtok(NULL, "-");
        i++;
    }
    adios2::core::Operator &opern = m_IO.m_ADIOS.DefineOperator("compress", op);
    variable.AddOperation(opern, {{key, value}});
  } else if ( ret == 0 ) {
    variable.RemoveOperations();
  } else if (ret == 2 ) {
    checkpoint_data<T>(rank, variable.m_Name, CurrentStep(), data, variable.m_Shape, adios2::Box<adios2::Dims>(variable.m_Start, variable.m_Count));
  }

  if (origMethod == nullptr)
  { 
    std::string tname = typeid(T).name();
    std::string symbol_nm = "_ZN6adios24core6Engine3PutI"+ tname  +"EEvRNS0_8VariableIT_EEPKS4_NS_4ModeE"; 
    auto origMethodPtr = dlsym(RTLD_NEXT, symbol_nm.c_str());
    *reinterpret_cast<void**>(&origMethod) = origMethodPtr;
  }

  return (this->*origMethod)(variable, data, launch);
}




template <class T>
void adios2::core::Engine::Get(adios2::core::Variable<T> & variable,  T *data, const adios2::Mode launch) {
  std::cout<< "Intercepted adios2::core::Engine::Get(adios2::core::Variable<T> variable, const T *data, const adios2::Mode launch)" << std::endl << std::flush;
  typedef void (adios2::core::Engine::*methodType)(adios2::core::Variable<T> &, T *, const adios2::Mode);

  static methodType origMethod = nullptr;

#ifdef ADIOS2_USE_MPI
  StreamActuator* strm = conn_streamer (this->m_Name, MPI_COMM_WORLD);
#else
  StreamActuator* strm = conn_streamer (this->m_Name);
#endif 

  strm->get(variable.m_Name);

  if (origMethod == nullptr)
  { 
    std::string tname = typeid(T).name();
    std::string symbol_nm = "_ZN6adios24core6Engine3GetI" + tname + "EEvRNS0_8VariableIT_EEPS4_NS_4ModeE";
    auto origMethodPtr = dlsym(RTLD_NEXT, symbol_nm.c_str());
    *reinterpret_cast<void**>(&origMethod) = origMethodPtr;
  }

  return (this->*origMethod)(variable, data, launch);
}


/** 
 *
size_t adios2::core::VariableBase::AddOperation(adios2::core::Operator &op,
                                  const adios2::Params &parameters) noexcept
{
  std::cout<< "Intercepted adios2::core::VariableBase::AddOperation" << std::endl << std::flush;
  typedef size_t (adios2::core::VariableBase::*methodType)(adios2::core::Operator &, const adios2::Params &);

  static methodType origMethod = nullptr;
  if (origMethod == nullptr)
  { 
    std::string symbol_nm = "_ZN6adios24core12VariableBase12AddOperationERNS0_8OperatorERKSt3mapINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEESA_St4lessISA_ESaISt4pairIKSA_SA_EEE";
    auto origMethodPtr = dlsym(RTLD_NEXT, symbol_nm.c_str());
    *reinterpret_cast<void**>(&origMethod) = origMethodPtr;
  }
  return (this->*origMethod)(op, parameters);
}

void adios2::core::VariableBase::RemoveOperations() noexcept { 
  std::cout<< "Intercepted adios2::core::VariableBase::RemoveOperation" << std::endl << std::flush;
  typedef void (adios2::core::VariableBase::*methodType)();

  static methodType origMethod = nullptr;  
  if (origMethod == nullptr)
  { 
    std::string symbol_nm = "_ZN6adios24core12VariableBase16RemoveOperationsEv";
    auto origMethodPtr = dlsym(RTLD_NEXT, symbol_nm.c_str());
    *reinterpret_cast<void**>(&origMethod) = origMethodPtr;
  }
  (this->*origMethod)();
}

adios2::core::Operator & adios2::core::ADIOS::DefineOperator(const std::string &name, const std::string type, const Params &parameters) {
  std::cout<< "Intercepted adios2::core::ADIOS::DefineOperator" << std::endl << std::flush;
  typedef  adios2::core::Operator& (adios2::core::ADIOS::*methodType)(const std::string&, const std::string, const Params&);

  static methodType origMethod = nullptr;  
  if (origMethod == nullptr)
  { 
    std::string symbol_nm = "_ZN6adios24core5ADIOS14DefineOperatorERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES7_RKSt3mapIS7_S7_St4lessIS7_ESaISt4pairIS8_S7_EEE";

    auto origMethodPtr = dlsym(RTLD_NEXT, symbol_nm.c_str());
    *reinterpret_cast<void**>(&origMethod) = origMethodPtr;
  }
  return (this->*origMethod)(name, type, parameters);
}
**/

#define declare_template_instantiation(T)                                      \
                                                                               \
    template void adios2::core::Engine::Put<T>(adios2::core::Variable<T> &, const T *, const adios2::Mode);        \
                                                                               \
    template void adios2::core::Engine::Get<T>(adios2::core::Variable<T> &, T *, const adios2::Mode);              \

/*
    template typename adios2::core::Variable<T>::BPInfo *Engine::Get<T>(adios2::core::Variable<T> &,       \
                                                          const adios2::Mode);         \
    template typename adios2::core::Variable<T>::BPInfo *Engine::Get<T>(const std::string &, \
                                                          const adios2::Mode);         \
                                                                               \
*/

ADIOS2_FOREACH_STDTYPE_1ARG(declare_template_instantiation)
#undef declare_template_instantiation
