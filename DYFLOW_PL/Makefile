CXX=mpic++

CXXFLAGS=-fPIC -shared -std=c++11 -DADIOS2_USE_MPI -I/homes/ssinghal/Downloads/particleAnalysis/ADIOS2/source -I/lustre/ssinghal/ADIOS2-install/include -I/lustre/ssinghal/json-install/include -I/lustre/ssinghal/zmqcpp-install/include -I/lustre/ssinghal/zmq-install/include
CXXFLAGS1=-g -std=c++11 -DADIOS2_USE_MPI -I/lustre/ssinghal/ADIOS2-install/include -I/lustre/ssinghal/json-install/include -I/lustre/ssinghal/zmqcpp-install/include -I/lustre/ssinghal/zmq-install/include

LDFLAGS1=-rdynamic -ldl -L/lustre/ssinghal/zmq-install/lib64 -lzmq -lstdc++ 
LDFLAGS2=-rdynamic -ldl -lstdc++ -Wl,-rpath,/lustre/ssinghal/ADIOS2-install/lib64 /lustre/ssinghal/ADIOS2-install/lib64/libadios2_cxx11_mpi.so.2.7.1 /lustre/ssinghal/ADIOS2-install/lib64/libadios2_cxx11.so.2.7.1 -Wl,-rpath-link,/lustre/ssinghal/ADIOS2-install/lib64  
LDFLAGS=-L/lustre/ssinghal/zmq-install/lib64 -lzmq -lstdc++ 

all: stream_arbitrator adios2_actuator.so global_arbitrator

#checkpoint_cxx11: checkpoint_cxx11.o 
#	${CXX} ${CXXFLAGS1} ${LDFLAGS2} checkpoint_cxx11.o -o checkpoint_cxx11

#adios2_interceptor_cxx.so: adios2_interceptor_cxx.o
#	${CXX} ${CXXFLAGS} ${LDFLAGS1} adios2_interceptor_cxx.o -o adios2_interceptor_cxx.so

stream_arbitrator:  stream_arbitrator.o info_manager.o stream_messenger.o stream_prop.o

adios2_actuator.so: adios2_interceptor.o stream_actuator.o stream_messenger.o
	${CXX} ${CXXFLAGS} ${LDFLAGS1} adios2_interceptor.o stream_actuator.o stream_messenger.o -o adios2_actuator.so

global_arbitrator: global_arbitrator.o stream_messenger.o

clean:
	rm -rf *.so *.o global_arbitrator stream_arbitrator 
