CXX=mpic++

CXXFLAGS=-fPIC -shared -std=c++11 -DADIOS2_USE_MPI -I/lustre/ssinghal/ADIOS2-install/include -I/lustre/ssinghal/json-install/include -I/lustre/ssinghal/zmqcpp-install/include -I/lustre/ssinghal/zmq-install/include

LDFLAGS1=-rdynamic -ldl -L/lustre/ssinghal/zmq-install/lib64 -lzmq -lstdc++ 
LDFLAGS=-L/lustre/ssinghal/zmq-install/lib64 -lzmq -lstdc++ 

all: interm my_adios2.so arbitrator


interm:  interm.o data_manager.o messenger.o stream_prop.o

my_adios2.so: my_adios2.o streamer.o messenger.o
	${CXX} ${CXXFLAGS} ${LDFLAGS1} my_adios2.o streamer.o messenger.o -o my_adios2.so

arbitrator: arbitrator.o messenger.o

clean:
	rm -rf my_adios2.so *.o arbitrator data_manager
