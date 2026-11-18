main: main.cpp epoll_server.cpp TaskQueue.cpp ThreadPool.cpp Message.cpp
	g++ main.cpp epoll_server.cpp TaskQueue.cpp ThreadPool.cpp Message.cpp -o main -pthread

clean:
	rm -f main