main: main.cpp epoll_server.cpp TaskQueue.cpp ThreadPool.cpp
	g++ main.cpp epoll_server.cpp TaskQueue.cpp ThreadPool.cpp -o main -pthread

clean:
	rm -f main