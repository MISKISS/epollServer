// 定义任务结构体
#include <mutex>
#include<queue>
#include <functional>
struct Task
{
    Task() = default;

     Task(std::function<void()> func)
        : function(std::move(func))
    {
    }
    std::function<void()> function;
};

// 任务队列
class TaskQueue
{
public:
    TaskQueue();
    ~TaskQueue();

    // 添加任务
    void addTask(const Task& task);
    void addTask(std::function<void()> func);

    // 取出一个任务
    Task takeTask();

    // 获取当前队列中任务个数
    inline int taskNumber()
    {
        return m_queue.size();
    }

private:
    pthread_mutex_t m_mutex;    // 互斥锁
    std::queue<Task> m_queue;   // 任务队列
};

