// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

// 修改构造函数配合新增的mutex锁（之前的写法说实话没看懂、、、）
lock_server::lock_server() {
  nacquire = 0;
  mutex = PTHREAD_MUTEX_INITIALIZER;
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here
  // 通过学习https://youtu.be/13dFggo4t_I和https://youtu.be/rMpOfbaP2PQ大致掌握了Pthread、Mutex和Condition Variable的用法
  printf("lock_server: trying to acquire lock\n");
  int times = 0;
  // 在视频里学到了另外一种拿锁的方式，感觉可以更加方便观察并发情况。
  while (pthread_mutex_trylock(&mutex))
  {
    times++;
    printf("lock_server: mutex in use, attempt %d failed\n", times);
  }
  printf("lock_server: mutex acquired\n");
  nacquire++;
  // 假如conditionOfLock里面没有记录当前锁的信息就新建一条放进去。
  // 同时说明只要我现在不放锁，那我肯定可以安全地修改当前锁的授予状态。
  if (conditionOfLock.find(lid) == conditionOfLock.end()) {
    printf("lock_server[%d]: this the first time lock %d is acquired, new condition variable will be created\n", nacquire, lid);
    conditionOfLock.insert(std::pair<lock_protocol::lockid_t, pthread_cond_t>(lid, PTHREAD_COND_INITIALIZER));
    isLockFree.insert(std::pair<lock_protocol::lockid_t, bool>(lid, true));
  }
  else {
    // 已经有记录了，表明它有可能已经被授予其它请求使用了。
    // 因此用上了pthread_cond_wait，把锁放掉然后睡下去直到别人放锁把我唤醒。
    printf("lock_server[%d]: condition variable detected, checking whether the mutex should be released for now\n", nacquire);
    while (!isLockFree[lid])
    {
      printf("lock_server[%d]: the lock is in use, release mutex and go to sleep\n", nacquire);
      pthread_cond_wait(&(conditionOfLock[lid]), &mutex);
      printf("lock_server[%d]: the lock should be free now, so I woke up\n", nacquire);
    }
  }
  printf("lock_server[%d]: got the lock\n", nacquire);
  isLockFree[lid] = false;
  r = nacquire;
  printf("lock_server[%d]: job done, mutex released\n", nacquire);
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here
  printf("lock_server: trying to release a lock\n");
  int times = 0;
  while (pthread_mutex_trylock(&mutex))
  {
    times++;
    printf("lock_server: mutex in use, attempt %d failed\n", times);
  }
  printf("lock_server: mutex acquired\n");
  nacquire++;
  // 先检查一下这个解锁合不合法以防万一。
  if (isLockFree.find(lid) == isLockFree.end()) {
    printf("lock_server[%d]: invalid operation, lock doesn't exist\n", nacquire);
    ret = lock_protocol::IOERR;
  }
  else {
    // 合法的话就把它的状态改为free这样假如有睡着等它的while，一旦下次拿到锁就可以跳出循环了。
    printf("lock_server[%d]: lock %d set to free\n", nacquire, lid);
    isLockFree[lid] = true;
    // 唤醒队伍中下一个等待这个状态的人（按照我的理解应该用signal应该只有一个会被唤醒所以不存在唤醒一堆又竞争锁的情况）。
    pthread_cond_signal(&(conditionOfLock[lid]));
  }
  r = nacquire;
  pthread_mutex_unlock(&mutex);
  return ret;
}
