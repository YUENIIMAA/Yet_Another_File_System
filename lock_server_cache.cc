// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

// 普通的构建函数。
lock_server_cache::lock_server_cache()
{
  nacquire = 0;
  mutex = PTHREAD_MUTEX_INITIALIZER;
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &r) // 补了一个r进去。
{
  lock_protocol::status ret = lock_protocol::OK;
  // 直接取自lab2的部分。
  printf("lsc[%s]: trying to acquire lock %llu\n", id.c_str(), lid);
  int times = 0;
  while (pthread_mutex_trylock(&mutex))
  {
    times++;
    //printf("lsc[%s]: mutex in use, attempt %d failed\n", id.c_str(), times);
  }
  printf("lsc[%s]: mutex acquired\n", id.c_str());
  nacquire++;
  r = nacquire;
  // lab3从这里开始。
  // 如果锁之前没有被访问过，先初始化它。
  if (lock_table.find(lid) == lock_table.end()) {
    printf("lsc[%s]: create serverLock for lock %llu\n", id.c_str(), lid);
    lock_table[lid].status = FREE;
    lock_table[lid].owner_id = "NULL";
    lock_table[lid].waiting_list.clear();
  }
  if (lock_table[lid].status == FREE) {
    // 无人使用的情况，直接返回OK给锁。
    printf("lsc[%s]: lock %llu is free, acquirement granted\n", id.c_str(), lid);
    lock_table[lid].status = HELD;
    lock_table[lid].owner_id = id;
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if (lock_table[lid].status == HELD) {
    // 锁在client缓存里的情况，呼叫client还锁，然后排队等锁。
    printf("lsc[%s]-acquire: lock %llu is cached by another client %s, revoking\n", id.c_str(), lid, lock_table[lid].owner_id.c_str());
    // RPC还没发就改成REVOKING是可行而且更优的，这样只会有一个client的请求不停地发送撤销RPC，其它要同一把锁的只会默默等待，减少了RPC和冲突。
    lock_table[lid].status = REVOKING;
    lock_table[lid].waiting_list.push_front(id);
    lock_table[lid].poor_client_id = id;
    std::string lockOwner = lock_table[lid].owner_id;
    handle h(lockOwner);
    // 发送RPC前先放锁，但是得在handle已经创建完之后才能放。
    pthread_mutex_unlock(&mutex);
    rpcc *cl = h.safebind();
    if (cl) {
      while (cl->call(rlock_protocol::revoke, lid, r) != rlock_protocol::OK); // 不成功就死循环在这里。
      printf("lsc[%s]-acquire: lock %llu is revoked from client %s\n", id.c_str(), lid, lockOwner.c_str());
    }
    ret = lock_protocol::RETRY;
    return ret;
  }
  if (lock_table[lid].status == REVOKING) {
    printf("lsc[%s]-acquire: lock %llu is revoking, client added to the waiting list\n", id.c_str(), lid);
    // 已经有一个人在发撤销申请了，只需要排队就好。
    lock_table[lid].waiting_list.push_front(id);
    ret = lock_protocol::RETRY;
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  ret = lock_protocol::NOENT;
  pthread_mutex_unlock(&mutex);
  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  // 直接取自lab2的部分。
  printf("lsc[%s]-release: trying to release lock %llu\n", id.c_str(), lid);
  int times = 0;
  while (pthread_mutex_trylock(&mutex))
  {
    times++;
    //printf("lsc[%s]-release: mutex in use, attempt %d failed\n", id.c_str(), times);
  }
  printf("lsc[%s]-release: mutex acquired\n", id.c_str());
  nacquire++;
  r = nacquire;
  // lab3从这里开始。
  // 检查是否尝试释放一个不存在的锁。
  if (lock_table.find(lid) == lock_table.end()) {
    printf("lsc[%s]-release: invalid operation, lock %llu doesn't exist\n", id.c_str(), lid);
    ret = lock_protocol::NOENT;
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  // 检查是否尝试释放一个不属于该客户端的锁。
  if (lock_table[lid].owner_id.compare(id)) {
    printf("lsc[%s]-release: invalid operation, lock %llu doesn't belong to client %s\n", id.c_str(), lid, id.c_str());
    ret = lock_protocol::NOENT;
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  // 假如队伍为空，无人等待这把锁，原地释放。
  if (lock_table[lid].waiting_list.empty()) {
    printf("lsc[%s]-release: no one is waiting for lock %llu, reset to default\n", id.c_str(), lid);
    lock_table[lid].status = FREE;
    lock_table[lid].owner_id = "NULL";
    lock_table[lid].waiting_list.clear();
    pthread_mutex_unlock(&mutex);
  }
  // 队伍里有人，调那个人的retry（实现上采用了用一条RPC把锁直接送到client手里的方案）。
  else {
    std::string nextClient = lock_table[lid].waiting_list.front();
    lock_table[lid].waiting_list.pop_front();
    lock_table[lid].owner_id = nextClient;
    printf("lsc[%s]-release: client %s is waiting for lock %llu, shifting ownership\n", id.c_str(), nextClient.c_str(),lid);
    handle h(nextClient);
    pthread_mutex_unlock(&mutex);
    rpcc *cl = h.safebind();
    if (cl) {
      while(cl->call(rlock_protocol::retry, lid, r) != rlock_protocol::OK);
      printf("lsc[%s]-release: client %s is now the owner of lock %llu\n", id.c_str(), nextClient.c_str(),lid);
    }
    printf("lsc[%s]-release: accquiring mutex\n", id.c_str());
    times = 0;
    while (pthread_mutex_trylock(&mutex))
    {
      times++;
      //printf("lsc[%s]-release: mutex in use, attempt %d failed\n", id.c_str(), times);
    }
    printf("lsc[%s]-release: mutex acquired\n", id.c_str());
    // 给完锁之后看一眼是否后面还有人在等，有的话往刚给完锁的客户端发一个revoke，让他用完就调release还锁。
    if (!lock_table[lid].waiting_list.empty()) {
      printf("lsc[%s]-release: next client is %s, send revoke RPC immediately\n", id.c_str(), lock_table[lid].waiting_list.front().c_str());
      std::string lockOwner = lock_table[lid].owner_id;
      std::string poorOwner = lock_table[lid].poor_client_id;
      if (!lockOwner.compare(poorOwner)) {
        lock_table[lid].poor_client_id = "NULL";
        lock_table[lid].waiting_list.push_back(poorOwner);
      }
      handle h(lockOwner);
      pthread_mutex_unlock(&mutex);
      rpcc *cl = h.safebind();
      if (cl) {
        while (cl->call(rlock_protocol::revoke, lid, r) != rlock_protocol::OK); // 不成功就死循环在这里。
        printf("lsc[%s]-release: lock %llu is revoked from client %s\n", id.c_str(), lid, lockOwner.c_str());
      }
    }
    else {
      // 说明在这个瞬间已经没有客户端在等待锁了，因此可以把锁放在最后一个人的缓存里。
      // 这种情况下poor client就不poor了，把它去掉。
      lock_table[lid].poor_client_id = "NULL";
      lock_table[lid].status = HELD;
      printf("lsc[%s]-release: no more client is waiting for this lock, job done\n", id.c_str());
      pthread_mutex_unlock(&mutex);;
    }
  }
  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

