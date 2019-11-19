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
  printf("lsc[%s]-a: acquire called for lock[%llu]\n", id.c_str(), lid);
  printf("lsc[%s]-a: lock mutex\n", id.c_str());
  pthread_mutex_lock(&mutex);
  nacquire++;
  r = nacquire;
  // 如果锁之前没有被访问过，先初始化它。
  if (lock_table.find(lid) == lock_table.end()) {
    lock_table[lid].status = FREE;
    lock_table[lid].owner_id = "NULL";
    //lock_table[lid].poor_client_id = "NULL";
    lock_table[lid].waiting_list.clear();
  }
  if (lock_table[lid].status == FREE) {
    // 无人使用的情况，直接返回OK给锁。
    printf("lsc[%s]-a: free, lock[%llu] --> client[%s]\n", id.c_str(), lid, id.c_str());
    lock_table[lid].status = HELD;
    lock_table[lid].owner_id = id;
    ret = lock_protocol::OK;
    printf("lsc[%s]-a: unlock mutex\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if (lock_table[lid].status == HELD) {
    // 锁在client缓存里的情况，呼叫client还锁，然后排队等锁。
    printf("lsc[%s]-a: held, lock[%llu] is hold by client[%s], revoking\n", id.c_str(), lid, lock_table[lid].owner_id.c_str());
    // RPC还没发就改成REVOKING是可行而且更优的，这样只会有一个client的请求不停地发送撤销RPC，其它要同一把锁的只会默默等待，减少了RPC和冲突。
    lock_table[lid].status = REVOKING;
    lock_table[lid].waiting_list.push_back(id);
    //lock_table[lid].poor_client_id = id;
    std::string lockOwner = lock_table[lid].owner_id;
    handle h(lockOwner);
    ret = lock_protocol::RETRY;
    pthread_mutex_unlock(&mutex);
    printf("lsc[%s]-a: unlock mutex\n", id.c_str());
    rpcc *cl = h.safebind();
    if (cl) {
      while (cl->call(rlock_protocol::revoke, lid, r) != rlock_protocol::OK); // 不成功就死循环在这里。
      printf("lsc[%s]-a: revoke RPC returned\n", id.c_str());
    }
    return ret;
  }
  if (lock_table[lid].status == REVOKING) {
    std::list<std::string>::iterator it;
    for (it = lock_table[lid].waiting_list.begin();it != lock_table[lid].waiting_list.end();it++) {
      if (!id.compare(*it)) {
        printf("lsc[%s]-a: revoking, client alreay in the waiting list\n", id.c_str(), lid);
        ret = lock_protocol::RETRY;
        printf("lsc[%s]-a: unlock mutex\n", id.c_str());
        pthread_mutex_unlock(&mutex);
        return ret;
      }
    }
    printf("lsc[%s]-a: revoking, lock[%llu] on its way back\n", id.c_str(), lid);
    // 已经有一个人在发撤销申请了，只需要排队就好。
    lock_table[lid].waiting_list.push_back(id);
    ret = lock_protocol::RETRY;
    printf("lsc[%s]-a: unlock mutex\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  printf("lsc[%s]-a: you should not see this message\n", id.c_str());
  ret = lock_protocol::NOENT;
  printf("lsc[%s]-a: unlock mutex\n", id.c_str());
  pthread_mutex_unlock(&mutex);
  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("lsc[%s]-r: release called for lock[%llu]\n", id.c_str(), lid);
  printf("lsc[%s]-r: lock mutex\n", id.c_str());
  pthread_mutex_lock(&mutex);
  nacquire++;
  r = nacquire;
  if (lock_table.find(lid) == lock_table.end()) { // 检查是否尝试释放一个不存在的锁。
    printf("lsc[%s]-r: invalid operation, lock[%llu] doesn't exist\n", id.c_str(), lid);
    ret = lock_protocol::NOENT;
    printf("lsc[%s]-r: unlock mutex\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if (lock_table[lid].owner_id.compare(id)) { // 检查是否尝试释放一个不属于该客户端的锁。
    printf("lsc[%s]-r: invalid operation, lock[%llu] doesn't belong to client %s\n", id.c_str(), lid, id.c_str());
    ret = lock_protocol::NOENT;
    printf("lsc[%s]-r: lock mutex\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if (lock_table[lid].waiting_list.empty()) { // 假如队伍为空，无人等待这把锁，原地释放。
    printf("lsc[%s]-r: no one is waiting for lock[%llu], released\n", id.c_str(), lid);
    lock_table[lid].status = FREE;
    lock_table[lid].owner_id = "NULL";
    //lock_table[lid].poor_client_id = "NULL";
    lock_table[lid].waiting_list.clear();
    ret = lock_protocol::OK;
    printf("lsc[%s]-r: unlock mutex\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  else { // 队伍里有人，调那个人的retry（实现上采用了用一条RPC把锁直接送到client手里的方案）。
    std::string nextClient = lock_table[lid].waiting_list.front();
    lock_table[lid].waiting_list.pop_front();
    lock_table[lid].owner_id = nextClient;
    printf("lsc[%s]-r: client[%s] is waiting for lock[%llu], calling retry\n", id.c_str(), nextClient.c_str(),lid);
    printf("lsc[%s]-a: lock[%llu] --> client[%s]\n", id.c_str(), lid, nextClient.c_str());
    handle h(nextClient);
    printf("lsc[%s]-r: unlock mutex\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    rpcc *cl = h.safebind();
    if (cl) {
      while(cl->call(rlock_protocol::retry, lid, r) != rlock_protocol::OK);
      printf("lsc[%s]-r: retry RPC returned\n", id.c_str());
    }
    printf("lsc[%s]-r: lock mutex\n", id.c_str());
    pthread_mutex_lock(&mutex);
    if (!lock_table[lid].waiting_list.empty()) { // 给完锁之后看一眼是否后面还有人在等，有的话往刚给完锁的客户端发一个revoke，让他用完就调release还锁。
      printf("lsc[%s]-r: after retry, client[%s] is the next, calling revoke\n", id.c_str(), lock_table[lid].waiting_list.front().c_str());
      std::string lockOwner = lock_table[lid].owner_id;
      //std::string poorOwner = lock_table[lid].poor_client_id;
      //if (!lockOwner.compare(poorOwner)) {
      //  lock_table[lid].poor_client_id = "NULL";
      //  lock_table[lid].waiting_list.push_back(poorOwner);
      //}
      handle h(lockOwner);
      ret = lock_protocol::OK;
      printf("lsc[%s]-r: lock mutex\n", id.c_str());
      pthread_mutex_unlock(&mutex);
      rpcc *cl = h.safebind();
      if (cl) {
        while (cl->call(rlock_protocol::revoke, lid, r) != rlock_protocol::OK); // 不成功就死循环在这里。
        printf("lsc[%s]-r: revoke returned\n", id.c_str());
      }
      return ret;
    }
    else {
      //lock_table[lid].poor_client_id = "NULL";
      lock_table[lid].status = HELD;
      printf("lsc[%s]-r: after retry, no more client is waiting, leaving it there\n", id.c_str());
      printf("lsc[%s]-r: lock mutex\n", id.c_str());
      ret = lock_protocol::OK;
      pthread_mutex_unlock(&mutex);
      return ret;
    }
  }
  printf("lsc[%s]-r: you should not see this message\n", id.c_str());
  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

