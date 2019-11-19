// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
  // 增加初始化全局锁。
  mutex = PTHREAD_MUTEX_INITIALIZER;
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int r;
  int ret = lock_protocol::OK;
  printf("lcc[%s]-a: acquire lock[%llu]\n", id.c_str(), lid);
  printf("lcc[%s]-a: lock mutex\n", id.c_str());
  pthread_mutex_lock(&mutex);
  if (lock_table.find(lid) == lock_table.end()) { // 锁第一次被被申请的时候先初始化它。
    printf("lcc[%s]-acquire: create clientLock for lock %llu\n", id.c_str(), lid);
    lock_table[lid].status = NOTCACHED;
    lock_table[lid].conditionVariable = PTHREAD_COND_INITIALIZER;
    lock_table[lid].needRevoke = false;
    lock_table[lid].pendingAcquire = false;
  }
  bool needToAcquire = false;
  if (lock_table[lid].status == HOLDING) { // 在缓存中，未被持有。
    printf("lcc[%s]-a: lock[%llu] is cached and free, acquired\n", id.c_str(), lid);
    lock_table[lid].status = USING; // 直接授权，立刻拿到。
    ret = lock_protocol::OK;
    printf("lcc[%s]-a: unlock mutex\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if (lock_table[lid].status == REVOKING) {
    printf("lcc[%s]-a: lock[%llu] is being revoked, waiting\n", id.c_str(), lid);
    while (lock_table[lid].status != NOTCACHED && lock_table[lid].status != ACQUIRING) {
      printf("lcc[%s]-a: unlock mutex\n", id.c_str());
      pthread_cond_wait(&lock_table[lid].conditionVariable, &mutex);
      printf("lcc[%s]-a: lock mutex\n", id.c_str());
    }
  }
  if (lock_table[lid].status == NOTCACHED) { // 不在缓存中。
    printf("lcc[%s]-a: lock[%llu] is not cached, acquiring it from server\n", id.c_str(), lid);
    lock_table[lid].status = ACQUIRING;
    lock_table[lid].pendingAcquire = true;
    printf("lcc[%s]-a: unlock mutex\n", id.c_str());
    pthread_cond_broadcast(&lock_table[lid].conditionVariable);
    pthread_mutex_unlock(&mutex);
    lock_protocol::status result = cl->call(lock_protocol::acquire, lid, id, r);
    while (result != lock_protocol::OK && result != lock_protocol::RETRY) {
      result = cl->call(lock_protocol::acquire, lid, id, r);
    }
    printf("lcc[%s]-a: lock mutex\n", id.c_str());
    pthread_mutex_lock(&mutex);
    lock_table[lid].pendingAcquire = false;
    if (result == lock_protocol::OK) {
      printf("lcc[%s]-a: lock[%llu] is acquired from server\n", id.c_str(), lid);
      lock_table[lid].status = USING;
      ret = lock_protocol::OK;
      printf("lcc[%s]-a: unlock mutex\n", id.c_str());
      pthread_mutex_unlock(&mutex);
      return ret;
    }
    if (result == lock_protocol::RETRY) {
      if (lock_table[lid].status == USING) {
        printf("lcc[%s]-a: although server telling me to wait lock[%llu], i actually have it\n", id.c_str(), lid);
        ret = lock_protocol::OK;
        printf("lcc[%s]-a: unlock mutex\n", id.c_str());
        pthread_mutex_unlock(&mutex);
        return ret;
      }
      printf("lcc[%s]-a: lock[%llu] is busy at server, but server acknowledges, waiting\n", id.c_str(), lid);
      lock_table[lid].status = ACQUIRING;
    }
  }
  if (lock_table[lid].status == USING || lock_table[lid].status == ACQUIRING) {
    printf("lcc[%s]-a: lock[%llu] is busy(cached but not free or acquiring), waiting\n", id.c_str(), lid);
    while (lock_table[lid].status != HOLDING) {
      printf("lcc[%s]-a: unlock mutex\n", id.c_str());
      pthread_cond_wait(&lock_table[lid].conditionVariable, &mutex);
      printf("lcc[%s]-a: lock mutex\n", id.c_str());
    }
    printf("lcc[%s]-a: lock[%llu] is cached and free now, acquired\n", id.c_str(), lid);
    lock_table[lid].status = USING;
    ret = lock_protocol::OK;
    printf("lcc[%s]-a: unlock mutex\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  ret = lock_protocol::IOERR;
  printf("lcc[%s]-a: you should not see this message\n", id.c_str());
  printf("lcc[%s]-a: unlock mutex\n", id.c_str());
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  int r;
  int ret = rlock_protocol::OK;
  printf("lcc[%s]-r: release lock[%llu]\n", id.c_str(), lid);
  printf("lcc[%s]-r: lock mutex\n", id.c_str());
  pthread_mutex_lock(&mutex);
  if (lock_table.find(lid) == lock_table.end()) { // 日常检查释放的锁是否存在。
    printf("lcc[%s]-r: invalid operation, lock %llu doesn't exist\n", id.c_str(), lid);
    ret = lock_protocol::NOENT;
    printf("lcc[%s]-r: unlock mutex\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if (lock_table[lid].status != USING) { // 日常检查释放的锁是否能被释放。
    printf("lcc[%s]-r: invalid operation, lock[%llu] can not be released\n", id.c_str(), lid);
    ret = lock_protocol::NOENT;
    printf("lcc[%s]-r: unlock mutex\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  printf("lcc[%s]-r: lock[%llu] set to HOLDING\n", id.c_str(), lid);
  lock_table[lid].status = HOLDING; // 释放锁。
  pthread_cond_broadcast(&lock_table[lid].conditionVariable); // 叫等锁的人起来，有没有人会起来暂不知道。
  printf("lcc[%s]-r: unlock mutex\n", id.c_str());
  pthread_mutex_unlock(&mutex);
  printf("lcc[%s]-r: lock mutex\n", id.c_str());
  pthread_mutex_lock(&mutex);
  if (lock_table[lid].needRevoke) {
    printf("lcc[%s]-r: server is revoking lock[%llu], checking if no one acquire it after release\n", id.c_str(), lid);
    if (lock_table[lid].status == HOLDING) { // 再查一次这把锁是否被使用，假如没有被使用说明该锁空闲，如果此时有阻塞的revoke，让它起来把锁拿回server。
      printf("lcc[%s]-r: yes, it's my job to return it to the server, releasing\n", id.c_str());
      lock_table[lid].status = REVOKING;
      lock_table[lid].needRevoke = false;
      printf("lcc[%s]-r: unlock mutex\n", id.c_str());
      pthread_mutex_unlock(&mutex);
      while(cl->call(lock_protocol::release, lid, id, r) != lock_protocol::OK);
      printf("lcc[%s]-r: lock mutex\n", id.c_str());
      pthread_mutex_lock(&mutex);
      lock_table[lid].status = NOTCACHED;
      ret = lock_protocol::OK;
      pthread_cond_broadcast(&lock_table[lid].conditionVariable);
      printf("lcc[%s]-r: unlock mutex\n", id.c_str());
      pthread_mutex_unlock(&mutex);
      return ret;
    }
    else {
      printf("lcc[%s]-r: no, it's other release's job to do so\n", id.c_str());
      ret = lock_protocol::OK;
      printf("lcc[%s]-r: unlock mutex\n", id.c_str());
      pthread_mutex_unlock(&mutex);
      return ret;
    }
  }
  else {
    printf("lcc[%s]-r: server is not revoking lock[%llu], none of my businese\n", id.c_str(), lid);
    ret = lock_protocol::OK;
    printf("lcc[%s]-r: unlock mutex\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  printf("lcc[%s]-r: you should not see this message\n", id.c_str());
  ret = lock_protocol::IOERR;
  printf("lcc[%s]-r: unlock mutex\n", id.c_str());
  pthread_mutex_unlock(&mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  int r;
  int ret = rlock_protocol::OK;
  printf("lcc[%s]-rvk: revoke lock[%llu]\n", id.c_str(), lid);
  printf("lcc[%s]-rvk: lock mutex\n", id.c_str());
  pthread_mutex_lock(&mutex);
  if (lock_table.find(lid) == lock_table.end()) { // 日常检查错误。
    printf("lcc[%s]-rvk: i dont know lock[%llu]\n", id.c_str(), lid);
    ret = rlock_protocol::RPCERR;
    printf("lcc[%s]-rvk: unlock mutex\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if (lock_table[lid].status == REVOKING) {
    printf("lcc[%s]-rvk: lock[%llu] already is revoking\n", id.c_str(), lid);
    ret = rlock_protocol::OK;
    printf("lcc[%s]-rvk: unlock mutex\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if (lock_table[lid].status == ACQUIRING || lock_table[lid].status == NOTCACHED) { // 非HOLDING或USING，锁都不在我手里，无法撤回。
    printf("lcc[%s]-rvk: i know lock[%llu] but it's not in my cache\n", id.c_str(), lid);
    ret = rlock_protocol::RPCERR;
    printf("lcc[%s]-rvk: unlock mutex\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if (lock_table[lid].status == HOLDING) { // HOLDING的情况，锁在我手里而且没人在用，改成未缓存然后叫服务器放掉。
    if (!lock_table[lid].pendingAcquire) {
      printf("lcc[%s]-rvk: i have lock[%llu] and it's free, returning\n", id.c_str(), lid);
      lock_table[lid].status = REVOKING;
      printf("lcc[%s]-rvk: unlock mutex\n", id.c_str());
      pthread_mutex_unlock(&mutex);
      while(cl->call(lock_protocol::release, lid, id, r) != lock_protocol::OK);
      printf("lcc[%s]-rvk: lock mutex\n", id.c_str());
      pthread_mutex_lock(&mutex);
      lock_table[lid].status = NOTCACHED;
      ret = rlock_protocol::OK;
      pthread_cond_broadcast(&lock_table[lid].conditionVariable);
      printf("lcc[%s]-rvk: unlock mutex\n", id.c_str());
      pthread_mutex_unlock(&mutex);
      return ret;
    }
    else {
      printf("lcc[%s]-rvk: i have lock[%llu] and it's free, but i also have blocked acquire, ask release to do my job\n", id.c_str(), lid);
      lock_table[lid].needRevoke = true;
      ret = rlock_protocol::OK;
      printf("lcc[%s]-rvk: unlock mutex\n", id.c_str());
      pthread_mutex_unlock(&mutex);
      return ret;
    }
    printf("lcc[%s]-rvk: you should not see this message\n", id.c_str());
    ret = rlock_protocol::RPCERR;
    printf("lcc[%s]-rvk: unlock mutex\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if (lock_table[lid].status == USING) {
    printf("lcc[%s]-rvk: i have lock[%llu] but it's not free, ask release to do my job\n", id.c_str(), lid);
    lock_table[lid].needRevoke = true;
    ret = rlock_protocol::OK;
    printf("lcc[%s]-rvk: unlock mutex\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  printf("lcc[%s]-rvk: you should not see this message\n", id.c_str());
  ret = rlock_protocol::RPCERR;
  printf("lcc[%s]-rvk: unlock mutex\n", id.c_str());
  pthread_mutex_unlock(&mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  int ret = rlock_protocol::OK;
  printf("lcc[%s]-rty: lock mutex\n", id.c_str());
  pthread_mutex_lock(&mutex);
  if (lock_table.find(lid) == lock_table.end()) { // 日常检查错误。
    printf("lcc[%s]-rty: i dont know lock[%llu]\n", id.c_str(), lid);
    ret = rlock_protocol::RPCERR;
    printf("lcc[%s]-rty: unlock mutex\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if (lock_table[lid].status == ACQUIRING) {
    if (!lock_table[lid].pendingAcquire) {
      printf("lcc[%s]-rty: lock[%llu] is now in my cache, and set to HOLDING\n", id.c_str(), lid);
      lock_table[lid].status = HOLDING;
      pthread_cond_broadcast(&lock_table[lid].conditionVariable);
      ret = rlock_protocol::OK;
      printf("lcc[%s]-rty: unlock mutex\n", id.c_str());
      pthread_mutex_unlock(&mutex);
      return ret;
    }
    else {
      printf("lcc[%s]-rty: lock[%llu] is now in my cache, and set to USING\n", id.c_str(), lid);
      lock_table[lid].status = USING;  // acquire遭遇held最差会发生这样的事情：
                                       // acquire->revoke->release->retry->revoke或acquire->revoke->release->retry
                                       // 发现retry返回时acquire仍被阻塞无法达到等待状态，因此虽逻辑上拥有锁，实际上错过了拿锁的时刻。
                                       // 因此retry把锁改成USING，防止被revoke回收，它虽然收到RETRY，但只要一查状态就会发现已经拿到了。
      ret = rlock_protocol::OK;
      printf("lcc[%s]-rty: unlock mutex\n", id.c_str());
      pthread_mutex_unlock(&mutex);
      return ret;
    }
    
  }
  printf("lcc[%s]-rty: you should not see this message\n", id.c_str());
  ret = rlock_protocol::RPCERR;
  printf("lcc[%s]-rty: unlock mutex\n", id.c_str());
  pthread_mutex_unlock(&mutex);
  return ret;
}



