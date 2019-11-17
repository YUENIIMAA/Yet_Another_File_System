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
  // 直接取自lab2的部分。
  printf("lcc[%s]-acquire: trying to acquire lock %llu\n", id.c_str(), lid);
  int times = 0;
  while (pthread_mutex_trylock(&mutex))
  {
    times++;
    printf("lcc[%s]-acquire: mutex in %s, attempt %d failed\n", id.c_str(), ownerOfMutex.c_str(), times);
    //assert(times < 512);
  }
  ownerOfMutex = "acquire";
  printf("lcc[%s]-acquire: mutex acquired\n", id.c_str());
  // lab3从这里开始。
  // 锁第一次被被申请的时候先初始化它。
  if (lock_table.find(lid) == lock_table.end()) {
    printf("lcc[%s]-acquire: create clientLock for lock %llu\n", id.c_str(), lid);
    lock_table[lid].status = NOTCACHED;
    lock_table[lid].conditionVariable = PTHREAD_COND_INITIALIZER;
    lock_table[lid].revoke = PTHREAD_COND_INITIALIZER;
    lock_table[lid].isRevoking = false;
  }
  bool needToAcquire = false;
  if (lock_table[lid].status == HOLDING) { // 在缓存中，未被持有。
    printf("lcc[%s]-acquire: I have lock %llu and it's free, granting\n", id.c_str(), lid);
    lock_table[lid].status = USING; // 直接授权，立刻拿到。
  }
  else if (lock_table[lid].status == NOTCACHED) { // 不在缓存中。
    printf("lcc[%s]-acquire: I don't have lock %llu, will acquire it soon\n", id.c_str(), lid);
    needToAcquire = true; // 需要问server要，说不定可以立刻拿到。
    lock_table[lid].status = ACQUIRING;
  }
  else {
    // 其它情况，肯定立刻拿不到，同lab2类似利用Condition Variable睡下去等。
    // 比如不在缓存里，需等待拿到锁。
    // 比如在缓存里，但是未被释放，需等待释放。
    // 比如在缓存里，但正在释放中，那就等到锁释放完再去要一次。
    printf("lcc[%s]-acquire: lock %llu is busy now, waiting\n", id.c_str(), lid);
    while (lock_table[lid].status != NOTCACHED && lock_table[lid].status != HOLDING) {
      printf("lcc[%s]-acquire: mutex released\n", id.c_str());
      pthread_cond_wait(&lock_table[lid].conditionVariable, &mutex);
      ownerOfMutex = "acquire";
      printf("lcc[%s]-acquire: mutex acquired\n", id.c_str());
    }
    if (lock_table[lid].status == NOTCACHED) { // 不在缓存中。
      needToAcquire = true; // 需要问server要，说不定可以立刻拿到。
      lock_table[lid].status = ACQUIRING;
    }
    else {
      lock_table[lid].status = USING;
    }
  }
  if (needToAcquire) {
    printf("lcc[%s]-acquire: mutex released\n", id.c_str());
    pthread_mutex_unlock(&mutex); // 惯例发RPC前放锁。
    printf("lcc[%s]-acquire: acquiring lock %llu from server\n", id.c_str(), lid);
    lock_protocol::status result = cl->call(lock_protocol::acquire, lid, id, r);
    while (result != lock_protocol::OK && result != lock_protocol::RETRY) {
      result = cl->call(lock_protocol::acquire, lid, id, r);
    }
    times = 0;
    while (pthread_mutex_trylock(&mutex))
    {
      times++;
      printf("lcc[%s]-acquire: mutex in %s, attempt %d failed\n", id.c_str(), ownerOfMutex.c_str(), times);
      //assert(times < 512);
    }
    ownerOfMutex = "acquire";
    printf("lcc[%s]-acquire: mutex acquired\n", id.c_str());
    if (result == lock_protocol::OK) { // 说明锁在server就是free的，但之前不在缓存里，现在拿到了，就在我手里。
      lock_table[lid].status = USING;
      printf("lcc[%s]-acquire: got lock %llu\n", id.c_str(), lid);
      printf("lcc[%s]-acquire: mutex released\n", id.c_str());
      pthread_mutex_unlock(&mutex);
    }
    else {
      printf("lcc[%s]-acquire: lock %llu currently not available, waiting\n", id.c_str(), lid);
      while (lock_table[lid].status != HOLDING) { // 说明锁在server里，但是无法立刻给到我，要等它进缓存再去和自己人抢锁。
        printf("lcc[%s]-acquire: mutex released\n", id.c_str());
        pthread_cond_wait(&lock_table[lid].conditionVariable, &mutex);
        ownerOfMutex = "acquire";
        printf("lcc[%s]-acquire: mutex acquired\n", id.c_str());
      }
      lock_table[lid].status = USING;
      printf("lcc[%s]-acquire: got lock %llu\n", id.c_str(), lid);
      printf("lcc[%s]-acquire: mutex released\n", id.c_str());
      pthread_mutex_unlock(&mutex);
    }
  }
  else {
    printf("lcc[%s]-acquire: mutex released\n", id.c_str());
    pthread_mutex_unlock(&mutex);
  }
  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  int ret = rlock_protocol::OK;
  // 直接取自lab2的部分。
  printf("lcc[%s]-release: trying to release lock %llu\n", id.c_str(), lid);
  int times = 0;
  while (pthread_mutex_trylock(&mutex))
  {
    times++;
    printf("lcc[%s]-release: mutex in %s, attempt %d failed\n", id.c_str(), ownerOfMutex.c_str(), times);
    //assert(times < 512);
  }
  ownerOfMutex = "release";
  printf("lcc[%s]-release: mutex acquired\n", id.c_str());
  // lab3从这里开始。
  // release不负责把锁还给服务器。
  if (lock_table.find(lid) == lock_table.end()) { // 日常检查释放的锁是否存在。
    printf("lcc[%s]-release: invalid operation, lock %llu doesn't exist\n", id.c_str(), lid);
    ret = lock_protocol::NOENT;
    printf("lcc[%s]-release: mutex released\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if (lock_table[lid].status != USING) { // 日常检查释放的锁是否能被释放。
    printf("lcc[%s]-release: invalid operation, lock %llu can not be released\n", id.c_str(), lid);
    ret = lock_protocol::NOENT;
    printf("lcc[%s]-release: mutex released\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  else {
    printf("lcc[%s]-release: releasing lock %llu\n", id.c_str(), lid);
    lock_table[lid].status = HOLDING; // 释放锁。
    pthread_cond_signal(&lock_table[lid].conditionVariable); // 叫等锁的人起来，有没有人会起来暂不知道。
    printf("lcc[%s]-release: mutex released\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    printf("lcc[%s]-release: checking whether lock %llu is now in use\n", id.c_str(), lid); // 放锁之后重新抢锁。
    times = 0;
    while (pthread_mutex_trylock(&mutex))
    {
      times++;
      printf("lcc[%s]-release: mutex in %s, attempt %d failed\n", id.c_str(), ownerOfMutex.c_str(), times);
      //assert(times < 512);
    }
    ownerOfMutex = "release";
    printf("lcc[%s]-release: mutex acquired\n", id.c_str());
    if (lock_table[lid].status == HOLDING) { // 再查一次这把锁是否被使用，假如没有被使用说明该锁空闲，如果此时有阻塞的revoke，让它起来把锁拿回server。
      printf("lcc[%s]-release: lock %llu is still free, waking up revoke\n", id.c_str(), lid);
      pthread_cond_signal(&lock_table[lid].revoke);
    }
    printf("lcc[%s]-release: mutex released\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  printf("lcc[%s]-release: mutex released\n", id.c_str());
  pthread_mutex_unlock(&mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  int r;
  int ret = rlock_protocol::OK;
  // 直接取自lab2的部分。
  printf("lcc[%s]-revoke: trying to revoke lock %llu\n", id.c_str(), lid);
  int times = 0;
  while (pthread_mutex_trylock(&mutex))
  {
    times++;
    printf("lcc[%s]-revoke: mutex in %s, attempt %d failed\n", id.c_str(), ownerOfMutex.c_str(), times);
    //assert(times < 512);
  }
  ownerOfMutex = "revoke";
  printf("lcc[%s]-revoke: mutex acquired\n", id.c_str());
  // lab3从这里开始。
  if (lock_table.find(lid) == lock_table.end()) { // 日常检查错误。
    printf("lcc[%s]-revoke: I dont have lock %llu\n", id.c_str(), lid);
    ret = rlock_protocol::RPCERR;
    printf("lcc[%s]-revoke: mutex released\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if (lock_table[lid].status == REVOKING) {
    printf("lcc[%s]-revoke: already started revoking lock %llu\n", id.c_str(), lid);
    printf("lcc[%s]-revoke: mutex released\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if (lock_table[lid].status != HOLDING && lock_table[lid].status != USING) { // 非HOLDING或USING，锁都不在我手里，无法撤回。
    printf("lcc[%s]-revoke: revoke lock %llu from me is meaningless\n", id.c_str(), lid);
    ret = rlock_protocol::RPCERR;
    printf("lcc[%s]-revoke: mutex released\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if (lock_table[lid].status == HOLDING) { // HOLDING的情况，锁在我手里而且没人在用，改成未缓存然后叫服务器放掉。
    printf("lcc[%s]-revoke: I have lock %llu and it's free, returning\n", id.c_str(), lid);
    lock_table[lid].status = NOTCACHED;
    while(cl->call(lock_protocol::release, lid, id, r) != lock_protocol::OK);
    pthread_cond_signal(&lock_table[lid].conditionVariable);
    printf("lcc[%s]-revoke: mutex released\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  else {
    printf("lcc[%s]-revoke: I have lock %llu but it's not free, waiting\n", id.c_str(), lid);
    while (lock_table[lid].status != HOLDING) { // 等到没有人用锁了我会再次醒来。
      printf("lcc[%s]-revoke: mutex released\n", id.c_str());
      pthread_cond_wait(&lock_table[lid].revoke, &mutex);
      ownerOfMutex = "revoke";
      printf("lcc[%s]-revoke: mutex acquired\n", id.c_str());
    }
    lock_table[lid].status = REVOKING;
    printf("lcc[%s]-revoke: mutex released\n", id.c_str());
    printf("lcc[%s]-revoke: returning lock %llu\n", id.c_str(), lid);
    printf("lcc[%s]-revoke: sending RPC\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    while(cl->call(lock_protocol::release, lid, id, r) != lock_protocol::OK);
    times = 0;
    while (pthread_mutex_trylock(&mutex))
    {
      times++;
      printf("lcc[%s]-revoke: mutex in %s, attempt %d failed\n", id.c_str(), ownerOfMutex.c_str(), times);
      //assert(times < 512);
    }
    ownerOfMutex = "revoke";
    printf("lcc[%s]-revoke: mutex acquired\n", id.c_str());
    lock_table[lid].status = NOTCACHED; /// 还回去。
    printf("lcc[%s]-revoke: lock %llu returned to server\n", id.c_str(), lid);
    pthread_cond_signal(&lock_table[lid].conditionVariable);
    printf("lcc[%s]-revoke: mutex released\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  printf("lcc[%s]-revoke: mutex released\n", id.c_str());
  pthread_mutex_unlock(&mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  int ret = rlock_protocol::OK;
  // 直接取自lab2的部分。
  printf("lcc[%s]-retry: retrying to get lock %llu\n", id.c_str(), lid);
  int times = 0;
  while (pthread_mutex_trylock(&mutex))
  {
    times++;
    printf("lcc[%s]-retry: mutex in %s, attempt %d failed\n", id.c_str(), ownerOfMutex.c_str(), times);
    //assert(times < 512);
  }
  ownerOfMutex = "retry";
  printf("lcc[%s]-retry: mutex acquired\n", id.c_str());
  // lab3从这里开始。
  if (lock_table.find(lid) == lock_table.end()) { // 日常检查错误。
    printf("lcc[%s]-retry: I dont have lock %llu\n", id.c_str(), lid);
    ret = rlock_protocol::RPCERR;
    printf("lcc[%s]-retry: mutex released\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if (lock_table[lid].status == ACQUIRING) {
    printf("lcc[%s]-retry: lock %llu is now in my cache\n", id.c_str(), lid);
    lock_table[lid].status = HOLDING;
    pthread_cond_signal(&lock_table[lid].conditionVariable);
    printf("lcc[%s]-retry: mutex released\n", id.c_str());
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  printf("lcc[%s]-retry: mutex released\n", id.c_str());
  pthread_mutex_unlock(&mutex);
  return ret;
}



