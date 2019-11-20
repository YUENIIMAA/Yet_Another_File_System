// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"

class lock_server {

 protected:
  int nacquire;
  // 增加一把全局锁
  pthread_mutex_t mutex;
  // 增加lid <-> Condition Variable的map，为每个lid维护一个单独的Condition Variable。
  std::map<lock_protocol::lockid_t, pthread_cond_t> conditionOfLock;
  // 增加lid <-> inUse的map的map，为每个lid维护一个是否可被授予的状态。
  std::map<lock_protocol::lockid_t, bool> isLockFree;


 public:
  lock_server();
  ~lock_server() {};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 







