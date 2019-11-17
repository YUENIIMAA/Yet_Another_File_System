// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"


// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 6.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};

class lock_client_cache : public lock_client {
 private:
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  std::string id;
  // 感觉lab3的带cache的lock客户端对应的是lab2里的lock服务端。
  pthread_mutex_t mutex;
  std::string ownerOfMutex; // 遭遇了死锁，增加了这个变量看谁拿着锁跪了。
  // 锁的一些状态。
  enum lock_status {
    HOLDING = 0, // 我拿着但没用。
    USING, // 我拿着而且在用。
    ACQUIRING, // 我正在要。
    REVOKING, // 我正在还回去。
    NOTCACHED, // 这锁与我何与也。
  };
  // 客户端锁的结构体。
  typedef struct clientLock {
    lock_client_cache::lock_status status;
    pthread_cond_t conditionVariable;
    pthread_cond_t revoke;
    bool isRevoking; // 用于在release的时候区分是保留在缓存中释放还是释放并且从缓存中移除。
  };
  // 锁号到锁。
  std::map<lock_protocol::lockid_t, clientLock> lock_table;
 public:
  static int last_port;
  lock_client_cache(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache() {};
  lock_protocol::status acquire(lock_protocol::lockid_t);
  lock_protocol::status release(lock_protocol::lockid_t);
  rlock_protocol::status revoke_handler(lock_protocol::lockid_t, 
                                        int &);
  rlock_protocol::status retry_handler(lock_protocol::lockid_t, 
                                       int &);
};


#endif
