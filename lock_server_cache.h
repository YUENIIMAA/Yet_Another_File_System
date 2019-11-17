#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>


#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"


class lock_server_cache {
 private:
  int nacquire;
  // 同lab2，增加一把全局锁。
  pthread_mutex_t mutex;
  // 根据实验指导，创建一个锁的类，包含了锁的状态（是否被持有或者正在撤回），锁的持有者，锁的等待名单。
  // 为了让锁的状态在敲代码的时候可以用单词表示，模仿了lab中其它protocol的写法设置了enum。
  enum lock_status {
    FREE = 0, // 无人使用。
    HELD, // 在某个client的cache里面。
    REVOKING // 已经才催client还锁了。
  };
  typedef struct serverLock {
    lock_server_cache::lock_status status;
    std::string owner_id;
    // 由于设计上的问题，第二个要锁的客户端（让status从HELD变成REVOKING的那位），
    // 如果在它的acquire rpc返回前又来一个要同一把锁的客户端，即使通过retry把锁给到它，
    // 它也会因为acquire被acquire rpc阻塞导致拿到了也用不了，
    // 陷入一种外部看它是用完了锁的HOLDING、内部看是被挡在了等待它变成HOLDING之前一点的位置的状态，
    // 当队伍空了，它的acquire rpc返回时，锁早就被撤回了，server认为它已经用好锁了，这时acquire会陷入死循环，永远等待一个不会抵达的锁。
    // 因此增加了poor_client_id，修改waiting list插入方式为放在前面，当且仅当第二个要锁的客户端在发送revoke前是唯一等待者，发送后有新的等待者时，它会被再次加到队伍的最后面。
    std::string poor_client_id;
    std::list<std::string> waiting_list;
  };
  // 和lab2不同的是，lab3假如拿不到锁的话不用在server端睡死，直接送一个RETRY回去就好，所以lab2用的Condition Variable这里就去掉了。
  // 和lab2一样的是，每个lid要有自己对应的锁，lab2里简单对应了一个布尔值，这里则是对应一个复杂的锁。
  std::map<lock_protocol::lockid_t, serverLock> lock_table;
 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
