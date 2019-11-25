// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"
#include "inode_manager.h"

class extent_server {
 protected:
#if 0
  typedef struct extent {
    std::string data;
    struct extent_protocol::attr attr;
  } extent_t;
  std::map <extent_protocol::extentid_t, extent_t> extents;
#endif
  inode_manager *im;
  std::map<extent_protocol::extentid_t, std::string> cachedBy;
  //std::map<extent_protocol::extentid_t, std::string> attrCachedBy;
  //std::map<extent_protocol::extentid_t, std::string> dataCachedBy; // 只有一个flush却有两个cachedBy是因为最开始只有一个cachedBy在debug的过程中发现了这样的情况。
                                                                     // Client A缓存了最新的data和attr， Client B发起一个get attr之后ES调A的flush使得ES同步到了最新数据。
                                                                     // Client A在此时又发起了get attr， 由于单个cachedBy导致ES认为B缓存了数据，B的flush被调用，于是B中旧的数据覆盖了ES中的新数据。
                                                                     // 修正：尝试后发现还是不行，attr本身似乎无法被cache

 public:
  extent_server();

  //int create(uint32_t type, extent_protocol::extentid_t &id);
  //int put(extent_protocol::extentid_t id, std::string, int &);
  //int get(extent_protocol::extentid_t id, std::string &);
  //int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  //int remove(extent_protocol::extentid_t id, int &);
  // 改成带客户端clientid的新API。
  int create(std::string cid, uint32_t type, extent_protocol::extentid_t &id);
  int put(std::string cid, extent_protocol::extentid_t id, std::string, int &);
  int get(std::string cid, extent_protocol::extentid_t id, std::string &);
  int getattr(std::string cid, extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(std::string cid, extent_protocol::extentid_t id, int &);
};

#endif 







