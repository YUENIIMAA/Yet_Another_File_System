// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "extent_server.h"

class extent_client {
 private:
  rpcc *cl;
  int rextent_port;
  std::string cid;
  typedef struct cache {
    bool isAttrCached; // attr不能被单独cache，它可以被data的cache重建。
    bool isDataCached;
    extent_protocol::attr attr;
    std::string data;
  };
  std::map<extent_protocol::extentid_t, cache> cache_table;
  // data和attr果然还是要拆开。
  //typedef struct data_cache {
  //  bool isDataCached;
  //  std::string data;
  //};
  //typedef struct attr_cache {
  //  bool isAttrCached;
  //  extent_protocol::attr attr;
  //};
  //std::map<extent_protocol::extentid_t, data_cache> data_cache;
  //std::map<extent_protocol::extentid_t, attr_cache> attr_cache;

 public:
  static int last_port;
  extent_client(std::string dst);

  extent_protocol::status create(uint32_t type, extent_protocol::extentid_t &eid);
  extent_protocol::status get(extent_protocol::extentid_t eid, 
			                        std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				                          extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);
  rextent_protocol::status flush_handler(extent_protocol::extentid_t eid, std::string &s);
  rextent_protocol::status sync_handler(extent_protocol::extentid_t eid, extent_protocol::attr &a);
  rextent_protocol::status clear_handler(extent_protocol::extentid_t eid, int&);
};

#endif 

