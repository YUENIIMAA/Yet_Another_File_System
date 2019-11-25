// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <handle.h>

extent_server::extent_server() 
{
  im = new inode_manager();
}

int extent_server::create(std::string cid, uint32_t type, extent_protocol::extentid_t &id)
{
  // alloc a new inode and return inum
  int r;
  printf("es[%s]-create: create inode\n", cid.c_str());
  id = im->alloc_inode(type);
  printf("es[%s]-create: got new inode %llu\n", cid.c_str(), id);
  printf("es[%s]-create: create inode done\n", cid.c_str());
  return extent_protocol::OK;
}

int extent_server::put(std::string cid, extent_protocol::extentid_t id, std::string buf, int &)
{
  id &= 0x7fffffff;
  int r;
  printf("es[%s]-put: put inode %llu\n", cid.c_str(), id);
  const char * cbuf = buf.c_str();
  int size = buf.size();
  im->write_file(id, cbuf, size);
  printf("es[%s]-put: put inode %llu done\n", cid.c_str(), id);
  return extent_protocol::OK;
}

int extent_server::get(std::string cid, extent_protocol::extentid_t id, std::string &buf)
{
  printf("es[%s]-get: get inode %lld\n", cid.c_str(), id);
  int r;
  if (cachedBy.find(id) == cachedBy.end()) {
    cachedBy[id] = cid;
  }
  else if (!cachedBy[id].compare("NULL")) {
    cachedBy[id] = cid;
  }
  else if (cachedBy[id].compare(cid) != 0) {
    printf("es[%s]-get: inode is cached by client[%s], calling flush\n",cid.c_str(), cachedBy[id].c_str());
    std::string dataFlushedBack;
    handle(cachedBy[id]).safebind()->call(rextent_protocol::flush, id, dataFlushedBack);
    put(cid, id, dataFlushedBack, r);
    cachedBy[id] = cid;
    printf("es[%s]-get: inode %lld is flushed\n", cid.c_str(), id);
  }
  else {
    printf("es[%s]-get: client cached inode %lld is sending RPC, not good\n", cid.c_str(), id);
  }
  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }
  printf("es[%s]-get: get inode %lld done\n", cid.c_str(), id);
  return extent_protocol::OK;
}

int extent_server::getattr(std::string cid, extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("es[%s]-getattr: getattr inode %lld\n", cid.c_str(), id);
  int r;
  if (cachedBy.find(id) != cachedBy.end()) {
    if (!cachedBy[id].compare("NULL")) {
      printf("es[%s]-getattr: inode %lld's attr is up to date on server\n", cid.c_str(), id);
    }
    else if (cachedBy[id].compare(cid) != 0) {
      printf("es[%s]-getattr: inode is cached by client[%s], syncing data\n",cid.c_str(), cachedBy[id].c_str());
      handle(cachedBy[id]).safebind()->call(rextent_protocol::sync, id, a);
      printf("es[%s]-getattr: getattr inode %lld done\n", cid.c_str(), id);
      return extent_protocol::OK;
    }
    else {
      printf("es[%s]-getattr: client cached inode %lld's attr is sending RPC, this should not happen\n", cid.c_str(), id);
      handle(cachedBy[id]).safebind()->call(rextent_protocol::sync, id, a);
      printf("es[%s]-getattr: getattr inode %lld done\n", cid.c_str(), id);
      return extent_protocol::OK;
    }
  }
  else {
    printf("es[%s]-getattr: inode %lld's attr is up to date on server\n", cid.c_str(), id);
  }

  id &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->getattr(id, attr);
  a = attr;

  printf("es[%s]-getattr: getattr inode %lld done\n", cid.c_str(), id);
  return extent_protocol::OK;
}

int extent_server::remove(std::string cid, extent_protocol::extentid_t id, int &)
{
  printf("es[%s]-remove: remove inode %lld\n", cid.c_str(), id);
  int r;
  if (cachedBy.find(id) != cachedBy.end()) {
    if (!cachedBy[id].compare("NULL")) {
      printf("es[%s]-remove: inode %lld is not cached and ok to remove\n", cid.c_str(), id);
    }
    else if (cachedBy[id].compare(cid) != 0) {
      printf("es[%s]-remove: inode is cached by client[%s], calling remove\n",cid.c_str(), cachedBy[id].c_str());
      handle(cachedBy[id]).safebind()->call(rextent_protocol::clear, id, r);
    }
    else {
      printf("es[%s]-remove: client cached inode %lld is sending RPC, no need to clear\n", cid.c_str(), id);
    }
  }
  else {
    printf("es[%s]-remove: inode %lld is not cached and ok to remove\n", cid.c_str(), id);
  }
  
  id &= 0x7fffffff;
  
  im->remove_file(id);
  cachedBy[id] = "NULL";
  printf("es[%s]-remove: remove inode %lld done\n", cid.c_str(), id);
  return extent_protocol::OK;
}

