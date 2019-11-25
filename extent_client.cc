// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

int extent_client::last_port = 0;

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
  rextent_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.3";
  std::ostringstream host;
  host << hname << ":" << rextent_port;
  cid = host.str();
  last_port = rextent_port;
  rpcs *resrpc = new rpcs(rextent_port);
  // resrpc->reg(xxx,xxx,xxx,xxx)
  resrpc->reg(rextent_protocol::flush, this, &extent_client::flush_handler);
  resrpc->reg(rextent_protocol::sync, this, &extent_client::sync_handler);
  resrpc->reg(rextent_protocol::clear, this, &extent_client::clear_handler);
}

// a demo to show how to use RPC
extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  ret = cl->call(extent_protocol::create, cid, type, id);
  if (cache_table.find(id) == cache_table.end()) {
    printf("client-create[%s]: create cache entry for inode %llu\n", cid.c_str(), id);
    cache_table[id].isAttrCached = false;
    cache_table[id].isDataCached = false;
    cache_table[id].data.clear();
  }
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  if (cache_table.find(eid) == cache_table.end()) {
    printf("client-get[%s]: create data cache entry for inode %llu\n", cid.c_str(), eid);
    cache_table[eid].isDataCached = false;
    cache_table[eid].isAttrCached = false;
    cache_table[eid].data.clear();
  }
  if (cache_table[eid].isDataCached) {
    if (!cache_table[eid].isAttrCached) {
      getattr(eid, cache_table[eid].attr);
    }
    cache_table[eid].attr.atime = (unsigned)time(0);
    cache_table[eid].attr.size = cache_table[eid].data.size();
    printf("client-get[%s]: inode %llu's data is cached\n", cid.c_str(), eid);
    buf = cache_table[eid].data;
    return extent_protocol::OK;
  }
  else {
    printf("client-get[%s]: inode %llu's data is not cached\n", cid.c_str(), eid);
    if (!cache_table[eid].isAttrCached) {
      getattr(eid, cache_table[eid].attr);
    }
    ret = cl->call(extent_protocol::get, cid, eid, buf);
    cache_table[eid].attr.atime = (unsigned)time(0);
    cache_table[eid].attr.size = buf.size();
    cache_table[eid].data = buf;
    cache_table[eid].isDataCached = true;
    printf("client-get[%s]: after RPC, inode %llu is now cached\n", cid.c_str(), eid);
    return extent_protocol::OK;
  }
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  if (cache_table.find(eid) == cache_table.end()) {
    printf("client-getattr[%s]: create cache entry for inode %llu\n", cid.c_str(), eid);
    cache_table[eid].isAttrCached = false;
    cache_table[eid].isDataCached = false;
    cache_table[eid].data.clear();
  }
  if (!cache_table[eid].isAttrCached) {
    printf("client-getattr[%s]: inode %llu's attr is not cached\n", cid.c_str(), eid);
    ret = cl->call(extent_protocol::getattr, cid, eid, attr);
    cache_table[eid].attr.atime = attr.atime;
    cache_table[eid].attr.ctime = attr.ctime;
    cache_table[eid].attr.mtime = attr.mtime;
    cache_table[eid].attr.size = attr.size;
    cache_table[eid].attr.type = attr.type;
    cache_table[eid].isAttrCached = true;
    printf("client-getattr[%s]: after RPC, inode %llu's attr is now cached\n", cid.c_str(), eid);
    return extent_protocol::OK;
  }
  else if (!cache_table[eid].isDataCached){
    printf("client-getattr[%s]: inode %llu's attr is cached but data is not, updating attr\n", cid.c_str(), eid);
    ret = cl->call(extent_protocol::getattr, cid, eid, attr);
    cache_table[eid].attr.atime = attr.atime;
    cache_table[eid].attr.ctime = attr.ctime;
    cache_table[eid].attr.mtime = attr.mtime;
    cache_table[eid].attr.size = attr.size;
    cache_table[eid].attr.type = attr.type;
    printf("client-getattr[%s]: after RPC, cached inode %llu's attr is updated\n", cid.c_str(), eid);
    return extent_protocol::OK;
  }
  else {
    printf("client-getattr[%s]: both inode %llu's attr and data are cached\n", cid.c_str(), eid);
    attr.atime = cache_table[eid].attr.atime;
    attr.ctime = cache_table[eid].attr.ctime;
    attr.mtime = cache_table[eid].attr.mtime;
    attr.size = cache_table[eid].attr.size;
    attr.type = cache_table[eid].attr.type;
    return extent_protocol::OK;
  }
  printf("client-getattr[%s]: you should not see this message\n", cid.c_str());
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  int r;
  if (cache_table.find(eid) == cache_table.end()) {
    printf("client-put[%s]: create cache entry for inode %llu, this should not happen\n", cid.c_str(), eid);
    cache_table[eid].isAttrCached = false;
    cache_table[eid].isDataCached = false;
    cache_table[eid].data.clear();
  }
  if (!cache_table[eid].isDataCached) {
    printf("client-put[%s]: inode %llu's data is not cached\n", cid.c_str(), eid);
    get(eid, cache_table[eid].data);
    printf("client-put[%s]: both inode %llu's data and attr should be cached\n", cid.c_str(), eid);
    if (!cache_table[eid].isAttrCached || !cache_table[eid].isDataCached) {
      printf("client-put[%s]: this should not happen\n", cid.c_str());
    }
  }
  cache_table[eid].attr.size = buf.size();
  cache_table[eid].attr.atime = (unsigned)time(0);
  cache_table[eid].attr.ctime = (unsigned)time(0);
  cache_table[eid].attr.mtime = (unsigned)time(0);
  cache_table[eid].data = buf;
  printf("client-put[%s]: update inode %llu's data in cache\n", cid.c_str(), eid);
  return extent_protocol::OK;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  int r;
  
  if (cache_table.find(eid) == cache_table.end()) {
    printf("client-remove[%s]: create cache entry for inode %llu, this should not happen\n", cid.c_str(), eid);
    cache_table[eid].isAttrCached = false;
    cache_table[eid].isDataCached = false;
    cache_table[eid].data.clear();
  }
  cache_table[eid].isAttrCached = false;
  cache_table[eid].isDataCached = false;
  
  ret = cl->call(extent_protocol::remove, cid, eid, r);
  return ret;
}

rextent_protocol::status
extent_client::flush_handler(extent_protocol::extentid_t eid, std::string &s) {
  int r;
  if (cache_table.find(eid) == cache_table.end()) {
    printf("client-flush[%s]: i don't know inode %llu, this should not happen\n", cid.c_str(), eid);
    return rextent_protocol::RPCERR;
  }
  if (!cache_table[eid].isDataCached) {
    printf("client-flush[%s]: inode %llu is not cached here, this should not happen\n", cid.c_str(), eid);
    return rextent_protocol::RPCERR;
  }
  printf("client-flush[%s] flush cached inode %llu's data and attr to extent_server\n", cid.c_str(), eid);
  s = cache_table[eid].data;
  cache_table[eid].data.clear();
  cache_table[eid].isDataCached = false;
  cache_table[eid].isAttrCached = false;
  return rextent_protocol::OK;
}

rextent_protocol::status
extent_client::sync_handler(extent_protocol::extentid_t eid, extent_protocol::attr &a) {
  int r;
  if (cache_table.find(eid) == cache_table.end()) {
    printf("client-sync[%s]: i don't know inode %llu, this should not happen\n", cid.c_str(), eid);
    return rextent_protocol::RPCERR;
  }
  if (!cache_table[eid].isDataCached) {
    printf("client-sync[%s]: inode %llu's data is not cached here, this should not happen\n", cid.c_str(), eid);
    return rextent_protocol::RPCERR;
  }
  printf("client-sync[%s] sync cached inode %llu's attr with extent_server\n", cid.c_str(), eid);
  a.atime = cache_table[eid].attr.atime;
  a.ctime = cache_table[eid].attr.ctime;
  a.mtime = cache_table[eid].attr.mtime;
  a.size = cache_table[eid].attr.size;
  a.type = cache_table[eid].attr.type;
  return rextent_protocol::OK;
}

rextent_protocol::status
extent_client::clear_handler(extent_protocol::extentid_t eid, int &) {
  int r;
  if (cache_table.find(eid) == cache_table.end()) {
    printf("client-clear[%s]: inode %llu is not cached here, this should not happen\n", cid.c_str(), eid);
    return rextent_protocol::RPCERR;
  }
  printf("client-clear[%s] remove inode %llu's data from cache\n", cid.c_str(), eid);
  cache_table[eid].isAttrCached = false;
  cache_table[eid].isDataCached = false;
  cache_table[eid].data.clear();
  return rextent_protocol::OK;
}