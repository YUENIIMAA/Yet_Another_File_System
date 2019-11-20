#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf) //part1A
{
  assert(id >= 0 && id < BLOCK_NUM);
  assert(buf);
  memcpy(buf,blocks[id],BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf) //part1A
{
  assert(id >= 0 && id < BLOCK_NUM);
  assert(buf);
  memcpy(blocks[id],buf,BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block() //Part1B
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  //check = IBLOCK(INODE_NUM,sb.nblocks) + 1
  pthread_mutex_lock(&bmlock);
  uint32_t base = IBLOCK(INODE_NUM,sb.nblocks) + 1;
  // 从inode_table后面的第一个块开始遍历直到找到第一个可以用的块。
  for(uint32_t check = 0;check < BLOCK_NUM; check++){
    if(using_blocks[check + base] == 0){
      using_blocks[check + base] = 1;
      pthread_mutex_unlock(&bmlock);
      return check + base;
    }
  }
  pthread_mutex_unlock(&bmlock);
  return 0;
}

void
block_manager::free_block(uint32_t id) //Part1B
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  // 简单地把块设置成可以被使用即可，感觉可以先不擦除数据。
  pthread_mutex_lock(&bmlock);
  if(using_blocks[id] == 0){ // Already freed
    printf("The block is already freed.\n");
    pthread_mutex_unlock(&bmlock);
    return;
  } else{
    using_blocks[id] = 0;
  }
  pthread_mutex_unlock(&bmlock);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();
  bmlock = PTHREAD_MUTEX_INITIALIZER;
  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM; // = DISK_SIZE = 16M
  sb.nblocks = BLOCK_NUM; //32K
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  imlock = PTHREAD_MUTEX_INITIALIZER;
  inode* root_dir = get_inode(1);
  if (root_dir != NULL) {
    printf("\tim: error! alloc first inode %d, should be 1\n", 0);// Modified
    exit(0);
  }
  root_dir = (struct inode*)malloc(sizeof(struct inode));
  root_dir->type = extent_protocol::T_DIR;
  root_dir->atime = (unsigned) time(0);
  root_dir->ctime = (unsigned) time(0);
  root_dir->mtime = (unsigned) time(0);
  root_dir->size = 0;
  put_inode(1,root_dir);
}

/* Create a new file.
 * Return its inum. */
// 写lab3的时候debug居然能一路de到这里我也是醉了，终于明白了lab3 testb的用意。
// 当在两个目录下分别海量创建文件时，由于inode未分配，无法给待创建的inode加锁，因为是不同的目录，同时都能拿到parent锁，
// 于是两个不同的create的RPC就有可能同时调用alloc_inode然后得到相同的inode号，从用户视角就是两个不同的文件指向了相同的inode，自然是错误的。
uint32_t
inode_manager::alloc_inode(uint32_t type) //part1A
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  uint32_t inode_start = 2;
  uint32_t target = inode_start;
  inode* new_inode;
  pthread_mutex_lock(&imlock);
  //从第2个inode开始查找inod_table中的空位。
  for(;target < INODE_NUM; target++){
    new_inode = get_inode(target);
    if(new_inode == NULL){
      new_inode = new inode();
      new_inode->type = (short)type;
      new_inode->size = 0;
      new_inode->atime = (unsigned) time(0);
      new_inode->ctime = (unsigned) time(0);
      new_inode->mtime = (unsigned) time(0);
      put_inode(target,new_inode);
      free(new_inode);
      pthread_mutex_unlock(&imlock);
      return target;
    }
  }
  pthread_mutex_unlock(&imlock);
  return 1;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  pthread_mutex_lock(&imlock);
  inode* node = get_inode(inum);
  // 拿到想要删除的inode。
  if (!node || node->type == 0){
    printf("The inode is already freed.\n");
    pthread_mutex_unlock(&imlock);
    return;
  } 
  node->type = 0;
  // 把type置零即可使得该inode在alloc_inode时因get_inode返回NULL而复用该inode。
  put_inode(inum,node);
  free(node);
  pthread_mutex_unlock(&imlock);
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);
  
  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);

}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size) //Part1B
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
  inode* node = get_inode(inum);
  assert(node != NULL);
  uint32_t fileSize = node->size;
  *size = node->size;
  int blocknum = FILE_BLOCK_NUM(*size);
  if ( blocknum > BLOCK_NUM){
    return;
  }
  char* file_data = (char *)malloc(fileSize);
  uint32_t readSize = 0;
  int i = 0;
  for (; i < NDIRECT && readSize < fileSize; i++){
    if (readSize + BLOCK_SIZE < fileSize){
      bm->read_block(node->blocks[i], file_data + readSize);
      readSize += BLOCK_SIZE;
    } else{
      char* buf = (char *) malloc(BLOCK_SIZE);
      int len = fileSize - readSize;
      bm->read_block(node->blocks[i],buf);
      memcpy(file_data + readSize,buf,len);
      readSize += len;
    }
  }
  if (readSize < fileSize){
    blockid_t indirectBlocks[BLOCK_SIZE];
    bm->read_block(node->blocks[NDIRECT],(char*)indirectBlocks);
    for(uint32_t j = 0;j < NINDIRECT && readSize < fileSize; j++){
      blockid_t inblock = indirectBlocks[j];
      if (readSize + BLOCK_SIZE < fileSize){
        bm->read_block(inblock,file_data + readSize);
        readSize += BLOCK_SIZE;
      } else{
        char* buf = (char *)malloc(BLOCK_SIZE);
        int len = fileSize - readSize;
        bm->read_block(inblock,buf);
        memcpy(file_data + readSize,buf,len);
        readSize += len;
      }
    }
  }
  node->atime = (unsigned) time(0);
  put_inode(inum,node);
  *buf_out = file_data;
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size) //Part1B
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */

  /*rewrite the function in lab3*/
  char block[BLOCK_SIZE];
  char indirect[BLOCK_SIZE];
  inode_t * ino = get_inode(inum);
  unsigned int oldBlockNum = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  unsigned int newBlockNum = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;

  // 需要释放块的情况。
  if (oldBlockNum > newBlockNum) {
    if (newBlockNum > NDIRECT) {
      bm->read_block(ino->blocks[NDIRECT], indirect);
      for (unsigned int i = newBlockNum; i < oldBlockNum; ++i) {
        bm->free_block(*((blockid_t *)indirect + (i - NDIRECT)));
      }
    } else {
      if (oldBlockNum > NDIRECT) {
        bm->read_block(ino->blocks[NDIRECT], indirect);
        for (unsigned int i = NDIRECT; i < oldBlockNum; ++i) {
          bm->free_block(*((blockid_t *)indirect + (i - NDIRECT)));
        }
        bm->free_block(ino->blocks[NDIRECT]);
        for (unsigned int i = newBlockNum; i < NDIRECT; ++i) {
          bm->free_block(ino->blocks[i]);
        }
      } else {
        for (unsigned int i = newBlockNum; i < oldBlockNum; ++i) {
          bm->free_block(ino->blocks[i]);
        }
      }
    }
  }

  // 需要额外分配块的情况。
  if (newBlockNum > oldBlockNum) {
    if (newBlockNum <= NDIRECT) {
      for (unsigned int i = oldBlockNum; i < newBlockNum; ++i) {
        ino->blocks[i] = bm->alloc_block();
      }
    } else {
      if (oldBlockNum <= NDIRECT) {
        for (unsigned int i = oldBlockNum; i < NDIRECT; ++i) {
          ino->blocks[i] = bm->alloc_block();
        }
        ino->blocks[NDIRECT] = bm->alloc_block();

        bzero(indirect, BLOCK_SIZE);
        for (unsigned int i = NDIRECT; i < newBlockNum; ++i) {
          *((blockid_t *)indirect + (i - NDIRECT)) = bm->alloc_block();
        }
        bm->write_block(ino->blocks[NDIRECT], indirect);
      } else {
        bm->read_block(ino->blocks[NDIRECT], indirect);
        for (unsigned int i = oldBlockNum; i < newBlockNum; ++i) {
          *((blockid_t *)indirect + (i - NDIRECT)) = bm->alloc_block();
        }
        bm->write_block(ino->blocks[NDIRECT], indirect);
      }
    }
  }

  // 分配完之后开始写入数据。
  int pos = 0;
  unsigned int i;
  for (i = 0; i < NDIRECT && pos < size; i++) {
    if (size - pos > BLOCK_SIZE) {
      bm->write_block(ino->blocks[i], buf + pos);
      pos += BLOCK_SIZE;
    } else {
      int len = size - pos;
      memcpy(block, buf + pos, len);
      bm->write_block(ino->blocks[i], block);
      pos += len;
    }
  }

  if (pos < size) {
    bm->read_block(ino->blocks[NDIRECT], indirect);
    for (i = 0; i < NINDIRECT && pos < size; i++) {
      blockid_t ix = *((blockid_t *)indirect + i);
      if (size - pos > BLOCK_SIZE) {
        bm->write_block(ix, buf + pos);
        pos += BLOCK_SIZE;
      } else {
        int len = size - pos;
        memcpy(block, buf + pos, len);
        bm->write_block(ix, block);
        pos += len;
      }
    }
  }

  // 更新inode的时间戳。
  ino->size = size;
  ino->mtime = time(0);
  ino->ctime = time(0);
  put_inode(inum, ino);
  free(ino);
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a) //part1A
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode* node = get_inode(inum);
  if(!node){
    return;
  }
  a.type = node->type;
  a.atime = node->atime;
  a.mtime = node->mtime;
  a.ctime = node->ctime;
  a.size = node->size;
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  // 删除基本复用之前分配和写入的代码，只不过此处alloc变成free。
  inode* node = get_inode(inum);
  int blocknum;
  blocknum = node->size==0 ? 0 : FILE_BLOCK_NUM(node->size);
  int i = 0;
  for(; i < blocknum && i < NDIRECT; i++){
    bm->free_block(node->blocks[i]);
  }
  if(i < blocknum){
    int freeSize = NDIRECT * BLOCK_SIZE;
    int fileSize = node->size;
    blockid_t indirectBlocks[BLOCK_SIZE];
    bm->read_block(node->blocks[NDIRECT],(char *)indirectBlocks); 
    for(uint32_t i = 0;i < NINDIRECT && freeSize < fileSize; i++,freeSize += BLOCK_SIZE){
      bm->free_block(indirectBlocks[i]);
    }
    bm->free_block(node->blocks[NDIRECT]);
  }
  free_inode(inum);
  free(node);
  return;
}
