#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  blockid_t unallocated_block_id;
  // 从inode_table后面的第一个块开始遍历直到找到第一个可以用的块。
  for (uint32_t i = IBLOCK(INODE_NUM, BLOCK_NUM) + 1; i < BLOCK_NUM; i++) {
    if (using_blocks[i] == 0) {
      unallocated_block_id = i;
      using_blocks[i] = 1;
      break;
    }
  }
  return unallocated_block_id;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  // 简单地把块设置成可以被使用即可，感觉可以先不擦除数据。
  using_blocks[id] = 0;
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
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
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  printf("\tdebug: allocating inode\n");
  inode_t *new_inode = (inode_t*)malloc(sizeof(inode_t));
  bzero(new_inode, sizeof(inode_t));

  // 初始化inode的时间戳。
  unsigned int current_time = (unsigned)time(NULL);
  new_inode->atime = current_time;
  new_inode->ctime = current_time;
  new_inode->mtime = current_time;

  // 初始化inode的大小。
  new_inode->size = 0;

  // 初始化inode的类型。
  new_inode->type = type;
  
  // inode的inode_to_block暂时不需要初始化。

  //从第1个inode开始查找inod_table中的空位。
  uint32_t inode_number;
  inode_t *temp;
  for (uint32_t i = 1; i < INODE_NUM; i++) { //注意此处要从1开始...
    temp = get_inode(i);
    if (temp != NULL) {
      continue;
    }
    else {
      inode_number = i;
      put_inode(i, new_inode);
      break;
    }
  }
  free(temp);
  free(new_inode);
  printf("\tdebug: allocating inode finished\n");
  return inode_number;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  // 拿到想要删除的inode。
  inode_t *inode = get_inode(inum);
  if (inode != NULL) {
    // 把type置零即可使得该inode在alloc_inode时因get_inode返回NULL而复用该inode。
    inode->type = 0;
    put_inode(inum, inode);
  }
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
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
  printf("\tdebug: start reading\n");
  inode_t *inode = get_inode(inum);
  if (inode != NULL) {
    printf("\tdebug: vaild inode number checked\n");
    unsigned int num_of_bytes = inode->size;
    // 算一下这个inode使用了多少个块。
    printf("\tdebug: inode size is %u\n", num_of_bytes);
    unsigned int num_of_blocks_used = num_of_bytes / BLOCK_SIZE;
    if (num_of_bytes % BLOCK_SIZE != 0) {num_of_blocks_used++;}
    printf("\tdebug: number of blocks used is %u\n", num_of_blocks_used);
    
    *buf_out = new char[num_of_blocks_used * BLOCK_SIZE];
    bzero(*buf_out, num_of_blocks_used * BLOCK_SIZE);
    for (unsigned int i = 0; i < NDIRECT && i < num_of_blocks_used; i++) {
      printf("\tdebug: reading direct block[%u] <-- block[%u]\n", i, inode->blocks[i]);
      bm->read_block(inode->blocks[i], *buf_out + i * BLOCK_SIZE);
    }
    if (num_of_blocks_used > NDIRECT) {
      uint *indirect_block = new uint[NINDIRECT];
      bm->read_block(inode->blocks[NDIRECT], (char *)indirect_block);
      for (unsigned int i = 0; i < num_of_blocks_used - NDIRECT; i++) {
        printf("\tdebug: reading indirect block[%u] <-- block[%u]\n", i, indirect_block[i]);
        bm->read_block(indirect_block[i], *buf_out + (NDIRECT + i) * BLOCK_SIZE);
      }
    }
    *size = inode->size;
    printf("\tdebug: %u written into size\n", inode->size);
    printf("\tdebug: finished reading\n");
  }
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  printf("\tdebug: start writing\n");
  inode_t *inode = get_inode(inum);
  // 修改inode的时间戳。
  printf("\tdebug: updating time stamp\n");
  unsigned int current_time = (unsigned)time(NULL);
  inode->atime = current_time;
  inode->ctime = current_time;
  inode->mtime = current_time;
  printf("\tdebug: time stamp updated\n");
  // 修改inode的大小。
  printf("\tdebug: updating size\n");
  unsigned int old_size = inode->size;
  inode->size = (unsigned)size;
  printf("\tdebug: size changed from %u to %u\n", old_size, size);
  // 计算之前用了多少个块和当前写入需要多少个块。
  printf("\tdebug: calculating blocks needed\n");
  unsigned int num_of_blocks_used = old_size / BLOCK_SIZE;
  if (old_size % BLOCK_SIZE != 0) {num_of_blocks_used++;}
  printf("\tdebug: num of blocks used %u\n", num_of_blocks_used);
  unsigned int num_of_blocks_to_use = size / BLOCK_SIZE;
  if (size % BLOCK_SIZE != 0) num_of_blocks_to_use++; 
  printf("\tdebug: num of blocks to use %u\n", num_of_blocks_to_use);
  
  if (num_of_blocks_to_use > num_of_blocks_used) {
    // 需要额外分配块的情况。
    printf("\tdebug: allocating new blocks");
    if (num_of_blocks_to_use <= NDIRECT) {
      // 全部新分配的块都在直接块里。
      printf("[direct blocks only]\n");
      for (unsigned int i = num_of_blocks_used; i < num_of_blocks_to_use; ++i) {
        printf("\tdebug: -->allocating block[%d] from %u to ", i, inode->blocks[i]);
        inode->blocks[i] = bm->alloc_block();
        printf("%u\n", inode->blocks[i]);
      }
      printf("\tdebug: new blocks allocated\n");
    }
    else if (num_of_blocks_used >= NDIRECT) {
      // 全部新分配的块都在间接块里。
      printf("[indirect blocks only]\n");
      uint *indirect_block = new uint[NINDIRECT];
      bm->read_block(inode->blocks[NDIRECT], (char *)indirect_block);
      for (unsigned int i = num_of_blocks_used - NDIRECT; i < num_of_blocks_to_use - NDIRECT; i++) {
        indirect_block[i] = bm->alloc_block();
      }
      bm->write_block(inode->blocks[NDIRECT], (char *)indirect_block);
    }
    else {
      // 部分新分配的块在直接块，部分新分配的块在间接块。
      printf("[direct blocks + indirect blocks]\n");
      for (unsigned int i = num_of_blocks_used; i < NDIRECT; i++) {
        inode->blocks[i] = bm->alloc_block();
      }
      uint *indirect_block = new uint[NINDIRECT];
      for (unsigned int i = 0; i < num_of_blocks_to_use - NDIRECT; i++) {
        indirect_block[i] = bm->alloc_block();
      }
      bm->write_block(inode->blocks[NDIRECT], (char *)indirect_block);
    }
  }
  else if (num_of_blocks_to_use < num_of_blocks_used) {
    printf("\tdebug: freeing no longer used blocks");
    // 需要释放块的情况。
    if (num_of_blocks_to_use > NDIRECT) {
      // 仍要使用间接块，只释放间接块即可。
      uint *indirect_block = new uint[NINDIRECT];
      bm->read_block(inode->blocks[NDIRECT], (char *)indirect_block);
      for (unsigned int i = num_of_blocks_to_use - NDIRECT; i < num_of_blocks_used - NDIRECT; i++) {
        bm->free_block(indirect_block[i]);
        indirect_block[i] = NULL;
      }
      bm->write_block(inode->blocks[NDIRECT], (char *)indirect_block);
    }
    else if (num_of_blocks_used <= NDIRECT) {
      // 原先就没有间接块，因此只释放直接块就好。
      for (unsigned int i = num_of_blocks_to_use; i < num_of_blocks_used; i++) {
        bm->free_block(inode->blocks[i]);
        inode->blocks[i] = NULL;
      }
    }
    else {
      // 既要释放间接块又要释放直接块，即释放全部的间接块和部分直接块。
      uint *indirect_block = new uint[NINDIRECT];
      bm->read_block(inode->blocks[NDIRECT], (char *)indirect_block);
      for (unsigned int i = 0; i < num_of_blocks_used - NDIRECT; i++) {
        bm->free_block(indirect_block[i]);
        indirect_block[i] = NULL;
      }
      // 依次释放完间接块中指向的块之后，把间接块也释放掉。
      bm->free_block(inode->blocks[NDIRECT]);
      // 最后释放直接块。
      for (unsigned int i = num_of_blocks_to_use; i < NDIRECT; i++) {
        bm->free_block(inode->blocks[i]);
        inode->blocks[i] = NULL;
      }
    }
  }
  // 块数不发生变更的情况直接往里写就是了，因此省略。
  // 分配完之后开始写入数据。
  printf("\tdebug: writing direct blocks\n");
  for (unsigned int i = 0; i < num_of_blocks_to_use && i < NDIRECT; i++) {
    bm->write_block(inode->blocks[i], buf + i * BLOCK_SIZE);
  }
  if (num_of_blocks_to_use > NDIRECT) {
    printf("\tdebug: writing indirect blocks\n");
    uint *indirect_block = new uint[NINDIRECT];
    bm->read_block(inode->blocks[NDIRECT], (char *)indirect_block);
    for (unsigned int i = 0;i < num_of_blocks_to_use - NDIRECT;i++) {
      bm->write_block(indirect_block[i], buf + (NDIRECT + i) * BLOCK_SIZE);
    }
  }
  printf("\tdebug: updating inode\n");
  put_inode(inum, inode);
  printf("\tdebug: finished writing\n");
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode_t *inode = get_inode(inum);
  if (inode != NULL) {
    a.atime = inode->atime;
    a.ctime = inode->ctime;
    a.mtime = inode->mtime;
    a.size = inode->size;
    a.type = inode->type;
    return;
  }
  else {
    return;
  }
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  // 删除基本复用之前[direct blocks + indirect blocks]和写入的代码，只不过此处alloc变成free。
  printf("\tdebug: start deleting\n");
  inode_t *inode = get_inode(inum);
  unsigned size = inode->size;
  printf("\tdebug: calculating blocks needed\n");
  unsigned int num_of_blocks_to_free = size / BLOCK_SIZE;
  if (size % BLOCK_SIZE != 0) {num_of_blocks_to_free++;}
  printf("\tdebug: freeing direct blocks\n");
  for (unsigned int i = 0; i < num_of_blocks_to_free && i < NDIRECT; i++) {
    bm->free_block(inode->blocks[i]);
  }
  if (num_of_blocks_to_free > NDIRECT) {
    printf("\tdebug: freeing indirect blocks\n");
    uint *indirect_block = new uint[NINDIRECT];
    bm->read_block(inode->blocks[NDIRECT], (char *)indirect_block);
    for (unsigned int i = 0;i < num_of_blocks_to_free - NDIRECT;i++) {
      bm->free_block(indirect_block[i]);
    }
    bm->free_block(inode->blocks[NDIRECT]);
  }
  printf("\tdebug: freeing inode\n");
  free_inode(inum);
  printf("\tdebug: finished deleting\n");
  return;
}
