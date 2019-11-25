// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst);
  lc->acquire(1);
  if (ec->put(1, "") != extent_protocol::OK) {
      printf("error init root dir\n"); // XYB: init root dir
  }
  lc->release(1);
}


yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    lc->acquire(inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        lc->release(inum);
        return false;
    }
    lc->release(inum);
    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a file\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
yfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    // 否，因为多了SYMLINK所以非文件并不代表就是目录。
    extent_protocol::attr a;
    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        lc->release(inum);
        return false;
    }
    lc->release(inum);
    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a dir\n", inum);
        return true;
    } 
    printf("isdir: %lld is not a dir\n", inum);
    return false;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;
    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    // 工作完不管对不对都把锁放掉。
    lc->release(inum);
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;
    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;
release:
    lc->release(inum);
    return r;
}


//#define EXT_RPC(xx) do { \
//    if ((xx) != extent_protocol::OK) { \
//        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
//        r = IOERR; \
//        goto release; \
//    } \
//} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    // 现在直到为啥给的代码里要用goto release这样的写法了，不然每个return都要配一个解锁代码。
    // 已全部修改为goto release的写法。
    // 然后又全部改了回去。
    printf("setattr: job started\n");
    std::string data;
    //extent_protocol::attr inode_attributes;
    lc->acquire(ino);
    if (ec->get(ino, data) == extent_protocol::OK) {
        printf("setattr: inode data fetched\n");
        if (data.size() > size) {
            printf("setattr: shrinking data\n");
            data = data.substr(0, size);
        }
        else if (data.size() < size) {
            printf("setattr: expanding data\n");
            data.append(std::string(size - data.size(), '\0'));
        }
        else {
            printf("setattr: no need to change data\n");
        }
        printf("setattr: writing back data\n");
        if (ec->put(ino, data) == extent_protocol::OK) {
            printf("setattr: done writing\n");
        }
        else {
            printf("setattr: failed to write back data\n");
            r = IOERR;
            goto release;
        }
    }
    else {
        printf("setattr: failed to fetch data\n");
        r = IOERR;
        goto release;
    }
    printf("setattr: job done\n");
release:
    lc->release(ino);
    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    // 由于使用了inode的编号作为锁的编号，因此lock 0是不会被某个文件or目录使用的，这样就可以用lock 0作为特殊的锁。
    // 需要lock 0是因为在create中用到了lookup，lookup的锁是他自己管的，在返回前它就把锁给放了，这样我无法保证另一个create不会在我修改它前拿到锁。
    // 有了lock 0我可以保证两个create不会交错执行。
    // 修正：上面的想法就tm是个全局锁沃日。
    // 修正：仔细一想lookup自己拿锁没有任何意义，看一眼放掉那改的时候一定还会有并行的问题、、、拿锁应该是调它的人干的活。
    printf("create: create %s in dir %llu\n", name, parent);
    bool found;
    lc->acquire(parent);
    if (_lookup(parent, name, found, ino_out) == extent_protocol::OK) {
        if (found) {
            lc->release(parent);
            printf("create: file name already exists\n");
            r = EXIST;
            return r;
        }
        printf("create: file is ok to be created\n");
    }
    else {
        printf("create: something wrong with lookup\n");
        r = IOERR;
        lc->release(parent);
        return r;
    }
    extent_protocol::extentid_t id;
    extent_protocol::attr attr;
    if (ec->create(extent_protocol::T_FILE, id) != extent_protocol::OK) {
        r = IOERR;
        printf("create: failed to create file\n");
        lc->release(parent);
        return r;
    }
    else {
        ino_out = id;
        lc->acquire(id);
        std::string blank;
        ec->get(id, blank);
        lc->release(id);
        printf("create: file created, new inode id:%llu\n", ino_out);
        std::string parent_entries;
        if (ec->get(parent, parent_entries) == extent_protocol::OK) {
            printf("create: updating parent\n");
            parent_entries.append(name);
            parent_entries.append("/");
            parent_entries.append(filename(ino_out));
            parent_entries.append("/");
            if (ec->put(parent, parent_entries) == extent_protocol::OK) {
                lc->release(parent);
                printf("create: parent updated\n");
                r = OK;
                printf("create: job done\n");
                return r;
            }
            else {
                lc->release(parent);
                r = IOERR;
                printf("create: failed to update parent\n");
                return r;
            }
        }
        else {
            lc->release(parent);
            r = IOERR;
            printf("create: failed to read parent\n");
            return r;
        }
    }
    lc->release(parent);
    r = IOERR;
    printf("create: you should not see this message\n");
    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    printf("mkdir: job started\n");
    bool found = false;
    lc->acquire(parent);
    if (_lookup(parent, name, found, ino_out) == extent_protocol::OK) {
        if (found) {
            lc->release(parent);
            printf("mkdir: dir name already exists\n");
            return EXIST;
        }
        printf("mkdir: dir is ok to be created\n");
    }
    else {
        lc->release(parent);
        printf("mkdir: something wrong with lookup\n");
        return IOERR;
    }
    printf("mkdir: creating new inode for dir\n");
    extent_protocol::extentid_t id;
    if (ec->create(extent_protocol::T_DIR, id) != extent_protocol::OK) {
        printf("mkdir: failed to create new inode\n");
        return IOERR;
    }
    ino_out = id;
    std::string parent_entries;
    if (ec->get(parent, parent_entries) == extent_protocol::OK) {
        printf("mkdir: parent inode entries data fetched\n");
        parent_entries.append(name);
        parent_entries.append("/");
        parent_entries.append(filename(ino_out));
        parent_entries.append("/");
        printf("mkdir: updating parent inode entries data\n");
        if (ec->put(parent, parent_entries) == extent_protocol::OK) {
            lc->release(parent);
            printf("mkdir: parent inode entries data updated\n");
            return OK;
        }
        else {
            lc->release(parent);
            printf("mkdir: failed to update parent inode\n");
            return IOERR;
        }
    }
    else {
        lc->release(parent);
        printf("mkdir: failed to fetch parent inode data\n");
        return IOERR;
    }
    printf("mkdir: job done\n");
    lc->release(parent);
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    lc->acquire(parent);
    int r = _lookup(parent, name, found, ino_out);
    lc->release(parent);
    return r;
}

int
yfs_client::_lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    printf("lookup: lookup %s in dir %llu\n", name, parent);
    std::list<dirent> dir_entries;
    if (_readdir(parent, dir_entries) == extent_protocol::OK) {
        printf("lookup: readdir returned OK\n");
        std::list<dirent>::iterator it;
        for (it = dir_entries.begin(); it != dir_entries.end(); it++) {
            //printf("look up: extract entry -> name:%s, inum: %llu\n", it->name.c_str(), it->inum);
            if (!it->name.compare(name)) {
                printf("lookup: got match\n");
                found = true;
                ino_out = it->inum;
                r = OK;
                printf("lookup: job done\n");
                return r;
            }
        }
        found = false;
        printf("lookup: no match\n");
        r = OK;
        printf("lookup: job done\n");
    }
    else {
        printf("lookup: readdir didn't return OK\n");
        r = IOERR;
        return r;
    }
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    lc->acquire(dir);
    int r = _readdir(dir, list);
    lc->release(dir);
    return r;
}

int
yfs_client::_readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    // 获取dir这个inode的属性。
    printf("readdir: read dir %llu\n", dir);
    extent_protocol::attr dir_attributes;
    //ec->getattr(dir, dir_attributes);
    // 检查这个inode是不是目录。
    //if (dir_attributes.type != extent_protocol::T_DIR) {
    //    printf("readdir: inode read is not a dir\n");
    //    r = IOERR;
    //    return r;
    //}
    //else {
        printf("readdir: inode is a dir\n");
        std::string dir_entries;
        ec->get(dir, dir_entries);
        //printf("readdir: will analyze dir entries: %s\n", dir_entries.c_str());
        std::string entry_name, entry_inum;
        unsigned int former_slash = 0, latter_slash = 0;
        while (former_slash < dir_entries.size()) {
            // 从前一个/开始找下一个/，二者之间的先是name后是inum，应该是成对出现的。
            latter_slash = dir_entries.find('/', former_slash);
            entry_name = dir_entries.substr(former_slash, latter_slash - former_slash);
            former_slash = latter_slash + 1;
            latter_slash = dir_entries.find('/', former_slash);
            entry_inum = dir_entries.substr(former_slash, latter_slash - former_slash);
            former_slash = latter_slash + 1;
            // 创建新的entry扔进list里。
            //printf("readdir: dir%llu/%s --> inode:%s\n", dir, entry_name.c_str(),entry_inum.c_str());
            dirent dir_entry;
            dir_entry.name = entry_name;
            dir_entry.inum = n2i(entry_inum);
            list.push_back(dir_entry);
        }
    //}
    printf("readdir: job done\n");
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    printf("read: job started\n");
    size_t remaining;
    std::string inode_data;
    lc->acquire(ino);
    if (ec->get(ino, inode_data) != extent_protocol::OK) {
        lc->release(ino);
        printf("read: failed to read data\n");
        return IOERR;
    }
    lc->release(ino);
    if ((size_t)off >= inode_data.size()) {
        printf("read: offset is larger than size\n");
        return IOERR;
    }
    data = inode_data.substr(off, size);
    printf("read: job done\n");
    return OK;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    printf("write: job started\n");
    bool needsfill = false;
    std::string inode_data;
    lc->acquire(ino);
    if (ec->get(ino, inode_data) != extent_protocol::OK) {
        lc->release(ino);
        printf("write: failed to read data\n");
        return IOERR;
    }
    if ((size_t)off > inode_data.size()) {
        printf("write: offset larger than size\n");
        needsfill = true;
    }
    std::string to_write;
    to_write.assign(data, size);
    if (needsfill) {
        printf("write: preparing data with gap\n");
        std::string gap(off - inode_data.size(), '\0');
        size_t oldSize = inode_data.size();
        inode_data.append(gap);
        inode_data.append(to_write);
        bytes_written = size + off - oldSize;
    }
    else if (off + size < inode_data.size()) {
        printf("write: preparing data with only middle part updated\n");
        std::string former_data, latter_data;
        former_data = inode_data.substr(0, off);
        latter_data = inode_data.substr(off + size, inode_data.size());
        former_data.append(to_write);
        former_data.append(latter_data);
        inode_data = former_data;
        bytes_written = size;
    }
    else {
        printf("write: preparing data with the right part updated\n");
        std::string former_data;
        former_data = inode_data.substr(0, off);
        former_data.append(to_write);
        inode_data = former_data;
        bytes_written = size;
    }
    if (ec->put(ino, inode_data) == extent_protocol::OK) {
        lc->release(ino);
        printf("write: writing success\n");
        return OK;
    }
    else {
        lc->release(ino);
        printf("write: failed to write new inode data\n");
        return IOERR;
    }
    printf("write: job done\n");
    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    printf("unlink: delete %s from dir%llu\n", name, parent);
    std::string parent_entries;
    lc->acquire(parent);
    if (ec->get(parent, parent_entries) == extent_protocol::OK) {
        printf("unlink: updating parent:dir%llu\n", parent);
        //printf("unlink: old entry:%s\n", parent_entries.c_str());
        std::string entry_name, entry_inum;
        unsigned int former_slash = 0, latter_slash = 0;
        unsigned int former_checkpoint = 0, latter_checkpoint = 0;
        bool found = false;
        while (former_slash < parent_entries.size()) {
            // 从前一个/开始找下一个/，二者之间的先是name后是inum，应该是成对出现的。
            former_checkpoint = former_slash;
            latter_slash = parent_entries.find('/', former_slash);
            entry_name = parent_entries.substr(former_slash, latter_slash - former_slash);
            former_slash = latter_slash + 1;
            latter_slash = parent_entries.find('/', former_slash);
            entry_inum = parent_entries.substr(former_slash, latter_slash - former_slash);
            latter_checkpoint = latter_slash + 1;
            former_slash = latter_slash + 1;
            if (!entry_name.compare(name)) {
                found = true;
                break;
            }
        }
        if (found) {
            std::string former_part, latter_part;
            former_part = parent_entries.substr(0, former_checkpoint);
            latter_part = parent_entries.substr(latter_checkpoint);
            former_part.append(latter_part);
            parent_entries = former_part;
            //printf("unlink: new entry:%s\n", parent_entries.c_str());
            if (ec->put(parent, parent_entries) == extent_protocol::OK) {
                printf("unlink: parent updated\n");
                printf("unlink: removing inode\n");
                lc->acquire(n2i(entry_inum));
                if (ec->remove(n2i(entry_inum)) == extent_protocol::OK) {
                    lc->release(n2i(entry_inum));
                    lc->release(parent);
                    printf("unlink: inode removed\n");
                    return OK;
                }
                else {
                    lc->release(n2i(entry_inum));
                    lc->release(parent);
                    printf("unlink: failed to remove inode\n");
                    return IOERR;
                }
            }
            else {
                lc->release(parent);
                printf("unlink: failed to update parent\n");
                return IOERR;
            }
        }
        else {
            lc->release(parent);
            printf("unlink: did't find the file to delete, it may have been deleted\n");
            return OK;
        }
    }
    else {
        lc->release(parent);
        printf("unlink: failed to read parent\n");
        return IOERR;
    }
    printf("unlink: you should not see this message\n");
    lc->release(parent);
    return r;
}

int yfs_client::symlink(inum parent, const char *name, const char *link, inum &ino_out) {
    int r = OK;
    printf("symlink: create symlink %s-->%s\n", name, link);
    std::string parent_entries;
    bool found;
    lc->acquire(parent);
    if (_lookup(parent, name, found, ino_out) == extent_protocol::OK) {
        if (found) {
            lc->release(parent);
            printf("symlink: symlink name already exists\n");
            return EXIST;
        }
        printf("symlink: symlink is ok to be created\n");
    }
    else {
        lc->release(parent);
        printf("symlink: something wrong with lookup\n");
        return IOERR;
    }
    printf("symlink: creating inode for symlink\n");
    extent_protocol::extentid_t id;
    if (ec->create(extent_protocol::T_SYMLINK, id) != extent_protocol::OK) {
        lc->release(parent);
        printf("symlink: failed to create symlink\n");
        return IOERR;
    }
    else {
        ino_out = id;
        printf("symlink: inode for symlink created\n");
        lc->acquire(id);
        if (ec->put(id, std::string(link)) != extent_protocol::OK) {
            lc->release(id);
            lc->release(parent);
            printf("symlink: failed to write the data of symlink\n");
            return IOERR;
        }
        else {
            lc->release(id);
            printf("symlink: inode for symlink written\n");
            if (ec->get(parent, parent_entries) == extent_protocol::OK) {
                printf("symlink: updating parent\n");
                parent_entries.append(name);
                parent_entries.append("/");
                parent_entries.append(filename(id));
                parent_entries.append("/");
                if (ec->put(parent, parent_entries) == extent_protocol::OK) {
                    lc->release(parent);
                    printf("symlink: parent updated\n");
                    return OK;
                }
                else {
                    lc->release(parent);
                    printf("symlink: failed to update parent\n");
                    return IOERR;
                }
            }
            else {
                lc->release(parent);
                printf("symlink: failed to read parent\n");
                return IOERR;
            }
        }
    }
    printf("symlink: job done\n");
    lc->release(parent);
    return r;
}

int yfs_client::readlink(inum ino, std::string &link) {
    int r = OK;
    std::string buf;
    lc->acquire(ino);
    printf("readlink: job started\n");
    if (ec->get(ino, buf) != extent_protocol::OK) {
        lc->release(ino);
        r = IOERR;
        printf("readlink: failed to get file\n");
        return IOERR;
    }
    link = buf;
    printf("readlink: job done\n");
    lc->release(ino);
    return r;
};
