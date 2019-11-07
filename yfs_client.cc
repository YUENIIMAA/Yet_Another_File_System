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

yfs_client::yfs_client()
{
    ec = new extent_client();

}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
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
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
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
    return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
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
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

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
    bool found;
    if (lookup(parent, name, found, ino_out) != extent_protocol::OK) {
        if (found) {
            printf("create: file already exists");
            r = EXIST;
            return r;
        }
        else {
            printf("create: something wrong with lookup");
            r = IOERR;
            return r;
        }
    }
    else {
        printf("create: file is ok to be created");
    }
    if (ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK) {
        r = IOERR;
        printf("create: failed to create file");
        return r;
    }
    else {
        printf("create: file created");
        std::string parent_entries;
        if (ec->get(parent, parent_entries) == extent_protocol::OK) {
            printf("create: updating parent");
            parent_entries.append(name);
            parent_entries.append("/");
            parent_entries.append(filename(ino_out));
            parent_entries.append("/");
            if (ec->put(parent, parent_entries) == extent_protocol::OK) {
                printf("create: parent updated");
            }
            else {
                r = IOERR;
                printf("create: failed to update parent");
                return r;
            }
        }
        else {
            r = IOERR;
            printf("create: failed to read parent");
            return r;
        }
    }
    printf("create: job finished");
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

    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    std::list<dirent> dir_entries;
    if (readdir(parent, dir_entries) == extent_protocol::OK) {
        printf("lookup: readdir returned OK\n");
        std::list<dirent>::iterator it;
        for (it = dir_entries.begin(); it != dir_entries.end(); it++) {
            printf("look up: extract entry -> name:%s, inum: %llu\n", it->name.c_str(), it->inum);
            if (!it->name.compare(name)) {
                printf("lookup: got match");
                found = true;
                ino_out = it->inum;
                r = EXIST;
                printf("lookup: job finished");
                return r;
            }
        }
        found = false;
        printf("lookup: no match");
        printf("lookup: job finished");
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
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    // 获取dir这个inode的属性。
    extent_protocol::attr dir_attributes;
    ec->getattr(dir, dir_attributes);
    // 检查这个inode是不是目录。
    if (dir_attributes.type != extent_protocol::T_DIR) {
        printf("readdir: inode read is not a dir\n");
        r = IOERR;
        return r;
    }
    else {
        printf("readdir: inode is a dir: %s\n");
        std::string dir_entries;
        ec->get(dir, dir_entries);
        printf("readdir: got dir entries: %s\n", dir_entries.c_str());
        std::string entry_name, entry_inum;
        int former_slash = 0, latter_slash = 0;
        while (former_slash < dir_entries.size()) {
            // 从前一个/开始找下一个/，二者之间的先是name后是inum，应该是成对出现的。
            latter_slash = dir_entries.find('/', former_slash);
            entry_name = dir_entries.substr(former_slash, latter_slash - former_slash);
            former_slash = latter_slash + 1;
            latter_slash = dir_entries.find('/', former_slash);
            entry_inum = dir_entries.substr(former_slash, latter_slash - former_slash);
            former_slash = latter_slash + 1;
            // 创建新的entry扔进list里。
            printf("readdir: extract entry -> name:%s, inum:%s\n", entry_name.c_str(),entry_inum.c_str());
            dirent dir_entry;
            dir_entry.name = entry_name;
            dir_entry.inum = n2i(entry_inum);
            list.push_back(dir_entry);
        }
    }
    printf("readdir: job finished");
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

    return r;
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

    return r;
}

