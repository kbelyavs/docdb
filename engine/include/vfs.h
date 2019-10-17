#ifndef ENGINE_INCLUDE_VFS_H_
#define ENGINE_INCLUDE_VFS_H_
/*
MIT License

Copyright (c) 2019 Konstantin Belyavskiy

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#include <dirent.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <iostream>
#include <vector>
#include <string>
#include <unordered_set>
#include <map>
#include <mutex>

#include "docdb.h"

std::string current_dir() {
    char cwd[PATH_MAX];
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        return std::string(cwd);
    } else {
        std::cerr << "getcwd() error: " << std::strerror(errno) << std::endl;
        exit(-1);
    }
}

void touch_dir(const std::string &path) {
    struct stat info;
    if (stat(path.c_str(), &info) == 0 && info.st_mode & S_IFDIR)
        return;
    if (mkdir(path.c_str(), 0777) != 0) {
        std::cerr << "touch_dir() error: " << std::strerror(errno) << std::endl;
        exit(-1);
    }
}

void get_files(const std::string &path, std::vector<std::string> *files) {
    DIR *dir;
    struct dirent *ent;
    if ((dir = opendir(path.c_str())) != NULL) {
        while ((ent = readdir(dir)) != NULL)
            files->push_back(ent->d_name);
        closedir(dir);
    } else {
        std::cerr << "get_files() error: " << std::strerror(errno) << std::endl;
        exit(-1);
    }
}

bool check_format(const std::string &name) {
    if (name.length() != 23 || name.substr(20, 3) != ".db")
        return false;
    for (int i = 0; i < 20; i++)
        if (!isdigit(name.c_str()[i]))
            return false;
    return true;
}

using std::mutex;
using std::lock_guard;

struct header {
    int32_t offset;
    int32_t size;
};

struct FileDB {
    header hd[10];
};

class VFS {
 public:
    VFS(): path(current_dir() + "/db") { load(); }
    bool exists(ID id) const {lock_guard<mutex> l(mtx); return cache.count(id);}
    int get(ID id, Document* doc) const {return 0;}
    int remove(ID id) {return 0;}
    int update(ID id, const std::string& data) {return 0;}
    int insert(const Document& doc) {return 0;}
 private:
    void load();
    void load_file(const std::string&);
    std::string path;
    mutable mutex mtx;
    std::map<ID, int> space;
    std::unordered_set<ID> cache;
};

void VFS::load() {
    std::vector<std::string> files;
    touch_dir(path);
    get_files(path, &files);
    for (auto file : files)
        if (check_format(file)) {
            std::cout << file << std::endl;
            load_file(file);
        }
}

void VFS::load_file(const std::string &file) {
    struct stat info;
    std::string fullpath = path + "/" + file;
    if (stat(fullpath.c_str(), &info) == 0 && info.st_mode & S_IFREG) {
        std::cout << file << " " << fullpath << std::endl;
        ID id = stoll(file);
        std::cout << id << std::endl;
        int nrecords = 0;  // TODO(kbelyavs): add code, feel cache
        lock_guard<mutex> l(mtx);
        space[id] = nrecords;
    }
}

#endif  // ENGINE_INCLUDE_VFS_H_
