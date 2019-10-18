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

#include <fstream>
#include <iostream>
#include <vector>
#include <string>
#include <unordered_set>
#include <map>
#include <mutex>

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

int read_file(const std::string &path, char *buf, size_t size,
              size_t offset = 0) {
    struct stat info;
    if (stat(path.c_str(), &info) == 0 && info.st_mode & S_IFREG) {
        std::ifstream file(path);
        if (offset)
            file.seekg(offset);
        file.read(buf, size);
        if (file.good())
            return 0;
    }
    return -1;
}

int write_file(const std::string &path, char *buf, size_t size,
               size_t offset = 0) {
    struct stat info;
    if ((stat(path.c_str(), &info) == 0 && info.st_mode & S_IFREG) || offset) {
        std::ofstream outfile;
        outfile.open(path);
        if (offset)
            outfile.seekp(offset);
        outfile.write(buf, size);
        if (outfile.good())
            return 0;
    }
    return -1;
}

int remove_file(const std::string &path) {
    return remove(path.c_str());
}

int rename_file(const std::string &oldname, const std::string &newname) {
    return rename(oldname.c_str(), newname.c_str());
}

// Up - fs, Bottom - VFS engine

const int NFILES = 10;

const char FILE_EXT[] = ".db";
const int EXT_LEN = strlen(FILE_EXT);
const int NDIGITS = 20;
const int FLENGTH = NDIGITS + EXT_LEN;

bool check_format(const std::string &name) {
    if (name.length() != FLENGTH || name.substr(NDIGITS, EXT_LEN) != FILE_EXT)
        return false;
    for (int i = 0; i < NDIGITS; i++)
        if (!isdigit(name.c_str()[i]))
            return false;
    return true;
}

struct Entry {
    int32_t offset;
    int32_t size;
    ID id;
};

struct FileHeader {
    Entry header[NFILES];
};

using std::mutex;
using std::lock_guard;
using ID = int64_t;

class VFS {
 public:
    VFS(): path(current_dir() + "/db") { recover(); }
    bool exists(ID id) const {lock_guard<mutex> l(mtx); return cache.count(id);}
    int get(ID, std::string&) const;
    int remove(ID);
    int update(ID id, const std::string& data) {return 0;}
    int insert(ID id, const std::string& data) {return 0;}
 private:
    void recover();
    void recover_file(const std::string&);
    int read_header(ID) const;
    ID find_file(ID, std::string&) const;
    std::string path;
    mutable mutex mtx;
    std::map<ID, int> space;  // keep number of entries in closest DB file
    std::unordered_set<ID> cache;  // to check if record exists
};

ID VFS::find_file(ID id, std::string &fullpath) const {
    if (!exists(id))
        return -1;
    auto it = space.upper_bound(id);
    if (it == space.begin()) {
        std::cerr << "Critical error, exists in cache, but no entry in space";
        return -2;
    }
    --it;
    if (it->second == 0) {
        std::cerr << "Error, exists in cache, but no records in space";
        return -2;
    }
    ID file_id = it->first;
    char name[FLENGTH + 1];
    snprintf(name, FLENGTH + 1, "%020lld", file_id);
    fullpath = path + "/" + std::string(name);
    std::cout << "for record " << id << " read file " << fullpath << std::endl;
    return file_id;
}

int VFS::get(ID id, std::string& data) const {
    lock_guard<mutex> l(mtx);
    std::string fullpath;
    if (find_file(id, fullpath) < 0)
        return -1;
    FileHeader hdr;
    if (read_file(fullpath, reinterpret_cast<char*>(&hdr), sizeof(hdr)) == 0) {
        for (int i = 0; i < NFILES; i++) {
            if (hdr.header[i].offset == 0 || hdr.header[i].id > id) {
                break;
            } else if (hdr.header[i].id == id) {
                size_t size = hdr.header[i].size;
                size_t offset = hdr.header[i].offset;
                std::vector<char> cbuf(size);
                if (read_file(fullpath, cbuf.data(), size, offset) != 0)
                    return -1;
                data = std::string(cbuf.data(), size);
                return 0;
            }
        }
    }
    return -1;
}

int VFS::remove(ID id) {
    lock_guard<mutex> l(mtx);
    std::string fullpath;
    ID file_id = find_file(id, fullpath);
    cache.erase(id);  // in any case, remove entry
    if (file_id < 0)
        return -1;
    if (space[file_id] < 2) {  // simply remove file, since only one record
        space.erase(file_id);
        return remove_file(fullpath);
    }
    FileHeader hdr;
    if (read_file(fullpath, reinterpret_cast<char*>(&hdr), sizeof(hdr)) == 0) {
        for (int i = 0; i < NFILES; i++) {
            if (hdr.header[i].offset == 0 || hdr.header[i].id > id) {
                break;
            } else if (hdr.header[i].id == id) {
                size_t size = hdr.header[i].size, bytes_total = 0;
                int j, offset = hdr.header[i].offset;
                for (j = i+1; j < NFILES; j++) {
                    if (hdr.header[j].offset == 0)
                        break;
                    hdr.header[j].offset -= size;
                    bytes_total += hdr.header[j].size;
                }
                if (j != i+1)  // to shift the rest of the entries if any
                    memmove(reinterpret_cast<Entry*>(&hdr) + i,
                            reinterpret_cast<Entry*>(&hdr) + i+1,
                            sizeof(Entry) * (j - i - 1));
                hdr.header[j].offset = 0;
                write_file(fullpath, reinterpret_cast<char*>(&hdr),
                           sizeof(hdr));
                std::vector<char> cbuf(size);
                read_file(fullpath, cbuf.data(), bytes_total, offset+size);
                write_file(fullpath, cbuf.data(), bytes_total, offset);
                space[file_id]--;
                if (i == 0) {
                    char newname[FLENGTH + 1];
                    snprintf(newname, FLENGTH + 1, "%020lld", hdr.header[0].id);
                    return rename_file(fullpath, path+"/"+std::string(newname));
                }
                return 0;
            }
        }
    }
    return -1;
}

void VFS::recover() {
    std::vector<std::string> files;
    touch_dir(path);
    get_files(path, &files);
    for (auto file : files)
        if (check_format(file)) {
            std::cout << file << std::endl;
            recover_file(file);
        }
}

void VFS::recover_file(const std::string &file) {
    std::string fullpath = path + "/" + file;
    ID id = stoll(file);
    std::cout << fullpath << " " << id << std::endl;
    int nrecords = 0;
    FileHeader hdr;
    lock_guard<mutex> l(mtx);
    if (read_file(fullpath, reinterpret_cast<char*>(&hdr), sizeof(hdr)) == 0) {
        std::cout << "processing " << file << std::endl;
        for (int i = 0; i < NFILES; i++) {
            if (hdr.header[i].offset == 0)
                break;
            cache.insert(hdr.header[i].id);
            nrecords++;
        }
        space[id] = nrecords;
    } else {  // to delete corrupted file or rename (.db -> .bad)
        std::cout << "can't recover " << file << " (skip)" << std::endl;
    }
}

#endif  // ENGINE_INCLUDE_VFS_H_
