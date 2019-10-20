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

int write_file(const std::string &path, const char *buf, size_t size,
               size_t offset = 0, bool need_truncate = false) {
    struct stat info;
    if ((stat(path.c_str(), &info) == 0 && info.st_mode & S_IFREG) && size) {
        std::ofstream outfile;
        bool good = true;
        if (size) {
            outfile.open(path);
            if (offset)
                outfile.seekp(offset);
            outfile.write(buf, size);
            good = outfile.good();
        }
        if (good && need_truncate)
            return truncate(path.c_str(), size + offset);
        return good ? 0 : -1;
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
    size_t offset;
    size_t size;
    ID id;
};

struct FileHeader {
    Entry header[NFILES];
};

int read_header(const std::string &path, FileHeader *hdr) {
    int ret = read_file(path, reinterpret_cast<char*>(hdr), sizeof(FileHeader));
    if (ret != 0) {
        std::cout << "Critical error: can't read " << path << " header\n";
    }
    return ret;
}

int write_header(const std::string &path, const FileHeader *hdr) {
    int ret = write_file(path, reinterpret_cast<const char*>(hdr),
                         sizeof(FileHeader));
    if (ret != 0) {
        std::cout << "Critical error: can't read " << path << " header\n";
    }
    return ret;
}

using std::mutex;
using std::lock_guard;
using ID = int64_t;

class VFS {
 public:
    VFS(): path(current_dir() + "/db") { recover(); }
    bool exists(ID id) const {lock_guard<mutex> l(mtx); return cache.count(id);}
    int get(ID, std::string&) const;
    int remove(ID);
    int update(ID, const std::string&);
    int insert(ID, const std::string&);
 private:
    int _insert() { return -1; }
    int _update() { return -1; }
    int find_file(ID, std::string&, ID* file_id = nullptr) const;
    void recover();
    void recover_file(const std::string&);
    std::string path;
    mutable mutex mtx;
    std::map<ID, int> space;  // keep number of entries in closest DB file
    std::unordered_set<ID> cache;  // to check if record exists
};

std::string get_fullpath(ID id, const std::string &rel_path) {
    char name[FLENGTH + 1];
    snprintf(name, FLENGTH + 1, "%020lld", id);
    return rel_path + "/" + std::string(name);
}

int VFS::find_file(ID id, std::string &fullpath, ID *file_id) const {
    bool found = exists(id);
    auto it = space.upper_bound(id);
    if (it == space.begin()) {
        fullpath = "";
        if (file_id)
            *file_id = 0;
        if (found) {
            std::cerr << "Critical error: exists in cache, but no file found";
            return -2;
        }
        return -1;
    }
    --it;
    ID _file_id = it->first;
    if (file_id)
        *file_id = _file_id;
    fullpath = get_fullpath(_file_id, path);
    std::cout << "for record " << id << " file " << fullpath << std::endl;
    if (found && it->second == 0) {
        std::cerr << "Critical error: exists in cache, but the file is empty";
        return -3;
    }
    if (!found)
        return -1;
    return 0;
}

int VFS::get(ID id, std::string &data) const {
    lock_guard<mutex> l(mtx);
    std::string fullpath;
    if (find_file(id, fullpath) < 0)
        return -1;
    FileHeader hdr;
    if (read_header(fullpath, &hdr) == 0) {
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
    ID file_id;
    int ret = find_file(id, fullpath, &file_id);
    cache.erase(id);  // in any case, remove entry
    if (ret < 0)
        return -1;
    if (space[file_id] < 2) {  // simply remove file, since only one record
        space.erase(file_id);
        return remove_file(fullpath);
    }
    FileHeader hdr;
    if (read_header(fullpath, &hdr) == 0) {
        for (int i = 0; i < NFILES; i++) {
            if (hdr.header[i].offset == 0 || hdr.header[i].id > id) {
                break;
            } else if (hdr.header[i].id == id) {
                size_t size = hdr.header[i].size;
                size_t offset = hdr.header[i].offset;
                size_t bytes_total = 0;
                int j;
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
                hdr.header[j].offset = 0;  // invalidate last record
                write_header(fullpath, &hdr);
                std::vector<char> cbuf(bytes_total);
                read_file(fullpath, cbuf.data(), bytes_total, offset+size);
                write_file(fullpath, cbuf.data(), bytes_total, offset, true);
                space[file_id]--;
                if (i == 0) {
                    char newname[FLENGTH + 1];
                    snprintf(newname, FLENGTH + 1, "%020lld", hdr.header[0].id);
                    return rename_file(fullpath, path+"/"+std::string(newname));
                }
                return 0;
            }
        }
        std::cout << "Critical error: can't find record " << id << " in "
                  << fullpath << std::endl;
    }
    return -1;
}

int VFS::update(ID id, const std::string &data) {
    lock_guard<mutex> l(mtx);
    std::string fullpath;
    if (find_file(id, fullpath) < 0)
        return -1;
    FileHeader hdr;
    if (read_header(fullpath, &hdr) == 0) {
        for (int i = 0; i < NFILES; i++) {
            if (hdr.header[i].offset == 0 || hdr.header[i].id > id) {
                break;
            } else if (hdr.header[i].id == id) {
                size_t size = hdr.header[i].size;
                size_t offset = hdr.header[i].offset;
                if (data.size() == size)
                    return write_file(fullpath, data.c_str(), size, offset);
                hdr.header[i].size = data.size();  // update size
                size_t bytes_total = 0, delta = data.size() - size;
                int j;
                for (j = i+1; j < NFILES; j++) {
                    if (hdr.header[j].offset == 0)
                        break;
                    hdr.header[j].offset += delta;
                    bytes_total += hdr.header[j].size;
                }
                write_header(fullpath, &hdr);
                std::vector<char> cbuf(data.size() + bytes_total);
                memmove(cbuf.data(), data.c_str(), data.size());
                if (bytes_total) {  // need to shift rest of record's data
                    size_t offset = hdr.header[i+1].offset;
                    read_file(fullpath, cbuf.data()+data.size(), bytes_total,
                              offset+size);
                }
                write_file(fullpath, cbuf.data(), cbuf.size(), offset,
                           delta < 0);  // update and resize if needed
                return 0;
            }
        }
    }
    return -1;
}

int VFS::insert(ID id, const std::string &data) {
    lock_guard<mutex> l(mtx);
    std::string fullpath;
    ID file_id;
    int ret = find_file(id, fullpath, &file_id);  // somehow should get file_id!
    if (ret == 0)  // record already exists, update
        return _update();  // TODO(kbelyavs)

    cache.insert(id);  // in any case, add entry
    size_t hdr_size = sizeof(FileHeader);
    if (fullpath.length() != 0 && space[file_id] < NFILES) {
        // simple case: file found, free space exists, move code to _insert();
        space[file_id]++;
        FileHeader hdr;
        if (read_header(fullpath, &hdr) == 0) {
            int i;
            for (i = 0; i < NFILES; i++)
                if (hdr.header[i].offset == 0 || hdr.header[i].id > id)
                    break;
            bool tomove = false;
            if (hdr.header[i].offset != 0) {
                tomove = true;
                int j;
                for (j = NFILES-1; j > i; j--) {
                    if (hdr.header[j-1].offset == 0)
                        continue;
                    hdr.header[j].size = hdr.header[j-1].size;
                    hdr.header[j].offset = hdr.header[j-1].offset + data.size();
                }
            }
            hdr.header[i].size = data.size();
            hdr.header[i].offset = (i > 0) ? hdr.header[i-1].offset :
                                             sizeof(hdr);
            if (!tomove) {
                write_header(fullpath, &hdr);
                write_file(fullpath, data.c_str(), data.size(),
                           hdr.header[i].offset);
            } else {
                assert(1);  // TODO(kbelyavs)
            }
            return 0;
        }
    } else {  // have to create a new file and may be move some entries
        std::vector<char> cbuf(hdr_size + data.size());
        FileHeader *hdr = reinterpret_cast<FileHeader*>(cbuf.data());
        hdr->header[0].offset = hdr_size;
        hdr->header[0].size = data.size();
        memcpy(cbuf.data() + hdr_size, data.c_str(), data.size());
        std::string newpath = get_fullpath(id, path);
        write_file(newpath, cbuf.data(), cbuf.size());
        space[id] = 1;
        if (fullpath.length() == 0)
            return 0;
        // move all entries from fullpath with ID > id, if any
        FileHeader hdr2;
        if (read_header(fullpath, &hdr2) == 0) {
            int i;
            for (i = 0; i < NFILES; i++)
                if (hdr2.header[i].offset != 0 && hdr2.header[i].id > id)
                    break;
            if (i == NFILES)
                return 0;  // nothing to move
            size_t size = hdr2.header[i].size;
            size_t offset = hdr2.header[i].offset;
            hdr->header[1].size = size;
            hdr->header[1].offset = hdr->header[0].offset + hdr->header[0].size;
            int k = 2;
            for (int j = i+1; j < NFILES; j++) {  // merge two loops
                if (hdr2.header[j].offset == 0)
                    break;
                hdr2.header[j].offset = 0;  // invalidate record
                size += hdr2.header[j].size;
                hdr->header[k].size = hdr2.header[j].size;
                hdr->header[k].offset = hdr->header[k-1].offset +
                                        hdr->header[k-1].size;
                k++;
            }
            std::vector<char> cbuf(size);
            // read data
            read_file(fullpath, cbuf.data(), size, offset);
            // update old (fullpath) file's header and truncate
            write_header(fullpath, &hdr2);
            write_file(fullpath, nullptr, 0, offset, true);
            // write header and data
            write_header(newpath, hdr);
            write_file(newpath, cbuf.data(), cbuf.size(),
                       hdr->header[1].offset);
            return 0;
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
