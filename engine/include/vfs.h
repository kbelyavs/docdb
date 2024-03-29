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

#include <iostream>
#include <vector>
#include <string>
#include <unordered_set>
#include <map>
#include <mutex>

#include "fs.h"
#include "constants.h"

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
    int ret = fs::read_file(path, reinterpret_cast<char*>(hdr),
                            sizeof(FileHeader));
    if (ret != 0) {
        std::cerr << "Critical error: can't read " << path << " header\n";
    }
    return ret;
}

int write_header(const std::string &path, const FileHeader *hdr) {
    int ret = fs::write_file(path, reinterpret_cast<const char*>(hdr),
                             sizeof(FileHeader));
    if (ret != 0) {
        std::cerr << "Critical error: can't write " << path << " header\n";
    }
    return ret;
}

std::string get_fullpath(ID id, const std::string &rel_path) {
    char name[FLENGTH + 1];
    snprintf(name, FLENGTH + 1, "%020lld%s", id, FILE_EXT);
    return rel_path + "/" + std::string(name);
}

using std::recursive_mutex;
using std::lock_guard;
using ID = int64_t;
enum class Opp { INSERT, UPDATE, DELETE };

class VFS {
 public:
    VFS(): path(fs::current_dir() + "/db") { recover(); }
    bool exists(ID id) const {
        return get(id, empty, false) == 0 ? true : false;
    }
    int get(ID, std::string&, bool read = true) const;
    int remove(ID id) { return do_magic(id, Opp::DELETE, empty); }
    int update(ID id, const std::string &data) {
        return do_magic(id, Opp::UPDATE, data);
    }
    int insert(ID id, const std::string &data) {
        return do_magic(id, Opp::INSERT, data);
    }
 private:
    ID find_file(ID) const;
    int do_magic(ID, Opp, const std::string&);
    void recover();
    void recover_file(const std::string&);
    std::string path;
    mutable recursive_mutex mtx;
    std::map<ID, int> space;  // keep number of entries in closest DB file
    mutable std::string empty = "";  // place holder
};

ID VFS::find_file(ID id) const {
    auto it = space.upper_bound(id);
    if (it == space.begin())
        return -1;
    --it;
    return it->first;
}

int VFS::get(ID id, std::string &data, bool read) const {
    lock_guard<recursive_mutex> l(mtx);
    ID file_id = find_file(id);
    if (file_id < 0)
        return -1;
    std::string fullpath = get_fullpath(file_id, path);
    if (space.at(file_id) == 0) {
        std::cerr << "Critical error: file " << fullpath << " is empty\n";
        return -1;
    }
    FileHeader hdr;
    if (read_header(fullpath, &hdr) == 0) {
        for (int i = 0; i < NFILES; i++) {
            if (hdr.header[i].offset == 0 || hdr.header[i].id > id) {
                break;
            } else if (hdr.header[i].id == id) {
                if (read) {
                    size_t size = hdr.header[i].size;
                    size_t offset = hdr.header[i].offset;
                    std::vector<char> cbuf(size);
                    if (fs::read_file(fullpath, cbuf.data(), size, offset) != 0)
                        return -1;
                    data = std::string(cbuf.data(), size);
                }
                return 0;
            }
        }
    }
    return -1;
}

int VFS::do_magic(ID id, Opp opp, const std::string &data) {
    lock_guard<recursive_mutex> l(mtx);
    if (exists(id) > 0) {  // to optimize, if found, store header.
        if (opp == Opp::INSERT)
            opp = Opp::UPDATE;
    } else {
        if (opp == Opp::DELETE)
            return -1;  // error, no entry found
        if (opp == Opp::UPDATE)
            opp = Opp::INSERT;
    }
    ID new_file_id = -1, file_id = find_file(id);  // < 0 - error, not found
    std::string src, dst, new_name;
    bool read_entry = false;
    bool write_etry_new = true;  // need to write new value (INSERT/UPDATE)
    bool read_write_after = true;  // move data after insert/update/delete row
    bool truncate = true;
    int pos_shift = 0;
    FileHeader *srcHdr = nullptr, *dstHdr = nullptr;
    int ret = -1;  // an error by default
    std::vector<char> cbuf;
    while (true) {  // goto workaround, in case of error move to the end
        switch (opp) {
        case Opp::INSERT:
            if (file_id >= 0) {
                src = get_fullpath(file_id, path);
                if (space[file_id] < NFILES) {  // enough space
                    dst = src;
                    truncate = false;
                    pos_shift = 1;
                } else {  // new file created, may be data move
                    dst = get_fullpath(id, path);
                }
            } else {  // new file created, no data move
                read_write_after = false;
                truncate = false;
                dst = get_fullpath(id, path);
            }
            break;
        case Opp::UPDATE:
            read_entry = true;
            src = dst = get_fullpath(file_id, path);
            break;
        case Opp::DELETE:
            src = dst = get_fullpath(file_id, path);
            write_etry_new = false;
            break;
        }
        size_t hdr_size = sizeof(FileHeader);
        if (src.length() != 0)
            srcHdr = new FileHeader;
        dstHdr = (dst == src) ? srcHdr : new FileHeader;
        if (srcHdr)
            if (read_header(src, srcHdr) != 0)
                break;
        if (dstHdr != srcHdr)  // initialize with zero
            memset(dstHdr, 0, hdr_size);
        if (opp == Opp::DELETE) {
            if (space[file_id] < 2) {
                assert(space[file_id] == 1);
                assert(srcHdr->header[0].id == id);
                assert(srcHdr->header[1].offset == 0);
                space.erase(file_id);
                ret = fs::remove_file(src);
                break;
            }
            if (srcHdr->header[0].id == id) {
                new_file_id = srcHdr->header[1].id;
                new_name = get_fullpath(new_file_id, path);
            }
        }
        int next_pos = NFILES;
        if (read_write_after) {
            for (int i = 0; i < NFILES; i++)
                if (srcHdr->header[i].offset == 0 ||
                    srcHdr->header[i].id > id) {
                    next_pos = i;
                    break;
                }
            assert(next_pos > 0);
            if (opp == Opp::UPDATE || opp == Opp::DELETE)
                assert(srcHdr->header[next_pos-1].id == id);
        }
        size_t bytes = 0;  // how much data to move, if any
        if (read_write_after) {
            for (int i = next_pos; i < NFILES; i++) {
                if (srcHdr->header[i].offset == 0)
                    break;
                bytes += srcHdr->header[i].size;
            }
        }
        int dst_pos = (src == dst) ? (next_pos - 1 + pos_shift) : 0;
        if (read_write_after && bytes == 0)
            read_write_after = false;  // nothing to write
        size_t offset;
        int shift;
        // At first read data (to move) if any
        if (read_write_after) {
            offset = srcHdr->header[next_pos].offset;
            cbuf.resize(bytes);
            assert(cbuf.size() == bytes);
            fs::read_file(src, cbuf.data(), cbuf.size(), offset);
        }
        // Then update header(s)
        if (opp == Opp::INSERT && src != dst) {
            dstHdr->header[0].offset = hdr_size;
            dstHdr->header[0].size = data.size();
            dstHdr->header[0].id = id;
        } else if (opp == Opp::UPDATE && !read_write_after) {
            shift = data.size() - dstHdr->header[dst_pos].size;
            dstHdr->header[dst_pos].size = data.size();
        }
        int n_rows = 0;
        if (read_write_after) {
            switch (opp) {
             case Opp::DELETE:
                shift = -srcHdr->header[next_pos - 1].size;
                break;
             case Opp::UPDATE:
                shift = data.size() - srcHdr->header[next_pos - 1].size;
                break;
             case Opp::INSERT:
                if (src == dst) {
                    shift = data.size();
                } else {  // new file
                    shift = hdr_size + data.size() -
                            srcHdr->header[next_pos].offset;
                }
                break;
            }
            for (int i = next_pos; i < NFILES; i++) {
                if (srcHdr->header[i].offset == 0)
                    break;
                srcHdr->header[i].offset += shift;  // update offsets
                n_rows++;
            }
            int _dst_pos = dst_pos + (write_etry_new ? 1 : 0);
            if (src != dst || next_pos != _dst_pos)
                memmove(&dstHdr->header[_dst_pos],
                        &srcHdr->header[next_pos],
                        sizeof(Entry) * n_rows);
            else if (shift)  // update size (INSERT/UPDATE)
                dstHdr->header[dst_pos].size = data.size();
            if (opp == Opp::INSERT && src == dst)
                dstHdr->header[dst_pos].id = id;
            if (opp == Opp::INSERT && src != dst) {  // invalidate src's entries
                for (int i = next_pos; i < NFILES; i++)
                    srcHdr->header[i].offset = 0;
            }
        } else if (opp == Opp::INSERT && src == dst) {
            dstHdr->header[dst_pos].offset = dstHdr->header[dst_pos-1].offset
                                           + dstHdr->header[dst_pos-1].size;
            dstHdr->header[dst_pos].size = data.size();
            dstHdr->header[dst_pos].id = id;
        }
        if (opp == Opp::DELETE)  // invalidate last entry
            dstHdr->header[dst_pos + n_rows].offset = 0;
        // Write updated header(s)
        if (srcHdr)
            write_header(src, srcHdr);
        if (dstHdr != srcHdr)
            write_header(dst, dstHdr);
        // And finally write data
        if (write_etry_new) {  // write new entry to dst, if needed
            bool _truncate = false;  // local variable
            if (opp == Opp::UPDATE && src == dst && !read_write_after &&
                shift < 0)
                _truncate = true;
            fs::write_file(dst, data.c_str(), data.size(),
                           dstHdr->header[dst_pos].offset, _truncate);
            dst_pos++;
        }
        if (read_write_after) {
            if (src != dst) {  // truncate src after data shift
                fs::write_file(src, nullptr, 0, offset, true);
                truncate = false;  // no need to truncate dst
            }
            if (shift >= 0)  // no need to truncate dst, since new size >= old
                truncate = false;
            offset = dstHdr->header[dst_pos].offset;
            fs::write_file(dst, cbuf.data(), cbuf.size(), offset, truncate);
        }
        // end of function body
        ret = 0;
        break;
    }
    // resource deallocation and error handling
    if (opp == Opp::INSERT) {
        if (src == dst)
            space[file_id]++;
        else
            space[id] = 1;
    }
    if (opp == Opp::DELETE && space.count(file_id))
        space[file_id]--;
    if (srcHdr)
        delete srcHdr;
    if (dstHdr != srcHdr)
        delete dstHdr;
    if (new_name.length() > 0) {  // to get rid of new_name
        if (fs::rename_file(dst, new_name) != 0)
            ret = -1;
        space[new_file_id] = space[file_id];
        space.erase(file_id);
    }
    if (ret)
        return ret;
    return 0;
}

void VFS::recover() {
    std::vector<std::string> files;
    fs::touch_dir(path);
    fs::get_files(path, &files);
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
    lock_guard<recursive_mutex> l(mtx);
    if (read_header(fullpath, &hdr) == 0) {
        std::cout << "processing " << file << std::endl;
        for (int i = 0; i < NFILES; i++) {
            if (hdr.header[i].offset == 0)
                break;
            nrecords++;
        }
        space[id] = nrecords;
    } else {  // to delete corrupted file or rename (.db -> .bad)
        std::cout << "can't recover " << file << " (skip)" << std::endl;
    }
}

#endif  // ENGINE_INCLUDE_VFS_H_
