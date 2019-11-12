#ifndef ENGINE_INCLUDE_FS_H_
#define ENGINE_INCLUDE_FS_H_
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
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <errno.h>

#include <vector>
#include <string>

namespace fs {

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
    int fd, ret;
    while ((fd = open(path.c_str(), O_RDWR)) == -1) {
        if (errno == EINTR)
            continue;
        perror("open");
        return -1;
    }
    while (size > 0 && (ret = pread(fd, buf, size, offset)) != 0) {
        if (ret == -1) {
            if (errno == EINTR)
                continue;
            perror("pread");
            return -1;
        }
        size -= ret;
        buf += ret;
    }
    while (close(fd) == -1) {
        if (errno == EINTR)
            continue;
        perror("close");
        return -1;
    }
    return 0;
}

int write_file(const std::string &path, const char *buf, size_t size,
               size_t offset = 0, bool need_truncate = false) {
    int fd;
    while ((fd = open(path.c_str(), O_RDWR)) == -1) {
        if (errno == EINTR)
            continue;
        break;
    }
    while (fd == -1) {  // not exist? try create
        fd = open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0640);
        if (fd == -1) {
            perror("open");
            return -1;
        }
    }
    if (size) {
        if (offset && lseek(fd, offset, SEEK_SET) == -1) {
            perror("lseek");
            return -1;
        }
        int ret, left = size;
        while (left > 0 && (ret = write(fd, buf, left)) != 0) {
            if (ret == -1) {
                if (errno == EINTR)
                    continue;
                perror("write");
                return -1;
            }
            left -= ret;
            buf += ret;
        }
    }
    while (need_truncate && ftruncate(fd, size + offset) == -1) {
        if (errno == EINTR)
            continue;
        perror("ftruncate");
        return -1;
    }
    while (fsync(fd) == -1) {
        if (errno == EINTR)
            continue;
        perror("fsync");
        return -1;
    }
    while (close(fd) == -1) {
        if (errno == EINTR)
            continue;
        perror("close");
        return -1;
    }
    return 0;
}

int remove_file(const std::string &path) {
    return remove(path.c_str());
}

int rename_file(const std::string &oldname, const std::string &newname) {
    return rename(oldname.c_str(), newname.c_str());
}

}  // namespace fs

#endif  // ENGINE_INCLUDE_FS_H_
