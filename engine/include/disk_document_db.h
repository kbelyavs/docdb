#ifndef ENGINE_INCLUDE_DISK_DOCUMENT_DB_H_
#define ENGINE_INCLUDE_DISK_DOCUMENT_DB_H_
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

#include <string>

#include "docdb.h"
#include "vfs.h"

class DiskDocumentDB : public DocumentDB {
 public:
    DiskDocumentDB(): vfs() {std::cout << "An instance of DocDB is created\n";}
    ~DiskDocumentDB() {std::cout << "An instance of DocDB is destroyed\n";}
    bool exists(ID id) const override {return vfs.exists(id);}
    int get(ID id, Document* doc) const override {return vfs.get(id, doc);};
    int remove(ID id) override {return vfs.remove(id);};
    int update(ID id, const std::string& data) override {
        return vfs.update(id, data);
    };
    int insert(const Document& doc) override {return vfs.insert(doc);};
 private:
    VFS vfs;
};

#endif  // ENGINE_INCLUDE_DISK_DOCUMENT_DB_H_
