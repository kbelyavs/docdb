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

class DiskDocumentDB : public DocumentDB {
 public:
    DiskDocumentDB();
    ~DiskDocumentDB();
    bool test(ID) const override {return true;}
    int get(ID, Document&) const override {return 0;};
    int remove(ID) override {return 0;};
    int update(ID, const std::string data) override {return 0;};
    int insert(const Document&) override {return 0;};
};

DiskDocumentDB::DiskDocumentDB() {
    std::cout << "An instance of DocDB is created\n";
}

DiskDocumentDB::~DiskDocumentDB() {
    std::cout << "An instance of DocDB is destroyed\n";
}

#endif  // ENGINE_INCLUDE_DISK_DOCUMENT_DB_H_
