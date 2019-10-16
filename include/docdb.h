#ifndef INCLUDE_DOCDB_H_
#define INCLUDE_DOCDB_H_
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

using ID = int64_t;

struct Document {
    ID id;
    std::string data;
};

class DocumentDB {
 public:
    virtual ~DocumentDB() {}
    virtual bool test(ID) const = 0;
    virtual int get(ID, Document&) const = 0;
    virtual int remove(ID) = 0;
    virtual int update(ID, const std::string data) = 0;
    virtual int insert(const Document&) = 0;
};

DocumentDB& get_instance();

#endif  // INCLUDE_DOCDB_H_
