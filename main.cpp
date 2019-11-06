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
#include "docdb.h"

void test_simple(DocumentDB& db) {
    Document doc1 = {101, "file1.txt"};
    Document doc2 = {102, "file2.json"};
    Document doc;
    assert(db.exists(doc1.id) == false);
    assert(db.get(doc1.id, &doc) < 0);
    assert(db.insert(doc1) == 0);
    assert(db.exists(doc1.id) == true);
    assert(db.get(doc1.id, &doc) == 0);
    assert(doc.id == doc1.id && doc.data == doc1.data);
    std::cout << "test_simple 1/4: insert/get Ok\n";
    assert(db.exists(doc2.id) == false);
    assert(db.insert(doc2) == 0);
    assert(db.exists(doc2.id) == true);
    assert(db.get(doc2.id, &doc) == 0);
    assert(doc.id == doc2.id && doc.data == doc2.data);
    assert(db.remove(doc2.id) == 0);
    assert(db.exists(doc2.id) == false);
    assert(db.get(doc1.id, &doc) == 0);
    assert(doc.id == doc1.id && doc.data == doc1.data);
    std::cout << "test_simple 2/4: insert/remove Ok\n";
    assert(db.insert(doc2) == 0);
    assert(db.exists(doc2.id) == true);
    assert(db.update(doc1.id, doc2.data) == 0);
    assert(db.exists(doc1.id) == true);
    assert(db.exists(doc2.id) == true);
    assert(db.get(doc1.id, &doc) == 0);
    assert(doc.id == doc1.id && doc.data == doc2.data);
    std::cout << "test_simple 3/4: update/get Ok\n";
    assert(db.remove(doc1.id) == 0);
    assert(db.exists(doc1.id) == false);
    assert(db.exists(doc2.id) == true);
    assert(db.get(doc2.id, &doc) == 0);
    assert(db.remove(doc2.id) == 0);
    assert(db.exists(doc2.id) == false);
    std::cout << "test_simple 4/4: remove Ok\n";
}

void test_perf(DocumentDB& db) {
    const int SIZE = 1000;
    Document doc;
    doc.data = "some data";
    for (int i = 0; i < SIZE; i++) {
        doc.id = i;
        assert(db.insert(doc) == 0);
    }
    std::cout << "test_perf 1/5: insert Ok\n";
    for (int i = 0; i < SIZE; i++)
        assert(db.exists(i) == true);
    std::cout << "test_perf 2/5: check Ok\n";
    for (int i = 0; i < SIZE; i++)
        assert(db.update(i, "Some other data") == 0);
    std::cout << "test_perf 3/5: update Ok\n";
    for (int i = 0; i < SIZE; i++)
        assert(db.remove(i) == 0);
    std::cout << "test_perf 4/5: remove Ok\n";
    for (int i = 0; i < SIZE; i++)
        assert(db.exists(i) == false);
    std::cout << "test_perf 5/5: check Ok\n";
}

int main(int argc, char *argv[]) {
    DocumentDB& db = get_instance();
    test_simple(db);
    test_perf(db);
    return 0;
}
