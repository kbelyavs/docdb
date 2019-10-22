# docdb
A simple document-oriented DB for study purposes

- docdb.h provide generic db interface and a method to create an instance. It also defines Document format (id and data).
- disk_document_db is an implementation based on simple disk engine. It use methods from VFS (virtual file system), used to store documents.

All parameters are inside "parameters.h"  
By default, up to 10 records per file, keep records within file in sorted order, first record same as file name.  
Each db file has a header (for each record it keeps offset, size and id).