#ifndef __PARQUET_H__
#define __PARQUET_H__

#include "parquet/parquet.h"

using namespace parquet;
using namespace parquet_cpp;
using namespace std;

// 
// Parquet file reader (based on https://github.com/Parquet/parquet-cpp)
// TODO: parquet-cpp is not supporting compression now
//
class ParquetReader
{
public:
    struct ParquetColumnReader 
    { 
        vector<uint8_t> column_buffer;
        InMemoryInputStream* stream;
        ColumnReader* reader;

        bool next(const ColumnChunk &col, char* &dst);

        ParquetColumnReader() : stream(NULL), reader(NULL) {}
        ~ParquetColumnReader() { 
            delete reader;
            delete stream;
        }
    };

    bool loadPart(char const* dir, size_t partNo);
    bool extractRow(char* buf);

    FileMetaData metadata;
    vector<ParquetColumnReader> columns;
};

template<class T>
class ParquetRDD : public RDD<T>
{
  public:
    ParquetRDD(char const* path) : dir(path), segno(Cluster::instance->nodeId), step(Cluster::instance->nNodes), nextPart(true) {}

    bool next(T& record) {
        while (true) {
            if (nextPart) { 
                if (!reader.loadPart(dir, segno + 1)) { 
                    return false;
                }
                nextPart = false;
            } 
            char buf[sizeof(T)];
            if (reader.extractRow(buf)) {
                unpack(record, buf);
                return true;
            } else { 
                segno += step;
                nextPart = true;
            }
        }
    }

  private:
    ParquetReader reader;
    char const* dir;
    size_t segno;
    size_t step;
    bool nextPart;
};

#endif
