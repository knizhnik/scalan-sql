#ifndef __PARQUET_H__
#define __PARQUET_H__

#include "parquet/parquet.h"

using namespace parquet;
using namespace parquet_cpp;
using namespace std;

// 
// Parquet file reader (based on https://github.com/Parquet/parquet-cpp)
//
class ParquetReader
{
public:
    struct ParquetColumnReader 
    { 
        vector<uint8_t> column_buffer;
        InMemoryInputStream* stream;
        ColumnReader* reader;

        ParquetColumnReader() : stream(NULL), reader(NULL) {}
        ~ParquetColumnReader() { 
            delete reader;
            delete stream;
        }
    };

    bool loadFile(char const* dir, size_t partNo);
    bool loadLocalFile(char const* dir, size_t partNo, bool& eof);

    FileMetaData metadata;
    vector<ParquetColumnReader> columns;
};

template<class T>
class ParquetRoundRobinRDD : public RDD<T>
{
  public:
    ParquetRoundRobinRDD(char* path) 
  : dir(path), segno(Cluster::instance->nodeId), nNodes(Cluster::instance->nNodes), step(nNodes), nextPart(true) {}

    RDD<T>* replicate() { 
        step = 1;
        segno = 0;
        return this;
    }

    bool isReplicated() { 
        return true;
    }

    virtual size_t sourceNode() { 
        return segno % nNodes;
    }

    bool next(T& record) {
        while (true) {
            if (nextPart) { 
                if (!reader.loadFile(dir, segno)) { 
                    return false;
                }
                nextPart = false;
            } 
            if (unpackParquet(record, reader)) {
                return true;
            } else { 
                segno += step;
                nextPart = true;
            }
        }
    }

    ~ParquetRoundRobinRDD() {
        delete[] dir;
    }
  private:
    ParquetReader reader;
    char* dir;
    size_t segno;
    size_t step;
    bool nextPart;
};

template<class T>
class ParquetLocalRDD : public RDD<T>
{
  public:
    ParquetLocalRDD(char* path) : dir(path), segno(0), nextPart(true) {}

    bool next(T& record) {
        while (true) {
            if (nextPart) { 
                bool eof;
                if (!reader.loadLocalFile(dir, segno++, eof)) { 
                    if (eof) { 
                        return false;
                    } else { 
                        continue;
                    }
                }
                nextPart = false;
            } 
            if (unpackParquet(record, reader)) {
                return true;
            } else { 
                nextPart = true;
            }
        }
    }

    ~ParquetLocalRDD() {
        delete[] dir;
    }
  private:
    ParquetReader reader;
    char* dir;
    size_t segno;
    bool nextPart;
};

inline bool unpackParquet(bool& dst, ColumnReader* reader, size_t)
{
    int def_level, rep_level;
    if (reader->HasNext()) {     
        dst = reader->GetBool(&def_level, &rep_level);
        assert(def_level >= rep_level);
        return true;
    }
    return false;
}

inline bool unpackParquet(int8_t& dst, ColumnReader* reader, size_t)
{
    int def_level, rep_level;
    if (reader->HasNext()) {     
        dst = (int8_t)reader->GetInt32(&def_level, &rep_level);
        assert(def_level >= rep_level);
        return true;
    }
    return false;
}

inline bool unpackParquet(char& dst, ColumnReader* reader, size_t)
{
    int def_level, rep_level;
    if (reader->HasNext()) {     
        dst = (char)reader->GetInt32(&def_level, &rep_level);
        assert(def_level >= rep_level);
        return true;
    }
    return false;
}

inline bool unpackParquet(int16_t& dst, ColumnReader* reader, size_t)
{
    int def_level, rep_level;
    if (reader->HasNext()) {     
        dst = (int16_t)reader->GetInt32(&def_level, &rep_level);
        assert(def_level >= rep_level);
        return true;
    }
    return false;
}

inline bool unpackParquet(int32_t& dst, ColumnReader* reader, size_t)
{
    int def_level, rep_level;
    if (reader->HasNext()) {     
        dst = reader->GetInt32(&def_level, &rep_level);
        assert(def_level >= rep_level);
        return true;
    }
    return false;
}

inline bool unpackParquet(uint32_t& dst, ColumnReader* reader, size_t)
{
    int def_level, rep_level;
    if (reader->HasNext()) {     
        dst = (uint32_t)reader->GetInt32(&def_level, &rep_level);
        assert(def_level >= rep_level);
        return true;
    }
    return false;
}

inline bool unpackParquet(int64_t& dst, ColumnReader* reader, size_t)
{
    if (reader->HasNext()) {     
        int def_level, rep_level;
        dst = reader->GetInt64(&def_level, &rep_level);
        assert(def_level >= rep_level);
        return true;
    }
    return false;
}

inline bool unpackParquet(float& dst, ColumnReader* reader, size_t)
{
    if (reader->HasNext()) {     
        int def_level, rep_level;
        dst = reader->GetFloat(&def_level, &rep_level);
        assert(def_level >= rep_level);
        return true;
    }
    return false;
}

inline bool unpackParquet(double& dst, ColumnReader* reader, size_t)
{
    if (reader->HasNext()) {     
        int def_level, rep_level;
        dst = reader->GetDouble(&def_level, &rep_level);
        assert(def_level >= rep_level);
        return true;
    }
    return false;
}

inline bool unpackParquet(char* dst, ColumnReader* reader, size_t size)
{
    if (reader->HasNext()) {     
        int def_level, rep_level;
        ByteArray arr = reader->GetByteArray(&def_level, &rep_level);
        assert(def_level >= rep_level);
        assert(arr.len <= size);
        memcpy(dst, arr.ptr, arr.len);
        if (arr.len < size) { 
            dst[arr.len] = '\0';
        }
        return true;
    }
    return false;
}


    
#endif
