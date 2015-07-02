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

        ParquetColumnReader() : stream(NULL), reader(NULL) {}
        ~ParquetColumnReader() { 
            delete reader;
            delete stream;
        }
    };

    bool loadPart(char const* dir, size_t partNo);

    FileMetaData metadata;
    vector<ParquetColumnReader> columns;
};

template<class T>
class ParquetRDD : public RDD<T>
{
  public:
    ParquetRDD(char* path) : dir(path), segno(Cluster::instance->nodeId), step(Cluster::instance->nNodes), nextPart(true) {}

    bool next(T& record) {
        while (true) {
            if (nextPart) { 
                if (!reader.loadPart(dir, segno)) { 
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

    ~ParquetRDD() {
        delete[] dir;
    }
  private:
    ParquetReader reader;
    char* dir;
    size_t segno;
    size_t step;
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
