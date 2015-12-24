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

    template<class T>
    void unpack(T& dst, size_t colNo);

    bool moveNext()
    { 
        assert(columnPos[lastColumn] == currPos);
        currPos += 1;
        return columns[lastColumn].reader->HasNext();
    }
    
    FileMetaData metadata;
    vector<ParquetColumnReader> columns;
    vector<size_t> columnPos;
    size_t currPos;
    size_t lastColumn;
};

template<class T>
class ParquetRoundRobinRDD : public RDD<T>
{
  public:
    ParquetRoundRobinRDD(char* path) : dir(path), segno(Cluster::instance->nodeId), step(Cluster::instance->nNodes), nextPart(true) {}

    virtual bool getNext(T& record) 
    {
        return next(record);
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

    virtual bool getNext(T& record) 
    {
        return next(record);
    }

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

template<class T>
class LazyParquetRoundRobinRDD : public RDD<T>
{
  public:
    LazyParquetRoundRobinRDD(char* path) : dir(path), segno(Cluster::instance->nodeId), step(Cluster::instance->nNodes), nextPart(true) {}

    virtual bool getNext(T& record) 
    {
        return next(record);
    }

    bool next(T& record) {
        while (true) {
            if (nextPart) { 
                if (!reader.loadFile(dir, segno)) { 
                    return false;
                }
                nextPart = false;
            } 
            if (reader.moveNext()) {
                record._reader = &reader;
                record._mask = 0;
                return true;
            } else { 
                segno += step;
                nextPart = true;
            }
        }
    }

    ~LazyParquetRoundRobinRDD() {
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
class LazyParquetLocalRDD : public RDD<T>
{
  public:
    LazyParquetLocalRDD(char* path) : dir(path), segno(0), nextPart(true) {}

    virtual bool getNext(T& record) 
    {
        return next(record);
    }

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
            if (reader.moveNext()) {
                record._reader = &reader;
                record._mask = 0;
                return true;
            } else { 
                nextPart = true;
            }
        }
    }

    ~LazyParquetLocalRDD() {
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

template<class T>
void ParquetReader::unpack(T& dst, size_t colNo)
{
    assert(columnPos[colNo] < currPos);
    lastColumn = colNo;
    do { 
        unpackParquet(dst, columns[colNo].reader, sizeof(T));
    } while (++columnPos[colNo] < currPos);
}

    
#endif
