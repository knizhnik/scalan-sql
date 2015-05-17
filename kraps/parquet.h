#include <parquet/parquet.h>
#include <rdd.h>

using namespace parquet;
using namespace parquet_cpp;


using namespace std;

class ParquetReader
{
public:
    FileMetaData metadata;
    vector<ColumnReader> columns;

    struct ColumnReader 
    { 
        vector<uint8_t> column_buffer;
        InMemoryInputStream* stream;
        ColumnReader* reader;

        bool next(const ColumnChunk &col, char* &dst);

        ColumnReader() : stream(NULL), reader(NULL) {}
        ~ColumnReader() { 
            delete reader;
            delete stream;
        }
    };

    bool loadPart(char const* dir, size_t partNo);
    bool extractRow(char* buf);
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
            if (reader.extractRaw(buf)) {
                record.unpack(buf);
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
