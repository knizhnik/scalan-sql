#if USE_PARQUET

#include <iostream>
#include <stdio.h>

#include "parquet/parquet.h"
#include "rdd.h"

using namespace parquet;
using namespace parquet_cpp;


using namespace std;

// 4 byte constant + 4 byte metadata len
const uint32_t FOOTER_SIZE = 8;
const uint8_t PARQUET_MAGIC[4] = {'P', 'A', 'R', '1'};

struct ScopedFile {
public:
    ScopedFile(FILE* f) : file_(f) { }
    ~ScopedFile() { fclose(file_); }

private:
    FILE* file_;
};

bool GetFileMetadata(const string& path, FileMetaData* metadata) {
    FILE* file = fopen(path.c_str(), "r");
    if (!file) {
        cerr << "Could not open file: " << path << endl;
        return false;
    }
    ScopedFile cleanup(file);
    fseek(file, 0L, SEEK_END);
    size_t file_len = ftell(file);
    if (file_len < FOOTER_SIZE) {
        cerr << "Invalid parquet file. Corrupt footer." << endl;
        return false;
    }

    uint8_t footer_buffer[FOOTER_SIZE];
    fseek(file, file_len - FOOTER_SIZE, SEEK_SET);
    size_t bytes_read = fread(footer_buffer, 1, FOOTER_SIZE, file);
    if (bytes_read != FOOTER_SIZE) {
        cerr << "Invalid parquet file. Corrupt footer." << endl;
        return false;
    }
    if (memcmp(footer_buffer + 4, PARQUET_MAGIC, 4) != 0) {
        cerr << "Invalid parquet file. Corrupt footer." << endl;
        return false;
    }

    uint32_t metadata_len = *reinterpret_cast<uint32_t*>(footer_buffer);
    size_t metadata_start = file_len - FOOTER_SIZE - metadata_len;
    if (metadata_start < 0) {
        cerr << "Invalid parquet file. File is less than file metadata size." << endl;
        return false;
    }

    fseek(file, metadata_start, SEEK_SET);
    uint8_t metadata_buffer[metadata_len];
    bytes_read = fread(metadata_buffer, 1, metadata_len, file);
    if (bytes_read != metadata_len) {
        cerr << "Invalid parquet file. Could not read metadata bytes." << endl;
        return false;
    }

    DeserializeThriftMsg(metadata_buffer, &metadata_len, metadata);
    return true;

}
    
bool ParquetReader::loadPart(char const* dir, size_t partNo)
{
    char path[1024];
    sprintf(path, "%s/part-r-%05d%.parquet", dir, partNo + 1);
    FILE* file = fopen(path, "rb");
    if (file == NULL) { 
        return false;
    }
    if (!GetFileMetadata(path, &metadata)) { 
        return false;
    }
    columns.resize(0);
    for (size_t i = 0; i < metadata.row_groups.size(); ++i) {
        const RowGroup& row_group = metadata.row_groups[i];
        for (size_t c = 0; c < row_group.columns.size(); ++c) {
            const ColumnChunk& col = row_group.columns[c];
            columns.push_back(ParquetColumnReader());
            ParquetColumnReader& cr = columns.back();
            
            size_t col_start = col.meta_data.data_page_offset;
            if (col.meta_data.__isset.dictionary_page_offset) {
                if (col_start > col.meta_data.dictionary_page_offset) {
                    col_start = col.meta_data.dictionary_page_offset;
                }
            }
            fseek(file, col_start, SEEK_SET);
            cr.column_buffer.resize(col.meta_data.total_compressed_size);
            size_t num_read = fread(&cr.column_buffer[0], 1, cr.column_buffer.size(), file);
            assert(num_read == cr.column_buffer.size());
            
            cr.stream = new InMemoryInputStream(&cr.column_buffer[0], cr.column_buffer.size());
            cr.reader = new ColumnReader(&col.meta_data, &metadata.schema[c + 1], cr.stream);
        }
    }
    return true;
}
            
#endif
