#if USE_PARQUET

#include <iostream>
#include <stdio.h>

#include "parquet/parquet.h"
#include "rdd.h"
#include "hdfs.h"

using namespace parquet;
using namespace parquet_cpp;


using namespace std;

// 4 byte constant + 4 byte metadata len
const uint32_t FOOTER_SIZE = 8;
const uint8_t PARQUET_MAGIC[4] = {'P', 'A', 'R', '1'};

class ParquetFile {
  public:
    ParquetFile(char const* url) {
        if (strncmp(url, "hdfs://", 7) == 0) {
            path = (char*)strchr(url+7, '/');
            if (fs == NULL) {
                *path = '\0';
                fs = hdfsConnect(url, 0);
                assert(fs);
                *path = '/';
            }
            if (hdfsExists(fs, path) == 0) { 
                hf = hdfsOpenFile(fs, path, O_RDONLY, 0, 0, 0);
                assert(hf);
            } else {
                hf = NULL;
            }
            f = NULL;
        } else {
            hf = NULL;
            f = fopen(url, "rb");
            //assert(f);
        }        
    }

    static bool isLocal(char const* url, bool& eof) {
        char* path = (char*)strchr(url+7, '/');
        if (fs == NULL) { 
            *path = '\0';
            fs = hdfsConnect(url, 0);
            assert(fs);
            *path = '/';
        }
        char*** hosts = hdfsGetHosts(fs, path, 0, 0);//FOOTER_SIZE); 
	if (hosts == NULL) { 
	    eof = true;
	    return false;
	}
	eof = false;
	printf("%s -> %s\n", hosts[0][0], url);
	bool my = Cluster::instance.get()->isLocalNode(hosts[0][0]);
        hdfsFreeHosts(hosts);
        return my;
    }

    bool exists() { 
        return hf != NULL || f != NULL;
    }

    size_t size() { 
        if (hf) {
            hdfsFileInfo* info = hdfsGetPathInfo(fs, path);
            size_t sz = info->mSize;
            hdfsFreeFileInfo(info, 1);
            return sz;
            //return hdfsAvailable(fs, hf);
        } else { 
            int rc = fseek(f, 0, SEEK_END);
            assert(rc == 0);
            return ftell(f);
        }
    }

    void seek(size_t pos) {
        if (hf) { 
            int rc = hdfsSeek(fs, hf, pos);
            assert(rc == 0);
        } else {
            int rc = fseek(f, pos, SEEK_SET);
            assert(rc == 0);
        }
    }

    void read(void* buf, size_t size) {
        if (hf) {
            size_t offs = 0;
            while (offs < size) { 
                int n = hdfsRead(fs, hf, (char*)buf + offs, size - offs);
                assert(n > 0);
                offs += n;
            }
        } else {
            int rc = fread(buf, size, 1, f);
            assert(rc == 1);
        }
    }
    
    ~ParquetFile() {
        if (f != NULL) { 
            fclose(f);
        } else if (hf != NULL) { 
            hdfsCloseFile(fs, hf);
        }
    }

  private:
    static hdfsFS fs;
    hdfsFile hf;
    char* path;
    FILE* f;
};


hdfsFS ParquetFile::fs;

bool GetFileMetadata(ParquetFile& file, FileMetaData* metadata)
{
    size_t file_len = file.size();
    if (file_len < FOOTER_SIZE) {
        cerr << "Invalid parquet file. Corrupt footer." << endl;
        return false;
    }

    uint8_t footer_buffer[FOOTER_SIZE];
    file.seek(file_len - FOOTER_SIZE);
    file.read(footer_buffer, FOOTER_SIZE);
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

    file.seek(metadata_start);
    uint8_t metadata_buffer[metadata_len];
    file.read(metadata_buffer, metadata_len);

    DeserializeThriftMsg(metadata_buffer, &metadata_len, metadata);
    return true;

}
    
bool ParquetReader::loadFile(char const* dir, size_t partNo)
{
    char path[MAX_PATH_LEN];
    sprintf(path, "%s/part-r-%05d.parquet", dir, (int)partNo + 1);
    ParquetFile file(path);

    if (!file.exists()) { 
    	return false;
    }
    if (!GetFileMetadata(file, &metadata)) { 
        return false;
    }
    size_t nColumns = 0;
    for (size_t i = 0; i < metadata.row_groups.size(); ++i) {
        const RowGroup& row_group = metadata.row_groups[i];
        nColumns += row_group.columns.size();
    }
    columns.resize(0); // do cleanup
    columns.resize(nColumns);
    nColumns = 0;
    
    for (size_t i = 0; i < metadata.row_groups.size(); ++i) {
        const RowGroup& row_group = metadata.row_groups[i];
        for (size_t c = 0; c < row_group.columns.size(); ++c) {
            const ColumnChunk& col = row_group.columns[c];
            
            size_t col_start = col.meta_data.data_page_offset;
            if (col.meta_data.__isset.dictionary_page_offset) {
                if (col_start > col.meta_data.dictionary_page_offset) {
                    col_start = col.meta_data.dictionary_page_offset;
                }
            }
            file.seek(col_start);
            ParquetColumnReader& cr = columns[nColumns++];
            cr.column_buffer.resize(col.meta_data.total_compressed_size);
            file.read(&cr.column_buffer[0], cr.column_buffer.size());
            
            cr.stream = new InMemoryInputStream(&cr.column_buffer[0], cr.column_buffer.size());
            cr.reader = new ColumnReader(&col.meta_data, &metadata.schema[c + 1], cr.stream);
        }
    }
    return true;
}

bool ParquetReader::loadLocalFile(char const* dir, size_t partNo, bool& eof)
{
    char url[MAX_PATH_LEN];
    sprintf(url, "%s/part-r-%05d.parquet", dir, (int)partNo + 1);
    Cluster* cluster = Cluster::instance.get();
    size_t nExecutors = cluster->nExecutorsPerHost;
    size_t nHosts = cluster->nNodes / nExecutors;
    if (ParquetFile::isLocal(url, eof) && cluster->nodeId / nHosts == partNo % nExecutors) { 
        printf("Node %ld loads file %s\n", cluster->nodeId,url);
	fflush(stdout);
	
	ParquetFile file(url);
        if (!GetFileMetadata(file, &metadata)) { 
            return false;
        }
        size_t nColumns = 0;
        for (size_t i = 0; i < metadata.row_groups.size(); ++i) {
            const RowGroup& row_group = metadata.row_groups[i];
            nColumns += row_group.columns.size();
        }
        columns.resize(0); // do cleanup
        columns.resize(nColumns);
        nColumns = 0;
        
        for (size_t i = 0; i < metadata.row_groups.size(); ++i) {
            const RowGroup& row_group = metadata.row_groups[i];
            for (size_t c = 0; c < row_group.columns.size(); ++c) {
                const ColumnChunk& col = row_group.columns[c];
                
                size_t col_start = col.meta_data.data_page_offset;
                if (col.meta_data.__isset.dictionary_page_offset) {
                    if (col_start > col.meta_data.dictionary_page_offset) {
                        col_start = col.meta_data.dictionary_page_offset;
                    }
                }
                file.seek(col_start);
                ParquetColumnReader& cr = columns[nColumns++];
                cr.column_buffer.resize(col.meta_data.total_compressed_size);
                file.read(&cr.column_buffer[0], cr.column_buffer.size());
                
                cr.stream = new InMemoryInputStream(&cr.column_buffer[0], cr.column_buffer.size());
                cr.reader = new ColumnReader(&col.meta_data, &metadata.schema[c + 1], cr.stream);
            }
        }
	return true;
    }
    return false;
}

#endif
