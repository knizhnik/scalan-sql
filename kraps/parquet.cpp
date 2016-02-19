#if USE_PARQUET

#include <iostream>
#include <stdio.h>

#include "parquet/parquet.h"
#include "parquet/thrift/util.h"
#include "rdd.h"
#include "hdfs.h"

using namespace parquet;
using namespace parquet_cpp;


using namespace std;

// 4 byte constant + 4 byte metadata len
const uint32_t FOOTER_SIZE = 8;
const uint8_t PARQUET_MAGIC[4] = {'P', 'A', 'R', '1'};

class ParquetFile : public FileLike {
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
		bool my = Cluster::instance->isLocalNode(hosts[0][0]);
        hdfsFreeHosts(hosts);
        return my;
    }

    bool exists() { 
        return hf != NULL || f != NULL;
    }

    size_t Size() { 
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

	size_t Tell() {
        if (hf) { 
			return hdfsTell(fs, hf);
        } else {
            return ftell(f);
        }
    }
		
    void Seek(size_t pos) {
        if (hf) { 
            int rc = hdfsSeek(fs, hf, pos);
            assert(rc == 0);
        } else {
            int rc = fseek(f, pos, SEEK_SET);
            assert(rc == 0);
        }
    }

    size_t Read(size_t size, uint8_t* buf) {
		size_t offs;
        if (hf) {
            offs = 0;
            while (offs < size) { 
                int n = hdfsRead(fs, hf, buf + offs, size - offs);
                assert(n > 0);
                offs += n;
            }
        } else {
            offs = fread(buf, 1, size, f);
        }
		return offs;
    }

	void Close() {
        if (f != NULL) { 
            fclose(f);
			f = NULL;
        } else if (hf != NULL) { 
            hdfsCloseFile(fs, hf);
			hf = NULL;
        }
    }

    ~ParquetFile() {
		Close();
	}

  private:
    static hdfsFS fs;
    hdfsFile hf;
    char* path;
    FILE* f;
};


hdfsFS ParquetFile::fs;
    
bool ParquetReader::loadFile(char const* dir, size_t partNo)
{
    char path[MAX_PATH_LEN];
    sprintf(path, "%s/part-r-%05d.parquet", dir, (int)partNo + 1);
    ParquetFile file(path);

    if (!file.exists()) { 
    	return false;
    }
	
	reader.Open(&file);
	reader.ParseMetaData();
	
	columns.clear();        
	for (size_t i = 0; i < reader.num_row_groups(); ++i) {
		RowGroupReader* rr = reader.RowGroup(i);
		for (size_t c = 0; c < rr->num_columns(); ++c) {
			columns.push_back(rr->Column(c));
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
		ParquetFile file(url);
        printf("Node %ld loads file %s\n", cluster->nodeId,url);
		fflush(stdout);
		
		if (!file.exists()) {
            return false;
        }
		reader.Open(&file);
		reader.ParseMetaData();

		columns.clear();        
		for (size_t i = 0; i < reader.num_row_groups(); ++i) {
			RowGroupReader* rr = reader.RowGroup(i);
			for (size_t c = 0; c < rr->num_columns(); ++c) {
				columns.push_back(rr->Column(c));
			}
		}
		return true;
    }
    return false;
}

#endif
