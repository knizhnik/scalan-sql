#ifndef __TILE_H__
#define __TILE_H__

#include "rdd.h"

#ifndef MAX_TILE_SIZE
#define MAX_TILE_SIZE 128
#endif

/**
 * Tile class used for vector processing
 */
template<class T>
class Tile
{
  public:
    T data[MAX_TILE_SIZE] __attribute__((aligned(16)));
    size_t size;

    Tile<T> operator+ (Tile<T> const& other) const
    {
        Tile<T> result(size);
        for (size_t i = 0, n = size; i < n; i++) { 
            result.data[i] = data[i] + other.data[i];
        }
        return result;
    }

    Tile<T> operator- (Tile<T> const& other) const
    {
        Tile<T> result(size);
        for (size_t i = 0, n = size; i < n; i++) { 
            result.data[i] = data[i] - other.data[i];
        }
        return result;
    }

    Tile<T> operator- () const
    {
        Tile<T> result(size);
        for (size_t i = 0, n = size; i < n; i++) { 
            result.data[i] = -data[i];
        }
        return result;
    }

    Tile<T> operator* (Tile<T> const& other) const
    {
        Tile<T> result(size);
        for (size_t i = 0, n = size; i < n; i++) { 
            result.data[i] = data[i] * other.data[i];
        }
        return result;
    }

    Tile<T> operator/ (Tile<T> const& other) const
    {
        Tile<T> result(size);
        for (size_t i = 0, n = size; i < n; i++) { 
            result.data[i] = data[i] / other.data[i];
        }
        return result;
    }

    Tile<bool> operator== (Tile<T> const& other) const
    {
        Tile<bool> result(size);
        for (size_t i = 0, n = size; i < n; i++) { 
            result.data[i] = data[i] == other.data[i];
        }
        return result;
    }

    Tile<bool> operator!= (Tile<T> const& other) const
    {
        Tile<bool> result(size);
        for (size_t i = 0, n = size; i < n; i++) { 
            result.data[i] = data[i] != other.data[i];
        }
        return result;
    }

    Tile<bool> operator> (Tile<T> const& other) const
    {
        Tile<bool> result(size);
        for (size_t i = 0, n = size; i < n; i++) { 
            result.data[i] = data[i] > other.data[i];
        }
        return result;
    }

    Tile<bool> operator>= (Tile<T> const& other) const
    {
        Tile<bool> result(size);
        for (size_t i = 0, n = size; i < n; i++) { 
            result.data[i] = data[i] >= other.data[i];
        }
        return result;
    }

    Tile<bool> operator< (Tile<T> const& other) const
    {
        Tile<bool> result(size);
        for (size_t i = 0, n = size; i < n; i++) { 
            result.data[i] = data[i] < other.data[i];
        }
        return result;
    }

    Tile<bool> operator<= (Tile<T> const& other) const
    {
        Tile<bool> result(size);
        for (size_t i = 0, n = size; i < n; i++) { 
            result.data[i] = data[i] <= other.data[i];
        }
        return result;
    }

    Tile(size_t used = 0) : size(used) {}
} __attribute__((aligned(16)));


template<class T>
class TileRDD
{
  public:
    /**
     * Main RDD method for iterating thoough records
     * @param record [out] placeholder for the next record
     * @return true if there is next record, false otherwise
     */
    virtual bool next(Tile<T>& tile) = 0;

    /**
     * Decompose tiles into scalar elements
     */
    RDD<T>* untile();

    /**
     * Filter input RDD
     * @return RDD with records matching predicate 
     */
    template<bool (*predicate)(T const&)>
    RDD<T>* filter();
    
    /**
     * Map records of input RDD
     * @return projection of the input RDD. 
     */
    template<class P, void (*projection)(P& out, T const& in)>
    TileRDD<P>* project();

    virtual~TileRDD<T>() {}
};

/**
 * Filter resutls using provided condition
 */
template<class T, bool (*predicate)(T const&)>
class TileFilterRDD : public RDD<T>
{
  public:
    TileFilterRDD(TileRDD<T>* input) : in(input), pos(0) {}

    bool next(T& record) {
        do { 
            if (pos >= tile.size) {
                if (!in->next(tile)) { 
                    return false;
                }
                pos = 0;
            }
            record = tile.data[pos++];
        } while (!predicate(record));
        return true;
    }

    ~TileFilterRDD() { delete in; }

  private:
    TileRDD<T>* const in;
    size_t pos;
    Tile<T> tile;
};

/**
 * Project (map) RDD records
 */
template<class T, class P, void project(P& out, T const& in)>
class TileProjectRDD : public TileRDD<P>
{
  public:
    TileProjectRDD(TileRDD<T>* input) : in(input) {}

    bool next(Tile<P>& projection) { 
        Tile<T> tile;
        if (in->next(tile)) { 
            projection.size = tile.size;
            for (size_t i = 0, n = tile.size; i < n; i++) { 
                project(projection.data[i], tile.data[i]);
            }
            return true;
        }
        return false;
    }

    ~TileProjectRDD() { delete in; }

  private:
    TileRDD<T>* const in;
};

/**
 * Decompose tile into scalar elements
 */
template<class T>
class UntileRDD : public RDD<T>
{
  public:
    UntileRDD(TileRDD<T>* input) : in(input), pos(0) {}
    
    bool next(T& record) {
        if (pos >= tile.size) {
            if (!in->next(tile)) { 
                return false;
            }
            pos = 0;
        }
        record = tile.data[pos++];
        return true;
    }

    ~UntileRDD() { delete in; }

  private:
    TileRDD<T>* const in;
    size_t pos;
    Tile<T> tile;
};
    
/**
 * Read tiles from file
 */
template<class T>
class TileFileRDD : public TileRDD<T>
{
  public:
    TileFileRDD(char* path) : f(fopen(path, "rb")), segno(Cluster::instance->nodeId), split(Cluster::instance->sharedNothing ? 1 : Cluster::instance->nNodes) {
        assert(f != NULL);
        delete[] path;
        fseek(f, 0, SEEK_END);
        nTiles = (ftell(f)/sizeof(Tile<T>)+split-1)/split;
        tileNo = fseek(f, nTiles*segno*sizeof(Tile<T>), SEEK_SET) == 0 ? 0 : nTiles;
    }

    bool next(Tile<T>& tile) {
        return ++tileNo <= nTiles && (tile.size = fread(tile.data, sizeof(T), MAX_TILE_SIZE, f)) >= 1;
    }
    
    ~TileFileRDD() { fclose(f); }

  private:
    FILE* const f;    
    size_t segno;
    size_t split;
    long tileNo;
    long nTiles;
};

/**
 * Read tiles from directory files
 */
template<class T>
class TileDirRDD : public TileRDD<T>
{
  public:
    TileDirRDD(char* path) : dir(path), segno(Cluster::instance->nodeId), step(Cluster::instance->nNodes), split(Cluster::instance->split), f(NULL) {}

    bool next(Tile<T>& tile) {
        while (true) {
            if (f == NULL) { 
                char path[MAX_PATH_LEN];
                sprintf(path, "%s/%ld.rdd", dir, segno/split);
                f = fopen(path, "rb");
                if (f == NULL) { 
                    return false;
                }
                fseek(f, 0, SEEK_END);
                nTiles = (ftell(f)/sizeof(Tile<T>)+split-1)/split;
                tileNo = 0;
                int rc = fseek(f, nTiles*(segno%split)*sizeof(Tile<T>), SEEK_SET);
                assert(rc == 0);
            }
            if (++tileNo <= nTiles && (tile.size = fread(tile.data, sizeof(T), MAX_TILE_SIZE, f)) >= 1) { 
                return true;
            } else { 
                fclose(f);
                segno += step;
                f = NULL;
            }
        }
    }

    ~TileDirRDD() {
        delete[] dir;
    }
    
  private:
    char* dir;
    size_t segno;
    size_t step;
    size_t split;
    long tileNo;
    long nTiles;
    FILE* f;    
};

/**
 * Cache tile RDD in memory
 */
template<class T>
class TileCachedRDD : public TileRDD<T>
{
  public:
    TileCachedRDD(RDD<T>* input, size_t estimation) : copy(false) { 
        cacheData(input, estimation);
    }
    bool next(Tile<T>& tile) { 
        if (curr == size) { 
            return false;
        }
        tile.size = curr + MAX_TILE_SIZE <= size ? MAX_TILE_SIZE : size - curr;
        memcpy(tile.data, buf+curr, tile.size*sizeof(T));
        curr += tile.size;
        return true;
    }
    ~TileCachedRDD() { 
        if (!copy) { 
            delete[] buf;
        }
    }

    TileCachedRDD* get() { 
        return new TileCachedRDD(buf, size);
    }

  private:
    TileCachedRDD(T* buffer, size_t bufSize) : buf(buffer), curr(0), size(bufSize), copy(true) {}

    void cacheData(RDD<T>* input, size_t estimation) { 
        buf = new T[estimation];
        size_t i = 0;
        while (input->next(buf[i])) { 
            if (++i == estimation) {
                T* newBuf = new T[estimation *= 2];
                printf("Extend cache to %ld\n", estimation);
                memcpy(newBuf, buf, i*sizeof(T));
                delete[] buf;
                buf = newBuf;
            }
        }
        size = i;
        curr = 0;
        delete input;
    }

    T* buf;
    size_t curr;
    size_t size;
    bool copy;
};

/**
 * File manager to created proper file RDD based on file name
 */
class TileFileManager
{
public:
    template<class T>
    static TileRDD<T>* load(char* fileName) { 
        size_t len = strlen(fileName);
        
        return (strcmp(fileName + len - 4, ".rdd") == 0) 
            ? (TileRDD<T>*)new TileFileRDD<T>(fileName)
            : (TileRDD<T>*)new TileDirRDD<T>(fileName);
    }
};

template<class T>
RDD<T>* TileRDD<T>::untile()
{
    return new UntileRDD<T>(this);
}

template<class T>
template<bool (*predicate)(T const&)>
RDD<T>* TileRDD<T>::filter()
{
    return new TileFilterRDD<T, predicate>(this);
}

template<class T>
template<class P, void (*projection)(P& out, T const& in)>
TileRDD<P>* TileRDD<T>::project()
{
    return new TileProjectRDD<T, P, projection>(this);
}

#endif


