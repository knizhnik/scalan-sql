/*******************************************************************
 *                                                                 *
 *  mcoapi.cpp                                                     *
 *                                                                 *
 *  This file is a part of the eXtremeDB source code               *
 *  Copyright (c) 2001-2005 McObject LLC                           *
 *  All Rights Reserved                                            *
 *                                                                 *
 *******************************************************************/
/*
 * ++
 *
 * PROJECT:   eXtremeDB(tm) (c) McObject LLC
 *
 * SUBSYSTEM: SQL support
 *
 * MODULE:    mcoapi.cpp
 *
 * ABSTRACT:
 *
 * VERSION:   1.0
 *
 * HISTORY:
 *            1.0- 1 KK 16-Jan-2005 Created
 * --
 */
#include <ctype.h>
#ifdef __APPLE__
#include <inttypes.h>
#endif
extern "C" {
#include "mco.h"
#include "mcowrap.h"
#include "mcocfg.h"
};
#ifdef MCO_CFG_WCHAR_SUPPORT
    #ifndef _VXWORKS
        #include <wchar.h>
    #else
        #include <stdlib.h>
    #endif
#endif
extern "C" {
#include "mcoerr.h"
#include "mcoconst.h"
#include "mcosys.h"
#include "mcomem.h"
#include "mcodb.h"
#include "mcocobj.h"
#include "mcoconn.h"
#include "mcoindex.h"
#include "mcoabst.h"
#include "mcocsr.h"
#include "mcotmgr.h"
#include "mcotrans.h"
#include "mcoobj.h"
#include "mcowimp.h"
};
#include "mcoapi.h"
#include "convert.h"
#include "mcosql.h"
#include "mcoddl.h"
#include "util.h"
#ifdef MCO_CFG_WRAPPER_SEQUENCE_SUPPORT
#include "mcoapiseq.h"
#include "mcowseq.h"
#include <math.h>
#include "sync.h"
#endif
#ifndef NO_FLOATING_POINT
    #include <float.h>
#endif

#define USE_DELETEALL 1
#define MCO_DB_EVENT_ID_NONE ((uint2)-1)


#define strlen	(int)strlen
#define wcslen  (int)wcslen

using namespace McoSql;

const Type mco2sql[] = {
    tpNull,
    tpUInt1,
    tpUInt2,
    tpUInt4,
    tpInt1,
    tpInt2,
    tpInt4,
    tpString,
    tpString,
    tpReference,
    tpReal4,
    tpReal8,
    tpUInt8,
    tpInt8,
    tpReference,
    tpNull,
    tpDateTime,
    tpDateTime,
    tpReference,
    tpUnicode,
    tpUnicode,
    tpUnicode,
    tpUnicode,
    tpBool,
    tpDateTime,
    tpNull,
    tpNull,
    tpNull,
    tpNull,
    tpNull,
    tpUInt1,
    tpUInt2,
    tpUInt4,
    tpUInt8,
    tpInt1,
    tpInt2,
    tpInt4,
    tpInt8,
    tpReal4,
    tpReal8,
    tpString,
    tpNull,
    tpNull,
    tpNull,
    tpNull,
    tpNull,
    tpNull,
    tpNull,
    tpNull,
    tpNull,
    tpStruct,
    tpBlob
};

const Type uda2sql[] = {
    tpUInt1,
    tpUInt2,
    tpUInt4,
    tpUInt8,
    tpInt1,
    tpInt2,
    tpInt4,
    tpInt8,
    tpReal4,
    tpReal8,
    tpDateTime,
    tpDateTime,
    tpString,
    tpString,
    tpString,
    tpString,
    tpString,
    tpString,
    tpReference,
    tpReference,
    tpReference,
    tpStruct,
    tpBlob,
    tpReference,
    tpBool,
    tpUInt1,
    tpUInt2,
    tpUInt4,
    tpUInt8,
    tpInt1,
    tpInt2,
    tpInt4,
    tpInt8,
    tpReal4,
    tpReal8,
    tpString
};


const mco_size_t HASH_TABLE_INIT_SIZE = 100000;

#ifdef MCO_CFG_WRAPPER_SEQUENCE_SUPPORT

inline void checkSearchStatus(MCO_RET rc)
{
    if (rc != MCO_S_NOTFOUND) {
        McoDatabase::checkStatus(rc);
    }
}

size_t McoGenericSequence::printLimit = 100;

void  McoGenericSequence::replaceConnection(mco_db_h& con, int threadId)
{
    MCO_THROW InvalidOperation("Access to ignored sequence");
}

void* McoGenericSequence::operator new(size_t size, AbstractAllocator* allocator)
{
    return allocator->allocate(offsetof(McoGenericSequence, iterator.context));
}

void* McoGenericSequence::operator new(size_t size, Type elemType, AbstractAllocator* allocator)
{
    if (elemType != tpString && !mco_seq_is_rle()) {
        size -= sizeof(mco_seq_tile_t) - MCO_SEQ_MAX_SCALAR_SIZE*MCO_SEQ_TILE_SIZE;
    }
#ifdef __GNUC__
    // Align sequence on 16-byte boundary to make it possible to use SSE vector operations
    char* ptr = (char*)allocator->allocate(size + 16);
    return ptr + (-(mco_size_t)ptr & 15);
#else
    return allocator->allocate(size);
#endif
}

McoGenericSequence* McoGenericSequence::create(bool reentrant)
{
    return new (elemType, allocator) McoGenericSequence(allocator, elemType, reentrant);
}

McoGenericSequence* McoGenericSequence::getWrapper(mco_seq_iterator_h iterator)
{
    McoGenericSequence* seq = (McoGenericSequence*)((char*)iterator - offsetof(McoGenericSequence, iterator));
    assert(seq->type() == tpSequence);
    return seq;
}


McoGenericSequence::McoGenericSequence(Type t, bool reentrant)
: elemType(t), groupBy(0), projection(0), permutation(0), n_elems(0), isSearchResult(false), isReentrant(reentrant), isScatterable(false), isIgnored(false)
{
    allocator = MemoryManager::getAllocator();
    iterator.opd[0] = NULL;
    iterator.opd[1] = NULL;
    iterator.opd[2] = NULL;
}

McoGenericSequence::McoGenericSequence(AbstractAllocator* alloc, Type t, bool reentrant)
: elemType(t), groupBy(0), projection(0), permutation(0), n_elems(0), isSearchResult(false), isReentrant(reentrant), isScatterable(false), isIgnored(false)
{
    allocator = alloc;
    iterator.opd[0] = NULL;
    iterator.opd[1] = NULL;
    iterator.opd[2] = NULL;
}


McoSql::Value* McoGenericSequence::clone(AbstractAllocator* allocator)
{
    McoGenericSequence* copy = create();
    size_t size = offsetof(McoGenericSequence, iterator.tile);
    mco_memcpy(copy, this, size);
    copy->allocator = allocator;
    return copy;
}

static bool mco_seq_parallel_execution_is_possible_for_operator(mco_seq_iterator_h iterator, mco_seq_no_t& interval)
{
    if (iterator == NULL) {
        return true;
    }
    McoGenericSequence* seq = McoGenericSequence::getWrapper(iterator);
    if (seq->isReentrant) {
        if (seq->isScatterable) {
            if (seq->projection && !seq->projection->isReentrant) {
                return false;
            }
            mco_seq_no_t count = seq->count();
            if (interval > count) {
                interval = count;
            }
        } else {
            for (int i = 0; i < 3; i++) {
                if (!mco_seq_parallel_execution_is_possible_for_operator(iterator->opd[i], interval)) {
                    return false;
                }
            }
        }
        return true;
    }
    return false;
}


static int getIntEnvVariable(char const* var, int defaultVal = 0) {
    char* val = getenv(var);
    return val == NULL ? defaultVal : atoi(val);
}

static ThreadPool mco_seq_thread_pool(getIntEnvVariable("MCO_CPU_NUMBER"));
static Mutex mco_seq_par_mutex;
static Mutex mco_seq_sql_allocator_mutex;


class McoParallelSequence : public McoGenericSequence
{
  private:
    int nResults;
    int nCompleted;
    mco_seq_iterator_h inputIterator;

    static mco_seq_iterator_h cloneTree(AbstractAllocator* allocator, mco_seq_iterator_h iterator, int threadId, mco_seq_no_t interval, mco_db_h& con)
    {
        if (iterator == NULL) {
            return NULL;
        }
        McoGenericSequence* seq = (McoGenericSequence*)McoGenericSequence::getWrapper(iterator)->clone(allocator);
        if (seq->isScatterable) {
            if (seq->projection) {
                seq->projection = (McoGenericSequence*)seq->projection->clone(allocator);
                seq->projection->replaceConnection(con, threadId);
                seq->projection->subseq(seq->iterator,
                                        seq->iterator.first_seq_no + threadId*interval,
                                        seq->iterator.first_seq_no + (threadId+1)*interval-1 < seq->iterator.last_seq_no
                                        ? seq->iterator.first_seq_no + (threadId+1)*interval-1 : seq->iterator.last_seq_no);
            } else {
                seq->replaceConnection(con, threadId);
                seq->subseq(threadId*interval, (threadId+1)*interval-1);
            }
        } else {
            seq->iterator.opd[0] = cloneTree(allocator, seq->iterator.opd[0], threadId, interval, con);
            seq->iterator.opd[1] = cloneTree(allocator, seq->iterator.opd[1], threadId, interval, con);
            seq->iterator.opd[2] = cloneTree(allocator, seq->iterator.opd[2], threadId, interval, con);
        }
        return &seq->iterator;
    }

    static void doParallel(int threadId, int nThreads, void* arg)
    {
        McoParallelSequence* self = (McoParallelSequence*)arg;
        mco_seq_no_t interval = (self->n_elems + nThreads - 1)/nThreads;
        mco_seq_iterator_t iterator;
        mco_db_h con = NULL;
        mco_memcpy(&iterator, self->inputIterator, offsetof(mco_seq_iterator_t, tile));
        MemoryManager::allocator->replace(self->allocator);
        {
            CriticalSection cs(mco_seq_sql_allocator_mutex);
            iterator.opd[0] = cloneTree(self->allocator, iterator.opd[0], threadId, interval, con);
            iterator.opd[1] = cloneTree(self->allocator, iterator.opd[1], threadId, interval, con);
        }
        MCO_RET rc = iterator.prepare(&iterator);
        if (con != NULL) {
            mco_disk_reset_connection_cache(con);
        }
        {
            CriticalSection cs(mco_seq_par_mutex);
            self->nCompleted += 1;
            if (rc != MCO_S_CURSOR_END) {
                McoDatabase::checkStatus(rc);
                if (self->nResults++ == 0) {
                    self->iterator = iterator;
                } else {
                    McoDatabase::checkStatus(self->iterator.merge(&self->iterator, &iterator));
                }
            } else if (self->nCompleted == nThreads && self->nResults == 0) { // empty result
                self->iterator = iterator;
            }
        }
    }

    void merge()
    {
        if (nCompleted == 0) {
            allocator = MemoryManager::allocator->getCurrent();
            mco_seq_thread_pool.execute(doParallel, this);
            iterator.tile_offs = iterator.tile_size;
        }
    }

  public:
    void* operator new(size_t size, AbstractAllocator* allocator) {
        return allocator->allocate(size);
    }

    McoParallelSequence(AbstractAllocator* allocator, McoGenericSequence* result, mco_seq_no_t interval)
    : McoGenericSequence(allocator), nResults(0), nCompleted(0)
    {
        n_elems = interval;
        elemType = result->elemType;
        inputIterator = &result->iterator;
        groupBy = result->groupBy;
    }

    virtual McoSql::Value* next() {
        merge();
        return McoGenericSequence::next();
    }

    virtual mco_seq_iterator_t& getIterator() {
        merge();
        return iterator;
    }

    virtual mco_seq_no_t count() {
        merge();
        return McoGenericSequence::count();
    }

    virtual void subseq(mco_seq_iterator_t& outIterator, mco_seq_no_t from, mco_seq_no_t till) {
        merge();
        McoGenericSequence::subseq(outIterator, from, till);
    }

    virtual void project(mco_seq_iterator_t& outIterator, mco_seq_iterator_t& inIterator) {
        merge();
        McoGenericSequence::project(outIterator, inIterator);
    }

    virtual void map(mco_seq_iterator_t& outIterator, mco_seq_iterator_t& mapIterator) {
        merge();
        McoGenericSequence::map(outIterator, mapIterator);
    }

    virtual void join(mco_seq_iterator_t& outIterator, mco_seq_iterator_t& joinIterator) {
        merge();
        McoGenericSequence::join(outIterator, joinIterator);
    }
};

McoGenericSequence* McoGenericSequence::getParallelSequence()
{
    mco_size_t nThreads = mco_seq_thread_pool.nThreads();
    if (nThreads > 1 && MemoryManager::allocator->isReentrant() && iterator.merge != NULL)
    {
        mco_seq_no_t interval = MCO_SEQ_INFINITY;
        if (mco_seq_parallel_execution_is_possible_for_operator(iterator.opd[0], interval)
            && mco_seq_parallel_execution_is_possible_for_operator(iterator.opd[1], interval)
            && interval != MCO_SEQ_INFINITY
            && (interval+nThreads-1)/nThreads*(nThreads-1) < interval) // all threads has something to do
        {
            return new (allocator) McoParallelSequence(allocator, this, interval);
        }
    }
    return this;
}

Type McoGenericSequence::type()
{
    return tpSequence;
}

int McoGenericSequence::compare(McoSql::Value* x)
{
    return x->isNull() ? 1 : 0;
}

size_t McoGenericSequence::toString(char* buf, size_t bufSize)
{
    if (isIgnored) {
        return fillBuf(buf, "{?}", bufSize);
    }
    size_t i = 0;
    McoSql::Value* value;
    if (i < bufSize) {
        bool first = true;
        buf[i++] = '{';
        getIterator();
        size_t mark = 0;
        while ((value = next()) != NULL && i < bufSize) {
            if (first) {
                mark = MemoryManager::allocator->mark();
                first = false;
            } else {
                i += fillBuf(buf+i, ", ", bufSize-i);
            }
            i += fillBuf(buf+i, value->stringValue()->cstr(), bufSize-i);
            MemoryManager::allocator->reset(mark);
        }
        i += fillBuf(buf+i, "}", bufSize-i);
    }
    return i;
}

void McoGenericSequence::output(FormattedOutput *streams, size_t n_streams)
{

    size_t seq_count = 0;
    size_t mark = 0;
    size_t max_last = 0; /* maximum of seq_last in all streams */
    Type value_type = tpNull;
    void *last_buf = 0;
    McoSql::Value* value;
    

    if (isIgnored) {
        for (size_t i = 0; i < n_streams; ++i) {
            streams[i].print("{?}");
        }
        return;
    }
    mco_seq_iterator_t iterator = getIterator();
    for (size_t i = 0; i < n_streams; ++i) {
        streams[i].print("{");
        if (streams[i].seq_last != FormattedOutput::UNLIMITED && streams[i].isActive() && max_last < streams[i].seq_last) {
            max_last = streams[i].seq_last;
        }
    }
    if (max_last > 0) { /* create ring buffer for last elements */
        String* s = String::create(((iterator.elem_size > 8) ? iterator.elem_size : 8) * max_last, allocator);
        last_buf = (void*) s->body();
    }

    while ((value = next()) != NULL) {
        if (seq_count == 0) {
            mark = MemoryManager::allocator->mark();
        }
        for (size_t i = 0; i < n_streams; ++i) {
            if (streams[i].seq_first == seq_count)
                streams[i].disable();
            if (seq_count > 0) streams[i].print(", ");
            if (streams[i].seq_show_indexes) streams[i].print("%d:", seq_count);
        }
        if (max_last > 0) { /* save value in the ring buffer */
            switch (value_type = value->type()) {
                case tpInt8:   ((mco_int8*)last_buf)[seq_count % max_last] = value->intValue(); break;
                case tpReal8:  ((double*)last_buf)[seq_count % max_last] = value->realValue(); break;
                case tpString: memcpy((char*)last_buf + (seq_count % max_last) * iterator.elem_size, ((String*)value)->body(), iterator.elem_size); break;
                default:       MCO_THROW RuntimeError("Illegal type");
            }
        }
        value->output(streams, n_streams);
        MemoryManager::allocator->reset(mark);
        seq_count++;
    }

    if (max_last > 0 && seq_count > 0) { /* output last elements */
        switch (value_type) {
            case tpInt8:   value = new (allocator) IntValue(0); break;
            case tpReal8:  value = new (allocator) RealValue(0); break;
            case tpString: value = String::create(iterator.elem_size, allocator); ((String*)value)->body()[iterator.elem_size] = '\0'; break;
            default:       MCO_THROW RuntimeError("Illegal type");
        }
    }
    for (size_t i = 0; i < n_streams; ++i) {
        size_t elem_to_output = 0;
        if (streams[i].seq_first < seq_count) {
            streams[i].enable();
            elem_to_output = (streams[i].seq_first + streams[i].seq_last >= seq_count) ? (seq_count - streams[i].seq_first) : streams[i].seq_last;
        }
        if (! streams[i].isActive()) continue;

        if (streams[i].seq_first + streams[i].seq_last < seq_count) {
            streams[i].print("%s...<%ld element(s)>...", streams[i].seq_first == 0 ? "" : ", ",  seq_count - streams[i].seq_first - streams[i].seq_last);
        }
        
        for (size_t l = 0; l < elem_to_output; ++l) {
            size_t buf_pos = (seq_count - elem_to_output + l) % max_last;
            switch (value_type) {
                case tpInt8:   ((IntValue*)value)->val = ((mco_int8*)last_buf)[buf_pos]; break;
                case tpReal8:  ((RealValue*)value)->val = ((double*)last_buf)[buf_pos]; break;
                case tpString: memcpy(((String*)value)->body(), (char*)last_buf + buf_pos * iterator.elem_size, iterator.elem_size); break;
                default:       MCO_THROW RuntimeError("Illegal type");
            }
            if (streams[i].seq_first != 0 || l != 0 || streams[i].seq_last < seq_count) {
                streams[i].print(", ");
            }
            if (streams[i].seq_show_indexes) streams[i].print("%d:", seq_count - elem_to_output + l);
            value->output(&streams[i], 1);
        }
        streams[i].print("}");
    }
}

String* McoGenericSequence::stringValue()
{
    String* str = String::create(printLimit+5, allocator);
    size_t n = toString(str->body(), printLimit);
    if (str->body()[n-1] != '}') {
        strcpy(&str->body()[n], "...}");
    }
    return str;
}


void McoGenericSequence::updateType()
{
    elemType = uda2sql[iterator.elem_type];
}

void McoGenericSequence::project(mco_seq_iterator_t& outIterator, mco_seq_iterator_t& inIterator)
{
    MCO_THROW InvalidOperation("Unsupported McoGenericSequence::project");
}

void McoGenericSequence::fromCursor(mco_seq_iterator_t& outIterator, Cursor* cursor)
{
    MCO_THROW InvalidOperation("Unsupported McoGenericSequence::fromCursor");
}

McoSql::Value* mco_seq_sql_ignore(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_ignore: illegal type");
    }
    seq->isIgnored = true;
    return seq;
}

static SqlFunctionDeclaration seq_ignore(tpSequence, "seq_ignore", (void*)mco_seq_sql_ignore, 1);


McoSql::Value* mco_seq_sql_internal_ignore(McoGenericSequence* seq, IntValue* mark)
{
    if (seq->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_ignore: illegal type");
    }
    if (seq->isSearchResult) {
        char buf[offsetof(McoGenericSequence, iterator.context)];
        AbstractAllocator* allocator = seq->allocator;
        mco_memcpy(buf, seq, sizeof buf);
        allocator->reset(mark->intValue());
        seq = new (allocator) McoGenericSequence(allocator);
        memcpy(seq, buf, sizeof buf);
        seq->projection = NULL;
    }
    seq->isIgnored = true;
    return seq;
}

static SqlFunctionDeclaration seq_internal_ignore(tpSequence, "seq_internal_ignore", (void*)mco_seq_sql_internal_ignore, 2);

size_t McoGenericSequence::serialize(char* buf, size_t size)
{
    if (isIgnored) {
        if (size >= 6) {
            *buf++ = (char)tpArray;
            *buf++ = (char)elemType;
            buf = packInt(buf, 0);
            return 6;
        }
        return 0;
    }
    mco_seq_no_t nElems = count();
    mco_seq_iterator_h iterator = &getIterator();
    mco_size_t nBytes = 6 + ((elemType == tpString ? 6 : 0) + iterator->elem_size)*nElems; // 6 is overhead of serialization of String value
    mco_size_t i, n = nElems;
    reset();
    if (nBytes > size) {
#if MCO_CFG_RSQL_TRUNCATE_LARGE_SEQUENCES
        if (size >= 6) {
            *buf++ = (char)tpArray;
            *buf++ = (char)elemType;
            buf = packInt(buf, 0);
            return 6;
        }
#endif
        return 0;
    }
    *buf++ = (char)tpArray;
    *buf++ = (char)elemType;
    buf = packInt(buf, nElems);
    switch (elemType) {
      case tpInt1:
        McoDatabase::checkStatus(mco_seq_get_int1(iterator, (int1*)buf, &n));
        break;
      case tpInt2:
      {
          int2* arr = (int2*)buf;
          McoDatabase::checkStatus(mco_seq_get_int2(iterator, arr, &n));
          for (i = 0; i < nElems; i++) {
              buf = packShort(buf, arr[i]);
          }
          break;
      }
      case tpInt4:
      {
          int4* arr = (int4*)buf;
          McoDatabase::checkStatus(mco_seq_get_int4(iterator, arr, &n));
          for (i = 0; i < nElems; i++) {
              buf = packInt(buf, arr[i]);
          }
          break;
      }
      case tpInt8:
      {
          mco_int8* arr = (mco_int8*)buf;
          McoDatabase::checkStatus(mco_seq_get_int8(iterator, arr, &n));
          for (i = 0; i < nElems; i++) {
              buf = packLong(buf, arr[i]);
          }
          break;
      }
      case tpUInt1:
        McoDatabase::checkStatus(mco_seq_get_uint1(iterator, (uint1*)buf, &n));
        break;
      case tpUInt2:
      {
          int2* arr = (int2*)buf;
          McoDatabase::checkStatus(mco_seq_get_uint2(iterator, (uint2*)arr, &n));
          for (i = 0; i < nElems; i++) {
              buf = packShort(buf, arr[i]);
          }
          break;
      }
      case tpUInt4:
      {
          int4* arr = (int4*)buf;
          McoDatabase::checkStatus(mco_seq_get_uint4(iterator, (uint4*)arr, &n));
          for (i = 0; i < nElems; i++) {
              buf = packInt(buf, arr[i]);
          }
          break;
      }
      case tpUInt8:
      {
          mco_int8* arr = (mco_int8*)buf;
          McoDatabase::checkStatus(mco_seq_get_uint8(iterator, (uint8*)arr, &n));
          for (i = 0; i < nElems; i++) {
              buf = packLong(buf, arr[i]);
          }
          break;
      }
      case tpReal4:
      {
          int4* arr = (int4*)buf;
          McoDatabase::checkStatus(mco_seq_get_float(iterator, (float*)arr, &n));
          for (i = 0; i < nElems; i++) {
              buf = packInt(buf, arr[i]);
          }
          break;
      }
      case tpReal8:
      {
          mco_int8* arr = (mco_int8*)buf;
          McoDatabase::checkStatus(mco_seq_get_double(iterator, (double*)arr, &n));
          for (i = 0; i < nElems; i++) {
              buf = packLong(buf, arr[i]);
          }
          break;
      }
      case tpString:
      {
          char* arr = buf + nElems*6; // 6 is overhead of serialization of String value
          int elemSize = iterator->elem_size;
          McoDatabase::checkStatus(mco_seq_get_char(iterator, arr, &n));
          for (i = 0; i < nElems; i++) {
              *buf++ = tpString;
              buf = packInt(buf, elemSize);
              memcpy(buf, arr, elemSize);
              buf[elemSize] = '\0';
              buf += elemSize + 1;
              arr += elemSize;
          }
          break;
      }
      default:
        MCO_THROW RuntimeError("Illegal type");
    }
    if (n != nElems) {
        MCO_THROW RuntimeError("Failed to extract sequence");
    }
    reset();
    return nBytes;
}

McoSql::Value* McoGenericSequence::next()
{
    MCO_RET rc = MCO_S_OK;
    switch (elemType) {
      case tpInt1:
      {
          int1 value;
          rc = mco_seq_next_int1(&iterator, &value);
          if (rc == MCO_S_OK) {
              return new (allocator) IntValue(value);
          }
          break;
      }
      case tpInt2:
      {
          int2 value;
          rc = mco_seq_next_int2(&iterator, &value);
          if (rc == MCO_S_OK) {
              return new (allocator) IntValue(value);
          }
          break;
      }
      case tpInt4:
      {
          int4 value;
          rc = mco_seq_next_int4(&iterator, &value);
          if (rc == MCO_S_OK) {
              return new (allocator) IntValue(value);
          }
          break;
      }
      case tpInt8:
      {
          mco_int8 value;
          rc = mco_seq_next_int8(&iterator, &value);
          if (rc == MCO_S_OK) {
              return new (allocator) IntValue(value);
          }
          break;
      }
      case tpUInt1:
      {
          uint1 value;
          rc = mco_seq_next_uint1(&iterator, &value);
          if (rc == MCO_S_OK) {
              return new (allocator) IntValue(value);
          }
          break;
      }
      case tpUInt2:
      {
          uint2 value;
          rc = mco_seq_next_uint2(&iterator, &value);
          if (rc == MCO_S_OK) {
              return new (allocator) IntValue(value);
          }
          break;
      }
      case tpUInt4:
      {
          uint4 value;
          rc = mco_seq_next_uint4(&iterator, &value);
          if (rc == MCO_S_OK) {
              return new (allocator) IntValue(value);
          }
          break;
      }
      case tpUInt8:
      {
          uint8 value;
          rc = mco_seq_next_uint8(&iterator, &value);
          if (rc == MCO_S_OK) {
              return new (allocator) IntValue((mco_int8)value);
          }
          break;
      }
      case tpReal4:
      {
          float value;
          rc = mco_seq_next_float(&iterator, &value);
          if (rc == MCO_S_OK) {
              return new (allocator) RealValue(value);
          }
          break;
      }
      case tpReal8:
      {
          double value;
          rc = mco_seq_next_double(&iterator, &value);
          if (rc == MCO_S_OK) {
              return new (allocator) RealValue(value);
          }
          break;
      }
      case tpString:
      {
          String* str = String::create(iterator.elem_size, allocator);
          rc = mco_seq_next_char(&iterator, str->body());
          if (rc == MCO_S_OK) {
              str->body()[iterator.elem_size] = '\0';
              return str;
          }
          break;
      }
      default:
        MCO_THROW RuntimeError("Illegal type");
    }
    if (rc != MCO_S_CURSOR_END) {
        McoDatabase::checkStatus(rc);
    }
    //iterator.bounded = MCO_NO;
    iterator.reset(&iterator); // make it possible to use this sequence iterator in another expressions
    return NULL;
}

void McoGenericSequence::reset()
{
    MCO_RET rc = iterator.reset(&iterator);
    if (rc != MCO_S_NOTFOUND) {
        McoDatabase::checkStatus(rc);
    }
}

mco_seq_no_t McoGenericSequence::count()
{
    mco_seq_no_t value;
    if (isSearchResult && iterator.last_seq_no != MCO_SEQ_INFINITY) {
        value = iterator.last_seq_no - iterator.first_seq_no + 1;
    } else if (projection) {
        value = projection->count();
        if (value != 0) {
            if (iterator.last_seq_no < value) {
                value = iterator.last_seq_no + 1;
            }
            value -= iterator.first_seq_no;
        }
    } else {
        McoGenericSequence result;
        mco_seq_iterator_h out;
        McoDatabase::checkStatus(mco_seq_agg_count(&result.iterator, &iterator));
        out = &result.getParallelSequence()->getIterator();
        McoDatabase::checkStatus(mco_seq_next_uint8(out, &value));
    }
    return value;
}

void McoGenericSequence::subseq(mco_seq_iterator_t& outIterator, mco_seq_no_t from, mco_seq_no_t till)
{
    checkSearchStatus(mco_seq_limit(&outIterator, &iterator, from, till));
}

void McoGenericSequence::join(mco_seq_iterator_t& outIterator, mco_seq_iterator_t& joinIterator)
{
    MCO_THROW InvalidOperation("Unsupported McoGenericSequence::join");
}

void McoGenericSequence::map(mco_seq_iterator_t& outIterator, mco_seq_iterator_t& mapIterator)
{
    switch (iterator.elem_type) {
      case MCO_DD_UINT1:
        McoDatabase::checkStatus(mco_seq_map_uint1(&outIterator, &iterator,  &mapIterator));
        break;
      case MCO_DD_UINT2:
        McoDatabase::checkStatus(mco_seq_map_uint2(&outIterator, &iterator,  &mapIterator));
        break;
      case MCO_DD_UINT4:
        McoDatabase::checkStatus(mco_seq_map_uint4(&outIterator, &iterator,  &mapIterator));
        break;
      case MCO_DD_UINT8:
        McoDatabase::checkStatus(mco_seq_map_uint8(&outIterator, &iterator,  &mapIterator));
        break;
      case MCO_DD_INT1:
        McoDatabase::checkStatus(mco_seq_map_int1(&outIterator, &iterator,  &mapIterator));
        break;
      case MCO_DD_INT2:
        McoDatabase::checkStatus(mco_seq_map_int2(&outIterator, &iterator,  &mapIterator));
        break;
      case MCO_DD_INT4:
        McoDatabase::checkStatus(mco_seq_map_int4(&outIterator, &iterator,  &mapIterator));
        break;
      case MCO_DD_INT8:
        McoDatabase::checkStatus(mco_seq_map_int8(&outIterator, &iterator,  &mapIterator));
        break;
      case MCO_DD_FLOAT:
        McoDatabase::checkStatus(mco_seq_map_float(&outIterator, &iterator,  &mapIterator));
        break;
      case MCO_DD_DOUBLE:
        McoDatabase::checkStatus(mco_seq_map_double(&outIterator, &iterator,  &mapIterator));
        break;
      case MCO_DD_CHAR:
        McoDatabase::checkStatus(mco_seq_map_char(&outIterator, &iterator,  &mapIterator));
        break;
      default:
        MCO_THROW RuntimeError("seq_map: illegal type");
    }
}

McoGenericSequence* McoGenericSequence::parse(Type type, char const* str)
{
    McoGenericSequence* seq = new (type) McoGenericSequence(type);
    switch (type) {
      case tpInt1:
        McoDatabase::checkStatus(mco_seq_parse_int1(&seq->iterator, str));
        break;
      case tpInt2:
        McoDatabase::checkStatus(mco_seq_parse_int2(&seq->iterator, str));
        break;
      case tpInt4:
        McoDatabase::checkStatus(mco_seq_parse_int4(&seq->iterator, str));
        break;
      case tpInt8:
        McoDatabase::checkStatus(mco_seq_parse_int8(&seq->iterator, str));
        break;
      case tpUInt1:
        McoDatabase::checkStatus(mco_seq_parse_uint1(&seq->iterator, str));
        break;
      case tpUInt2:
        McoDatabase::checkStatus(mco_seq_parse_uint2(&seq->iterator, str));
        break;
      case tpUInt4:
        McoDatabase::checkStatus(mco_seq_parse_uint4(&seq->iterator, str));
        break;
      case tpUInt8:
        McoDatabase::checkStatus(mco_seq_parse_uint8(&seq->iterator, str));
        break;
      case tpReal4:
        McoDatabase::checkStatus(mco_seq_parse_float(&seq->iterator, str));
        break;
      case tpReal8:
        McoDatabase::checkStatus(mco_seq_parse_double(&seq->iterator, str));
        break;
      default:
        MCO_THROW RuntimeError("seq_parse: illegal type");
    }
    return seq;
}

#define SEQUENCE_METHODS(TYPE)                                          \
    template<>                                                          \
    void McoSequence<mco_##TYPE>::search(mco_seq_iterator_t& outIterator, mco_##TYPE low, mco_seq_boundary_kind_t low_boundary, mco_##TYPE high, mco_seq_boundary_kind_t high_boundary) \
    {                                                                   \
        checkSearchStatus(mco_w_seq_search_##TYPE(hnd, fd->layout.u_offset, fd->layout.c_offset, &outIterator, low, low_boundary, high, high_boundary, (mco_seq_order_t)fd->seq_order)); \
    }                                                                   \
                                                                        \
    template<>                                                          \
    void McoSequence<mco_##TYPE>::append(mco_##TYPE const* items, mco_size_t nItems) \
    {                                                                   \
        McoDatabase::checkStatus(mco_w_seq_append_##TYPE(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, (mco_##TYPE*)items, nItems, (mco_seq_order_t)fd->seq_order)); \
    }                                                                   \
                                                                        \
    template<>                                                          \
    void McoSequence<mco_##TYPE>::insert(mco_seq_no_t pos, mco_##TYPE const* items, mco_size_t nItems) \
    {                                                                   \
        McoDatabase::checkStatus(mco_w_seq_insert_##TYPE(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, pos, (mco_##TYPE*)items, nItems, (mco_seq_order_t)fd->seq_order)); \
    }                                                                   \
    template<>                                                          \
    mco_##TYPE McoSequence<mco_##TYPE>::first()                                     \
    {                                                                   \
        mco_##TYPE val;                                                       \
        McoDatabase::checkStatus(mco_w_seq_first_##TYPE(hnd, fd->layout.u_offset, fd->layout.c_offset, &val)); \
        return val;                                                     \
    }                                                                   \
    template<>                                                          \
    mco_##TYPE McoSequence<mco_##TYPE>::last()                                      \
    {                                                                   \
        mco_##TYPE val;                                                       \
        McoDatabase::checkStatus(mco_w_seq_last_##TYPE(hnd, fd->layout.u_offset, fd->layout.c_offset, &val)); \
        return val;                                                     \
    }                                                                   \
                                                                        \
    template<>                                                          \
    void McoSequence<mco_##TYPE>::remove(mco_seq_no_t from, mco_seq_no_t till) \
    {                                                                   \
        McoDatabase::checkStatus(mco_w_seq_delete_##TYPE(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, from, till, (mco_seq_order_t)fd->seq_order)); \
    }                                                                   \
                                                                        \
    template<>                                                          \
    void McoSequence<mco_##TYPE>::subseq(mco_seq_iterator_t& outIterator, mco_seq_no_t from, mco_seq_no_t till) \
    {                                                                   \
        checkSearchStatus(mco_w_seq_subseq_##TYPE(hnd, fd->layout.u_offset, fd->layout.c_offset, &outIterator, from, till)); \
    }                                                                   \
                                                                        \
    template<>                                                          \
    void McoSequence<mco_##TYPE>::map(mco_seq_iterator_t& outIterator, mco_seq_iterator_t& mapIterator) \
    {                                                                   \
        McoDatabase::checkStatus(mco_w_seq_map_##TYPE(hnd, fd->layout.u_offset, fd->layout.c_offset, &outIterator, &mapIterator)); \
    }                                                                   \
                                                                        \
    template<>                                                          \
    void McoSequence<mco_##TYPE>::join(mco_seq_iterator_t& outIterator, mco_seq_iterator_t& joinIterator) \
    {                                                                   \
        McoDatabase::checkStatus(mco_w_seq_join_##TYPE(hnd, fd->layout.u_offset, fd->layout.c_offset, &outIterator, &joinIterator, (mco_seq_order_t)fd->seq_order)); \
    }                                                                   \
                                                                        \
    template<>                                                          \
    void McoSequence<mco_##TYPE>::project(mco_seq_iterator_t& outIterator, mco_seq_iterator_t& inIterator) \
    {                                                                   \
        checkSearchStatus(mco_w_seq_project_##TYPE(hnd, fd->layout.u_offset, fd->layout.c_offset, &outIterator, &inIterator)); \
    }                                                                   \
                                                                        \
    template<>                                                          \
    void McoSequence<mco_##TYPE>::fromCursor(mco_seq_iterator_t& outIterator, Cursor* cursor) \
    {                                                                   \
        McoCursor* c = (McoCursor*)cursor;                              \
        McoDatabase::checkStatus(mco_w_seq_from_cursor_##TYPE(c->trans->transaction, fd->layout.u_offset, fd->layout.c_offset, fd->_table->classCode, &outIterator, &c->cursor)); \
    }                                                                   \
                                                                        \
    template<>                                                          \
    void McoSequence<mco_##TYPE>::store(mco_seq_iterator_t& inIterator)       \
    {                                                                   \
        McoDatabase::checkStatus(mco_w_seq_store_##TYPE(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, &inIterator, (mco_seq_order_t)fd->seq_order)); \
    }                                                                   \
                                                                        \

#define SEQUENCE_COMMON_METHODS(TYPE)                                   \
    template<>                                                          \
    mco_seq_no_t McoSequence<mco_##TYPE>::count()                             \
    {                                                                   \
        mco_seq_no_t value;                                             \
        McoDatabase::checkStatus(mco_w_seq_count(hnd, fd->layout.u_offset, fd->layout.c_offset, &value)); \
        return value;                                                   \
    }                                                                   \
                                                                        \
    template<>                                                          \
    McoSequence<mco_##TYPE>::McoSequence(McoField* f, Transaction* t, mco_objhandle_h h) \
    : McoGenericSequence(mco2sql[f->mcoType])                           \
    {                                                                   \
        fd = f;                                                         \
        trans = (McoTransaction*)t;                                     \
        hnd = h;                                                        \
        isReentrant = h->pm == ((mco_db_connection_h)trans->con)->pm_inmem || trans->db->nPooledConnections != 0; \
        isScatterable = true;                                           \
    }                                                                   \
                                                                        \
    template<>                                                          \
    void McoSequence<mco_##TYPE>::replaceConnection(mco_db_h& con, int threadId) \
    {                                                                   \
        if (trans->db->nPooledConnections != 0) {                       \
            assert((size_t)threadId < trans->db->nPooledConnections);   \
            mco_objhandle_h newHnd = (mco_objhandle_h)allocator->allocate(sizeof(mco_objhandle_t)); \
            con = trans->db->connectionPool[threadId];                  \
            *newHnd = *hnd;                                             \
            newHnd->con = (mco_db_connection_h)con;                     \
            newHnd->con->trans = hnd->con->trans;                       \
            mco_tmgr_init_handle(newHnd);                               \
            hnd = newHnd;                                               \
        }                                                               \
    }                                                                   \
                                                                        \
    template<>                                                          \
    McoSql::Value* McoSequence<mco_##TYPE>::clone(AbstractAllocator* allocator) { \
        McoSequence* copy = new (elemType, allocator) McoSequence(fd, trans, hnd); \
        size_t size = offsetof(McoSequence, iterator.tile);             \
        mco_memcpy(copy, this, size);                                   \
        copy->allocator = allocator;                                    \
        return copy;                                                    \
    }





SEQUENCE_METHODS(int1)
SEQUENCE_METHODS(int2)
SEQUENCE_METHODS(int4)
SEQUENCE_METHODS(int8)
SEQUENCE_METHODS(uint1)
SEQUENCE_METHODS(uint2)
SEQUENCE_METHODS(uint4)
SEQUENCE_METHODS(uint8)
SEQUENCE_METHODS(float)
SEQUENCE_METHODS(double)

typedef char mco_char;

SEQUENCE_COMMON_METHODS(char)
SEQUENCE_COMMON_METHODS(int1)
SEQUENCE_COMMON_METHODS(int2)
SEQUENCE_COMMON_METHODS(int4)
SEQUENCE_COMMON_METHODS(int8)
SEQUENCE_COMMON_METHODS(uint1)
SEQUENCE_COMMON_METHODS(uint2)
SEQUENCE_COMMON_METHODS(uint4)
SEQUENCE_COMMON_METHODS(uint8)
SEQUENCE_COMMON_METHODS(float)
SEQUENCE_COMMON_METHODS(double)



template<>
void McoSequence<char>::append(char const* items, mco_size_t nItems)
{
    McoDatabase::checkStatus(mco_w_seq_append_char(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, (char*)items, nItems, (mco_seq_order_t)fd->seq_order, fd->seq_elem_size));
}

template<>
void McoSequence<char>::insert(mco_seq_no_t pos, char const* items, mco_size_t nItems)
{
    McoDatabase::checkStatus(mco_w_seq_insert_char(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, pos, (char*)items, nItems, (mco_seq_order_t)fd->seq_order, fd->seq_elem_size));
}


template<>
void McoSequence<char>::fromCursor(mco_seq_iterator_t& outIterator, Cursor* cursor)
{
    McoCursor* c = (McoCursor*)cursor;
    McoDatabase::checkStatus(mco_w_seq_from_cursor_char(c->trans->transaction, fd->layout.u_offset, fd->layout.c_offset, fd->_table->classCode, &outIterator, &c->cursor, fd->seq_elem_size));
}

template<>
void McoSequence<char>::store(mco_seq_iterator_t& inIterator)
{
    McoDatabase::checkStatus(mco_w_seq_store_char(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, &inIterator, (mco_seq_order_t)fd->seq_order, fd->seq_elem_size));
}

template<>
void McoSequence<char>::search(mco_seq_iterator_t& outIterator, char low, mco_seq_boundary_kind_t low_boundary, char high, mco_seq_boundary_kind_t high_boundary) \
{
    MCO_THROW InvalidOperation("Unsupported McoSequence<char>::search operation");
}

template<>
char McoSequence<char>::first()
{
    MCO_THROW InvalidOperation("Unsupported McoSequence<char>::first operation");
}

template<>
char McoSequence<char>::last()
{
    MCO_THROW InvalidOperation("Unsupported McoSequence<char>::last operation");
}

template<>
void McoSequence<char>::remove(mco_seq_no_t from, mco_seq_no_t till)
{
    McoDatabase::checkStatus(mco_w_seq_delete_char(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, from, till, (mco_seq_order_t)fd->seq_order, fd->seq_elem_size));
}

template<>
void McoSequence<char>::subseq(mco_seq_iterator_t& outIterator, mco_seq_no_t from, mco_seq_no_t till)
{
    checkSearchStatus(mco_w_seq_subseq_char(hnd, fd->layout.u_offset, fd->layout.c_offset, &outIterator, from, till, fd->seq_elem_size));
}

template<>
void McoSequence<char>::map(mco_seq_iterator_t& outIterator, mco_seq_iterator_t& mapIterator)
{
    McoDatabase::checkStatus(mco_w_seq_map_char(hnd, fd->layout.u_offset, fd->layout.c_offset, &outIterator, &mapIterator, fd->seq_elem_size));
}

template<>
void McoSequence<char>::join(mco_seq_iterator_t& outIterator, mco_seq_iterator_t& joinIterator)
{
    McoDatabase::checkStatus(mco_w_seq_join_char(hnd, fd->layout.u_offset, fd->layout.c_offset, &outIterator, &joinIterator, (mco_seq_order_t)fd->seq_order));
}

template<>
void McoSequence<char>::project(mco_seq_iterator_t& outIterator, mco_seq_iterator_t& inIterator)
{
    checkSearchStatus(mco_w_seq_project_char(hnd, fd->layout.u_offset, fd->layout.c_offset, &outIterator, &inIterator, fd->seq_elem_size));
}


static void* mco_seq_sql_malloc(mco_size_t size)
{
    CriticalSection cs(mco_seq_sql_allocator_mutex);
    return MemoryManager::allocator->allocate(size);
}

static void mco_seq_sql_free(void* ptr) {}

#define CHECK_AGGREGATE(call)                       \
    {                                               \
        MCO_RET rc_ = call;                         \
        if (rc_ == MCO_S_CURSOR_END) return &Null;  \
        McoDatabase::checkStatus(rc_);              \
    }

#define SQL_SEQ_AGGREGATE(FUNC) \
    McoSql::Value* mco_seq_sql_##FUNC(McoGenericSequence* seq)          \
    {                                                                   \
        if (seq->type() != tpSequence) {                        \
            MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");      \
        }                                                               \
        McoGenericSequence result;                                      \
        mco_seq_iterator_t* out = &result.iterator;                     \
        mco_seq_iterator_t& in = seq->getIterator();                    \
        switch (seq->elemType) {                                        \
          case tpInt1:                                          \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_int1(out, &in)); \
            break;                                                      \
          case tpInt2:                                          \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_int2(out, &in)); \
            break;                                                      \
          case tpInt4:                                          \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_int4(out, &in)); \
            break;                                                      \
          case tpInt8:                                          \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_int8(out, &in)); \
            break;                                                      \
          case tpUInt1:                                         \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_uint1(out, &in)); \
            break;                                                      \
          case tpUInt2:                                         \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_uint2(out, &in)); \
            break;                                                      \
          case tpUInt4:                                         \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_uint4(out, &in)); \
            break;                                                      \
          case tpUInt8:                                         \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_uint8(out, &in)); \
            break;                                                      \
          case tpReal4:                                         \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_float(out, &in)); \
            break;                                                      \
          case tpReal8:                                         \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_double(out, &in)); \
            break;                                                      \
          default:                                                      \
            MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");      \
        }                                                               \
        out = &result.getParallelSequence()->getIterator();              \
        switch (out->elem_type) {                                       \
        case MCO_DD_UINT1:                                              \
        {                                                               \
            uint1 val;                                                  \
            CHECK_AGGREGATE(mco_seq_next_uint1(out, &val));             \
            return new (seq->allocator) RealValue(val);                                  \
        }                                                               \
        case MCO_DD_UINT2:                                              \
        {                                                               \
            uint2 val;                                                  \
            CHECK_AGGREGATE(mco_seq_next_uint2(out, &val));    \
            return new (seq->allocator) RealValue(val);                                  \
        }                                                               \
        case MCO_DD_UINT4:                                              \
        {                                                               \
            uint4 val;                                                  \
            CHECK_AGGREGATE(mco_seq_next_uint4(out, &val));    \
            return new (seq->allocator) RealValue(val);                                  \
        }                                                               \
        case MCO_DD_UINT8:                                              \
        {                                                               \
            uint8 val;                                                  \
            CHECK_AGGREGATE(mco_seq_next_uint8(out, &val));    \
            return new (seq->allocator) RealValue(val);                                  \
        }                                                               \
        case MCO_DD_INT1:                                               \
        {                                                               \
            int1 val;                                                   \
            CHECK_AGGREGATE(mco_seq_next_int1(out, &val));     \
            return new (seq->allocator) RealValue(val);                                  \
        }                                                               \
        case MCO_DD_INT2:                                               \
        {                                                               \
            int2 val;                                                   \
            CHECK_AGGREGATE(mco_seq_next_int2(out, &val));     \
            return new (seq->allocator) RealValue(val);                                  \
        }                                                               \
        case MCO_DD_INT4:                                               \
        {                                                               \
            int4 val;                                                   \
            CHECK_AGGREGATE(mco_seq_next_int4(out, &val));     \
            return new (seq->allocator) RealValue(val);                                  \
        }                                                               \
        case MCO_DD_INT8:                                               \
        {                                                               \
            mco_int8 val;                                                   \
            CHECK_AGGREGATE(mco_seq_next_int8(out, &val));     \
            return new (seq->allocator) RealValue(val);                                  \
        }                                                               \
        case MCO_DD_FLOAT:                                              \
        {                                                               \
            float val;                                                  \
            CHECK_AGGREGATE(mco_seq_next_float(out, &val));    \
            return new (seq->allocator) RealValue(val);                                  \
        }                                                               \
        case MCO_DD_DOUBLE:                                             \
        {                                                               \
            double val;                                                 \
            CHECK_AGGREGATE(mco_seq_next_double(out, &val));   \
            return new (seq->allocator) RealValue(val);                                  \
        }                                                               \
        default:                                                        \
          MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");        \
        }                                                               \
        return NULL;                                                    \
    }                                                                   \
    static SqlFunctionDeclaration FUNC##_decl(tpReal, "seq_"#FUNC, (void*)mco_seq_sql_##FUNC, 1)

SQL_SEQ_AGGREGATE(max);
SQL_SEQ_AGGREGATE(min);
SQL_SEQ_AGGREGATE(sum);
SQL_SEQ_AGGREGATE(prd);
SQL_SEQ_AGGREGATE(avg);
SQL_SEQ_AGGREGATE(var);
SQL_SEQ_AGGREGATE(dev);
SQL_SEQ_AGGREGATE(var_samp);
SQL_SEQ_AGGREGATE(dev_samp);


#define SQL_SEQ_INT_AGGREGATE(FUNC) \
    McoSql::Value* mco_seq_sql_i##FUNC(McoGenericSequence* seq)          \
    {                                                                   \
        if (seq->type() != tpSequence) {                        \
            MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");      \
        }                                                               \
        McoGenericSequence result;                                      \
        mco_seq_iterator_t* out = &result.iterator;                     \
        mco_seq_iterator_t& in = seq->getIterator();                    \
        switch (seq->elemType) {                                        \
          case tpInt1:                                          \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_int1(out, &in)); \
            break;                                                      \
          case tpInt2:                                          \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_int2(out, &in)); \
            break;                                                      \
          case tpInt4:                                          \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_int4(out, &in)); \
            break;                                                      \
          case tpInt8:                                          \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_int8(out, &in)); \
            break;                                                      \
          case tpUInt1:                                         \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_uint1(out, &in)); \
            break;                                                      \
          case tpUInt2:                                         \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_uint2(out, &in)); \
            break;                                                      \
          case tpUInt4:                                         \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_uint4(out, &in)); \
            break;                                                      \
          case tpUInt8:                                         \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_uint8(out, &in)); \
            break;                                                      \
          case tpReal4:                                         \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_float(out, &in)); \
            break;                                                      \
          case tpReal8:                                         \
            McoDatabase::checkStatus(mco_seq_agg_##FUNC##_double(out, &in)); \
            break;                                                      \
          default:                                                      \
            MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");      \
        }                                                               \
        out = &result.getParallelSequence()->getIterator();              \
        switch (out->elem_type) {                                       \
        case MCO_DD_UINT1:                                              \
        {                                                               \
            uint1 val;                                                  \
            CHECK_AGGREGATE(mco_seq_next_uint1(out, &val));             \
            return new (seq->allocator) IntValue(val);                                  \
        }                                                               \
        case MCO_DD_UINT2:                                              \
        {                                                               \
            uint2 val;                                                  \
            CHECK_AGGREGATE(mco_seq_next_uint2(out, &val));    \
            return new (seq->allocator) IntValue(val);                                  \
        }                                                               \
        case MCO_DD_UINT4:                                              \
        {                                                               \
            uint4 val;                                                  \
            CHECK_AGGREGATE(mco_seq_next_uint4(out, &val));    \
            return new (seq->allocator) IntValue(val);                                  \
        }                                                               \
        case MCO_DD_UINT8:                                              \
        {                                                               \
            uint8 val;                                                  \
            CHECK_AGGREGATE(mco_seq_next_uint8(out, &val));    \
            return new (seq->allocator) IntValue((int64_t)val);                                  \
        }                                                               \
        case MCO_DD_INT1:                                               \
        {                                                               \
            int1 val;                                                   \
            CHECK_AGGREGATE(mco_seq_next_int1(out, &val));     \
            return new (seq->allocator) IntValue(val);                                  \
        }                                                               \
        case MCO_DD_INT2:                                               \
        {                                                               \
            int2 val;                                                   \
            CHECK_AGGREGATE(mco_seq_next_int2(out, &val));     \
            return new (seq->allocator) IntValue(val);                                  \
        }                                                               \
        case MCO_DD_INT4:                                               \
        {                                                               \
            int4 val;                                                   \
            CHECK_AGGREGATE(mco_seq_next_int4(out, &val));     \
            return new (seq->allocator) IntValue(val);                                  \
        }                                                               \
        case MCO_DD_INT8:                                               \
        {                                                               \
            mco_int8 val;                                                   \
            CHECK_AGGREGATE(mco_seq_next_int8(out, &val));     \
            return new (seq->allocator) IntValue(val);                                  \
        }                                                               \
        case MCO_DD_FLOAT:                                              \
        {                                                               \
            float val;                                                  \
            CHECK_AGGREGATE(mco_seq_next_float(out, &val));    \
            return new (seq->allocator) IntValue((int64_t)val);                                  \
        }                                                               \
        case MCO_DD_DOUBLE:                                             \
        {                                                               \
            double val;                                                 \
            CHECK_AGGREGATE(mco_seq_next_double(out, &val));   \
            return new (seq->allocator) IntValue((int64_t)val);         \
        }                                                               \
        default:                                                        \
          MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");        \
        }                                                               \
        return NULL;                                                    \
    }                                                                   \
    static SqlFunctionDeclaration FUNC##_decl_int(tpInt, "seq_i"#FUNC, (void*)mco_seq_sql_i##FUNC, 1)

SQL_SEQ_INT_AGGREGATE(max);
SQL_SEQ_INT_AGGREGATE(min);
SQL_SEQ_INT_AGGREGATE(sum);
SQL_SEQ_INT_AGGREGATE(prd);


typedef MCO_RET (*mco_seq_converter_t)(mco_seq_iterator_h result, mco_seq_iterator_h input);
#define CAST(from,to) mco_seq_##from##_to_##to

static const mco_seq_converter_t cnvMatrix[10][10] =
{
/*          tpInt1,           tpUInt1,          tpInt2,           tpUInt2,          tpInt4,           tpUInt4,          tpInt8,           tpUInt8,          tpReal4,           tpReal8 */
/*tpInt1 */{NULL,             CAST(int1,uint8), CAST(int1,int2),  CAST(int1,uint8), CAST(int1,int4),  CAST(int1,uint8), CAST(int1,int8),  CAST(int1,uint8), CAST(int1,float),  CAST(int1,double) },
/*tpUInt1*/{CAST(uint1,uint8),NULL,             CAST(uint1,uint8),CAST(uint1,uint2),CAST(uint1,uint8),CAST(uint1,uint4),CAST(uint1,uint8),CAST(uint1,uint8),CAST(uint1,float), CAST(uint1,double)},
/*tpInt2 */{NULL,             CAST(int2,uint8), NULL,             CAST(int2,uint8), CAST(int2,int4),  CAST(int2,uint8), CAST(int2,int8),  CAST(int2,uint8), CAST(int2,float),  CAST(int2,double) },
/*tpUInt2*/{CAST(uint2,uint8),NULL,             CAST(uint2,uint8),NULL,             CAST(uint2,uint8),CAST(uint2,uint4),CAST(uint2,uint8),CAST(uint2,uint8),CAST(uint2,float), CAST(uint2,double)},
/*tpInt4 */{NULL,             CAST(int4,uint8), NULL,             CAST(int4,uint8), NULL,             CAST(int4,uint8), CAST(int4,int8),  CAST(int4,uint8), CAST(int4,float),  CAST(int4,double) },
/*tpUInt4*/{CAST(uint4,uint8),NULL,             CAST(uint4,uint8),NULL,             CAST(uint4,uint8),NULL,             CAST(uint4,uint8),CAST(uint4,uint8),CAST(uint4,float), CAST(uint4,double)},
/*tpInt8 */{NULL,             CAST(int8,uint8), NULL,             CAST(int8,uint8), NULL,             CAST(int8,uint8), NULL,             CAST(int8,uint8), CAST(int8,double), CAST(int8,double) },
/*tpUInt8*/{NULL,             NULL,             NULL,             NULL,             NULL,             NULL,             NULL,             NULL,             CAST(uint8,double),CAST(uint8,double)},
/*tpReal4*/{NULL,             NULL,             NULL,             NULL,             NULL,             NULL,             NULL,             NULL,             NULL,              CAST(float,double)},
/*tpReal8*/{NULL,             NULL,             NULL,             NULL,             NULL,             NULL,             NULL,             NULL,             NULL,              NULL              }
};

static McoGenericSequence* mco_seq_sql_propagate_operand_type(McoGenericSequence* dst, McoGenericSequence* src)
{
    if (src->type() == tpString && dst->elemType != tpString) {
        src = McoGenericSequence::parse(dst->elemType, ((String*)src)->cstr());
    } else if (src->type() != tpSequence) {
        McoSql::Value* value = src;
        src = dst->create(true);
        switch (dst->elemType) {
          case tpInt1:
            McoDatabase::checkStatus(mco_seq_const_int1(&src->iterator, (int1)value->intValue()));
            break;
          case tpInt2:
            McoDatabase::checkStatus(mco_seq_const_int2(&src->iterator, (int2)value->intValue()));
            break;
          case tpInt4:
            McoDatabase::checkStatus(mco_seq_const_int4(&src->iterator, (int4)value->intValue()));
            break;
          case tpInt8:
            McoDatabase::checkStatus(mco_seq_const_int8(&src->iterator, value->intValue()));
            break;
          case tpUInt1:
            McoDatabase::checkStatus(mco_seq_const_uint1(&src->iterator, (uint1)value->intValue()));
            break;
          case tpUInt2:
            McoDatabase::checkStatus(mco_seq_const_uint2(&src->iterator, (uint2)value->intValue()));
            break;
          case tpUInt4:
            McoDatabase::checkStatus(mco_seq_const_uint4(&src->iterator, (uint4)value->intValue()));
            break;
          case tpUInt8:
            McoDatabase::checkStatus(mco_seq_const_uint8(&src->iterator, (uint8)value->intValue()));
            break;
          case tpReal4:
            McoDatabase::checkStatus(mco_seq_const_float(&src->iterator, (float)value->realValue()));
            break;
          case tpReal8:
            McoDatabase::checkStatus(mco_seq_const_double(&src->iterator, value->realValue()));
            break;
          case tpString:
          {
              mco_size_t elemSize = dst->getIterator().elem_size;
              String* str = value->stringValue();
              mco_size_t strLen = str->size();
              char* body = (char*)MemoryManager::allocator->allocate(elemSize);
              if (strLen > elemSize) {
                  strLen = elemSize;
              }
              mco_memcpy(body, str->body(), strLen);
              mco_memnull(body + strLen, elemSize - strLen);
              McoDatabase::checkStatus(mco_seq_const_char(&src->iterator, body, elemSize));
              break;
          }
          default:
            MCO_THROW RuntimeError("Illegal type");
        }
    } else if (dst->type() == tpSequence && src->elemType != dst->elemType && src->elemType != tpString && dst->elemType != tpString) {
        mco_seq_converter_t converter = cnvMatrix[(int)src->elemType - (int)tpInt1][(int)dst->elemType - (int)tpInt1];
        if (converter != NULL) {
            McoGenericSequence* cnv = dst->create();
            cnv->isReentrant = true;
            McoDatabase::checkStatus(converter(&cnv->iterator, &src->getIterator()));
            cnv->updateType();
            return cnv;
        }
    }
    return src;
}

#define SQL_SEQ_BIN_OP(FUNC, NAME, CHAR_OP)                             \
    static McoSql::Value* mco_seq_sql_##NAME(McoGenericSequence* left, McoGenericSequence* right)  \
    {                                                                   \
        if (left->type() != tpSequence && right->type() != tpSequence) { \
            MCO_THROW RuntimeError("seq_" #NAME ": illegal type");      \
        }                                                               \
        right = mco_seq_sql_propagate_operand_type(left, right);        \
        left = mco_seq_sql_propagate_operand_type(right, left);         \
        McoGenericSequence* result = left->create();                    \
        result->isReentrant = true;                                     \
        mco_seq_iterator_t& l = left->getIterator();                    \
        mco_seq_iterator_t& r = right->getIterator();                   \
        switch (left->elemType) {                                       \
          case tpInt1:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int1(&result->iterator, &l, &r)); \
              break;                                                    \
          case tpInt2:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int2(&result->iterator, &l, &r)); \
              break;                                                    \
          case tpInt4:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int4(&result->iterator, &l, &r)); \
              break;                                                    \
          case tpInt8:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int8(&result->iterator, &l, &r)); \
              break;                                                    \
          case tpUInt1:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint1(&result->iterator, &l, &r)); \
              break;                                                    \
          case tpUInt2:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint2(&result->iterator, &l, &r)); \
              break;                                                    \
          case tpUInt4:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint4(&result->iterator, &l, &r)); \
              break;                                                    \
          case tpUInt8:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint8(&result->iterator, &l, &r)); \
              break;                                                    \
          case tpReal4:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_float(&result->iterator, &l, &r)); \
              break;                                                    \
          case tpReal8:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_double(&result->iterator, &l, &r)); \
              break;                                                    \
          case tpString:                                        \
              CHAR_OP(McoDatabase::checkStatus(mco_seq_##FUNC##_char(&result->iterator, &l, &r))); \
          default:                                                      \
              MCO_THROW RuntimeError("seq_" #NAME ": illegal type");    \
        }                                                               \
        result->updateType();                                           \
        return result;                                                  \
    }                                                                   \
    static SqlFunctionDeclaration NAME##_decl(tpSequence, "seq_"#NAME, (void*)mco_seq_sql_##NAME, 2)

#define SQL_SEQ_CHAR_OP(op)   op; break
#define SQL_SEQ_CHAR_NOP(op)  /*no break*/

SQL_SEQ_BIN_OP(add, add, SQL_SEQ_CHAR_OP);
SQL_SEQ_BIN_OP(sub, sub, SQL_SEQ_CHAR_NOP);
SQL_SEQ_BIN_OP(mul, mul, SQL_SEQ_CHAR_NOP);
SQL_SEQ_BIN_OP(div, div, SQL_SEQ_CHAR_NOP);
SQL_SEQ_BIN_OP(mod, mod, SQL_SEQ_CHAR_NOP);
SQL_SEQ_BIN_OP(max, maxof, SQL_SEQ_CHAR_NOP);
SQL_SEQ_BIN_OP(min, minof, SQL_SEQ_CHAR_NOP);
SQL_SEQ_BIN_OP(lt, lt, SQL_SEQ_CHAR_OP);
SQL_SEQ_BIN_OP(gt, gt, SQL_SEQ_CHAR_OP);
SQL_SEQ_BIN_OP(eq, eq, SQL_SEQ_CHAR_OP);
SQL_SEQ_BIN_OP(le, le, SQL_SEQ_CHAR_OP);
SQL_SEQ_BIN_OP(ge, ge, SQL_SEQ_CHAR_OP);
SQL_SEQ_BIN_OP(ne, ne, SQL_SEQ_CHAR_OP);

static McoSql::Value* mco_seq_sql_filter(McoGenericSequence* left, McoGenericSequence* right)
{
    if (left->type() != tpSequence || right->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_filter: illegal type");
    }
    mco_seq_iterator_t& l = left->getIterator();
    mco_seq_iterator_t& r = right->getIterator();
    McoGenericSequence* result = right->create(true);
    switch (right->elemType) {
      case tpInt1:
        McoDatabase::checkStatus(mco_seq_filter_int1(&result->iterator, &l, &r));
        break;
      case tpInt2:
        McoDatabase::checkStatus(mco_seq_filter_int2(&result->iterator, &l, &r));
        break;
      case tpInt4:
        McoDatabase::checkStatus(mco_seq_filter_int4(&result->iterator, &l, &r));
        break;
      case tpInt8:
        McoDatabase::checkStatus(mco_seq_filter_int8(&result->iterator, &l, &r));
        break;
      case tpUInt1:
        McoDatabase::checkStatus(mco_seq_filter_uint1(&result->iterator, &l, &r));
        break;
      case tpUInt2:
        McoDatabase::checkStatus(mco_seq_filter_uint2(&result->iterator, &l, &r));
        break;
      case tpUInt4:
        McoDatabase::checkStatus(mco_seq_filter_uint4(&result->iterator, &l, &r));
        break;
      case tpUInt8:
        McoDatabase::checkStatus(mco_seq_filter_uint8(&result->iterator, &l, &r));
        break;
      case tpReal4:
        McoDatabase::checkStatus(mco_seq_filter_float(&result->iterator, &l, &r));
        break;
      case tpReal8:
        McoDatabase::checkStatus(mco_seq_filter_double(&result->iterator, &l, &r));
        break;
      case tpString:
        McoDatabase::checkStatus(mco_seq_filter_char(&result->iterator, &l, &r));
        break;
      default:
        MCO_THROW RuntimeError("seq_filter: illegal type");
    }
    return result;
}
static SqlFunctionDeclaration filter_decl(tpSequence, "seq_filter", (void*)mco_seq_sql_filter, 2);


#define SQL_SEQ_GROUP_BY_OP(FUNC) \
    static McoSql::Value* mco_seq_sql_##FUNC(McoGenericSequence* left, McoGenericSequence* right) \
    {                                                                   \
        if (left->type() != tpSequence || right->type() != tpSequence) { \
            MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");      \
        }                                                               \
        mco_seq_iterator_t& l = left->getIterator();                    \
        mco_seq_iterator_t& r = right->getIterator();                   \
        McoGenericSequence* result = left->create();                    \
        switch (left->elemType) {                                       \
          case tpInt1:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int1(&result->iterator, &l, &r)); \
              break;                                                    \
          case tpInt2:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int2(&result->iterator, &l, &r)); \
              break;                                                    \
          case tpInt4:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int4(&result->iterator, &l, &r)); \
              break;                                                    \
          case tpInt8:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int8(&result->iterator, &l, &r)); \
              break;                                                    \
          case tpUInt1:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint1(&result->iterator, &l, &r)); \
              break;                                                    \
          case tpUInt2:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint2(&result->iterator, &l, &r)); \
              break;                                                    \
          case tpUInt4:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint4(&result->iterator, &l, &r)); \
              break;                                                    \
          case tpUInt8:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint8(&result->iterator, &l, &r)); \
              break;                                                    \
          case tpReal4:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_float(&result->iterator, &l, &r)); \
              break;                                                    \
          case tpReal8:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_double(&result->iterator, &l, &r)); \
              break;                                                    \
          default:                                                      \
            MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");      \
        }                                                               \
        result->updateType();                                           \
        return result;                                                  \
    }                                                                   \
    static SqlFunctionDeclaration FUNC##_decl(tpSequence, "seq_"#FUNC, (void*)mco_seq_sql_##FUNC, 2)

SQL_SEQ_GROUP_BY_OP(group_agg_min);
SQL_SEQ_GROUP_BY_OP(group_agg_max);
SQL_SEQ_GROUP_BY_OP(group_agg_sum);
SQL_SEQ_GROUP_BY_OP(group_agg_avg);
SQL_SEQ_GROUP_BY_OP(group_agg_var);
SQL_SEQ_GROUP_BY_OP(group_agg_dev);
SQL_SEQ_GROUP_BY_OP(group_agg_var_samp);
SQL_SEQ_GROUP_BY_OP(group_agg_dev_samp);
SQL_SEQ_GROUP_BY_OP(group_agg_first);
SQL_SEQ_GROUP_BY_OP(group_agg_last);


#define SQL_SEQ_AGG2_GROUP_BY_OP(FUNC) \
    static McoSql::Value* mco_seq_sql_##FUNC(McoGenericSequence* left, McoGenericSequence* right, McoGenericSequence* gby) \
    {                                                                   \
        if (left->type() != tpSequence || right->type() != tpSequence || gby->type() != tpSequence) { \
            MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");      \
        }                                                               \
        right = mco_seq_sql_propagate_operand_type(left, right);        \
        left = mco_seq_sql_propagate_operand_type(right, left);         \
        mco_seq_iterator_t& l = left->getIterator();                    \
        mco_seq_iterator_t& r = right->getIterator();                   \
        mco_seq_iterator_t& g = gby->getIterator();                   \
        McoGenericSequence* result = new (tpReal8, gby->allocator) McoGenericSequence(gby->allocator, tpUInt8); \
        switch (left->elemType) {                                       \
          case tpInt1:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int1(&result->iterator, &l, &r, &g)); \
              break;                                                    \
          case tpInt2:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int2(&result->iterator, &l, &r, &g)); \
              break;                                                    \
          case tpInt4:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int4(&result->iterator, &l, &r, &g)); \
              break;                                                    \
          case tpInt8:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int8(&result->iterator, &l, &r, &g)); \
              break;                                                    \
          case tpUInt1:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint1(&result->iterator, &l, &r, &g)); \
              break;                                                    \
          case tpUInt2:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint2(&result->iterator, &l, &r, &g)); \
              break;                                                    \
          case tpUInt4:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint4(&result->iterator, &l, &r, &g)); \
              break;                                                    \
          case tpUInt8:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint8(&result->iterator, &l, &r, &g)); \
              break;                                                    \
          case tpReal4:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_float(&result->iterator, &l, &r, &g)); \
              break;                                                    \
          case tpReal8:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_double(&result->iterator, &l, &r, &g)); \
              break;                                                    \
          default:                                                      \
            MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");      \
        }                                                               \
        result->updateType();                                           \
        return result;                                                  \
    }                                                                   \
    static SqlFunctionDeclaration FUNC##_decl(tpSequence, "seq_"#FUNC, (void*)mco_seq_sql_##FUNC, 3)

SQL_SEQ_AGG2_GROUP_BY_OP(group_agg_wavg);


static McoSql::Value* mco_seq_sql_concat(McoGenericSequence* left, McoGenericSequence* right)
{
    if (left->type() != tpSequence && right->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_concat: illegal type");
    }
    right = mco_seq_sql_propagate_operand_type(left, right);
    left = mco_seq_sql_propagate_operand_type(right, left);
    McoGenericSequence* result = left->create();
    mco_seq_iterator_t& l = left->getIterator();
    mco_seq_iterator_t& r = right->getIterator();
    McoDatabase::checkStatus(mco_seq_concat(&result->iterator, &l, &r));
    return result;
}
static SqlFunctionDeclaration concat_decl(tpSequence, "seq_concat", (void*)mco_seq_sql_concat, 2);

static McoSql::Value* mco_seq_sql_cat(McoGenericSequence* left, McoGenericSequence* right)
{
    if (left->type() != tpSequence || right->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_cat: illegal type");
    }
    McoGenericSequence* result = new (tpString, left->allocator) McoGenericSequence(left->allocator, tpString, true);
    mco_seq_iterator_t& l = left->getIterator();
    mco_seq_iterator_t& r = right->getIterator();
    McoDatabase::checkStatus(mco_seq_cat(&result->iterator, &l, &r));
    return result;
}
static SqlFunctionDeclaration cat_decl(tpSequence, "seq_cat", (void*)mco_seq_sql_cat, 2);



#define SQL_SEQ_BIN_SCALAR_OP(FUNC)                                     \
    McoSql::Value* mco_seq_sql_##FUNC(McoGenericSequence* left, McoGenericSequence* right)  \
    {                                                                   \
        if (left->type() != tpSequence || right->type() != tpSequence) { \
            MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");      \
        }                                                               \
        double value;                                                   \
        right = mco_seq_sql_propagate_operand_type(left, right);        \
        left = mco_seq_sql_propagate_operand_type(right, left);         \
        mco_seq_iterator_t& l = left->getIterator();                    \
        mco_seq_iterator_t& r = right->getIterator();                   \
        McoGenericSequence result;                                      \
        mco_seq_iterator_t* out = &result.iterator;                     \
        switch (left->elemType) {                                       \
          case tpInt1:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int1(out, &l, &r)); \
              break;                                                    \
          case tpInt2:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int2(out, &l, &r)); \
              break;                                                    \
          case tpInt4:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int4(out, &l, &r)); \
              break;                                                    \
          case tpInt8:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int8(out, &l, &r)); \
              break;                                                    \
          case tpUInt1:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint1(out, &l, &r)); \
              break;                                                    \
          case tpUInt2:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint2(out, &l, &r)); \
              break;                                                    \
          case tpUInt4:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint4(out, &l, &r)); \
              break;                                                    \
          case tpUInt8:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint8(out, &l, &r)); \
              break;                                                    \
          case tpReal4:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_float(out, &l, &r)); \
              break;                                                    \
          case tpReal8:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_double(out, &l, &r)); \
              break;                                                    \
          default:                                                      \
            MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");      \
        }                                                               \
        out = &result.getParallelSequence()->getIterator();              \
        CHECK_AGGREGATE(mco_seq_next_double(out, &value));              \
        return new (left->allocator) RealValue(value);                  \
    }                                                                   \
    static SqlFunctionDeclaration FUNC##_decl(tpReal, "seq_"#FUNC, (void*)mco_seq_sql_##FUNC, 2)

SQL_SEQ_BIN_SCALAR_OP(wavg);
SQL_SEQ_BIN_SCALAR_OP(wsum);
SQL_SEQ_BIN_SCALAR_OP(cov);
SQL_SEQ_BIN_SCALAR_OP(corr);


#define SQL_SEQ_UNARY_OP_NAME(FUNC, NAME, CHAR_OP)                      \
    static McoSql::Value* mco_seq_sql_##FUNC(McoGenericSequence* seq)   \
    {                                                                   \
        if (seq->type() != tpSequence) {                        \
            MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");      \
        }                                                               \
        McoGenericSequence* result = seq->create();                     \
        result->isReentrant = true;                                     \
        mco_seq_iterator_t& in = seq->getIterator();                    \
        switch (seq->elemType) {                                        \
          case tpInt1:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int1(&result->iterator, &in)); \
              break;                                                    \
          case tpInt2:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int2(&result->iterator, &in)); \
              break;                                                    \
          case tpInt4:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int4(&result->iterator, &in)); \
              break;                                                    \
          case tpInt8:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int8(&result->iterator, &in)); \
              break;                                                    \
          case tpUInt1:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint1(&result->iterator, &in)); \
              break;                                                    \
          case tpUInt2:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint2(&result->iterator, &in)); \
              break;                                                    \
          case tpUInt4:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint4(&result->iterator, &in)); \
              break;                                                    \
          case tpUInt8:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint8(&result->iterator, &in)); \
              break;                                                    \
          case tpReal4:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_float(&result->iterator, &in)); \
              break;                                                    \
          case tpReal8:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_double(&result->iterator, &in)); \
              break;                                                    \
          case tpString:                                        \
              CHAR_OP(McoDatabase::checkStatus(mco_seq_##FUNC##_char(&result->iterator, &in))); \
          default:                                                      \
              MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");    \
        }                                                               \
        result->updateType();                                           \
        return result;                                                  \
    }                                                                   \
    static SqlFunctionDeclaration FUNC##_decl(tpSequence, "seq_"#NAME, (void*)mco_seq_sql_##FUNC, 1)


#define SQL_SEQ_UNARY_OP(OP, CHAR_OP) SQL_SEQ_UNARY_OP_NAME(OP, OP, CHAR_OP)

SQL_SEQ_UNARY_OP(abs, SQL_SEQ_CHAR_NOP);
SQL_SEQ_UNARY_OP(neg, SQL_SEQ_CHAR_NOP);
SQL_SEQ_UNARY_OP(reverse, SQL_SEQ_CHAR_OP);
SQL_SEQ_UNARY_OP(diff, SQL_SEQ_CHAR_NOP);
SQL_SEQ_UNARY_OP(trend, SQL_SEQ_CHAR_NOP);
SQL_SEQ_UNARY_OP(unique, SQL_SEQ_CHAR_OP);
SQL_SEQ_UNARY_OP(norm, SQL_SEQ_CHAR_NOP);
SQL_SEQ_UNARY_OP(cum_agg_avg, SQL_SEQ_CHAR_NOP);
SQL_SEQ_UNARY_OP(cum_agg_sum, SQL_SEQ_CHAR_NOP);
SQL_SEQ_UNARY_OP(cum_agg_min, SQL_SEQ_CHAR_NOP);
SQL_SEQ_UNARY_OP(cum_agg_max, SQL_SEQ_CHAR_NOP);
SQL_SEQ_UNARY_OP(cum_agg_prd, SQL_SEQ_CHAR_NOP);
SQL_SEQ_UNARY_OP(cum_agg_var, SQL_SEQ_CHAR_NOP);
SQL_SEQ_UNARY_OP(cum_agg_dev, SQL_SEQ_CHAR_NOP);
SQL_SEQ_UNARY_OP(cum_agg_var_samp, SQL_SEQ_CHAR_NOP);
SQL_SEQ_UNARY_OP(cum_agg_dev_samp, SQL_SEQ_CHAR_NOP);

SQL_SEQ_UNARY_OP_NAME(int8_from, integer, SQL_SEQ_CHAR_NOP);
SQL_SEQ_UNARY_OP_NAME(double_from, real, SQL_SEQ_CHAR_NOP);

#define SQL_SEQ_UNARY_OP_WITH_PARAM(FUNC, PARAM_TYPE, CHAR_OP, PAR)   \
    static McoSql::Value* mco_seq_sql_##FUNC(McoGenericSequence* seq, McoSql::Value* param) \
    {                                                                   \
        if (seq->type() != tpSequence) {                        \
            MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");      \
        }                                                               \
        PARAM_TYPE p = (PARAM_TYPE)param->intValue();                   \
        McoGenericSequence* result = seq->create();          \
        mco_seq_iterator_t& in = seq->getIterator();                    \
        switch (seq->elemType) {                                        \
          case tpInt1:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int1(&result->iterator, &in, p)); \
              break;                                                    \
          case tpInt2:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int2(&result->iterator, &in, p)); \
              break;                                                    \
          case tpInt4:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int4(&result->iterator, &in, p)); \
              break;                                                    \
          case tpInt8:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int8(&result->iterator, &in, p)); \
              break;                                                    \
          case tpUInt1:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint1(&result->iterator, &in, p)); \
              break;                                                    \
          case tpUInt2:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint2(&result->iterator, &in, p)); \
              break;                                                    \
          case tpUInt4:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint4(&result->iterator, &in, p)); \
              break;                                                    \
          case tpUInt8:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint8(&result->iterator, &in, p)); \
              break;                                                    \
          case tpReal4:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_float(&result->iterator, &in, p)); \
              break;                                                    \
          case tpReal8:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_double(&result->iterator, &in, p)); \
              break;                                                    \
          case tpString:                                        \
              CHAR_OP(McoDatabase::checkStatus(mco_seq_##FUNC##_char(&result->iterator, &in, p))); \
          default:                                                      \
              MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");    \
        }                                                               \
        result->updateType();                                           \
        return PAR(result);                                             \
    }                                                                   \
    static SqlFunctionDeclaration FUNC##_decl(tpSequence, "seq_"#FUNC, (void*)mco_seq_sql_##FUNC, 2)


#define MCO_SEQ_SEQUENTIAL(r) r
#define MCO_SEQ_PARALLEL(r)  (r)->getParallelSequence()

SQL_SEQ_UNARY_OP_WITH_PARAM(grid_agg_max, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(grid_agg_min, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(grid_agg_sum, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(grid_agg_avg, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(grid_agg_var, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(grid_agg_dev, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(grid_agg_var_samp, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(grid_agg_dev_samp, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(window_agg_max, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(window_agg_min, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(window_agg_sum, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(window_agg_avg, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(window_agg_var, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(window_agg_dev, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(window_agg_var_samp, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(window_agg_dev_samp, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(window_agg_ema, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(window_agg_atr, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(top_max, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_PARALLEL);
SQL_SEQ_UNARY_OP_WITH_PARAM(top_min, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_PARALLEL);
SQL_SEQ_UNARY_OP_WITH_PARAM(top_pos_max, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_PARALLEL);
SQL_SEQ_UNARY_OP_WITH_PARAM(top_pos_min, mco_size_t, SQL_SEQ_CHAR_NOP, MCO_SEQ_PARALLEL);
SQL_SEQ_UNARY_OP_WITH_PARAM(repeat, int, SQL_SEQ_CHAR_OP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(cross, int, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);
SQL_SEQ_UNARY_OP_WITH_PARAM(extrema, int, SQL_SEQ_CHAR_NOP, MCO_SEQ_SEQUENTIAL);

#define SQL_SEQ_ANY_BIN_OP(OP, SEQ_TYPE, REENTRANT)                     \
    static McoSql::Value* mco_seq_sql_##OP(McoGenericSequence* left, McoGenericSequence* right) \
    {                                                                   \
        if (left->type() != tpSequence || right->type() != tpSequence) { \
            MCO_THROW RuntimeError("seq_" #OP ": illegal type");      \
        }                                                               \
        McoGenericSequence* result = new (SEQ_TYPE, left->allocator) McoGenericSequence(left->allocator, SEQ_TYPE, REENTRANT); \
        mco_seq_iterator_t& l = left->getIterator();                    \
        mco_seq_iterator_t& r = right->getIterator();                   \
        McoDatabase::checkStatus(mco_seq_##OP(&result->iterator, &l, &r)); \
        return result;                                                  \
    }                                                                   \
    static SqlFunctionDeclaration OP##_decl(tpSequence, "seq_"#OP, (void*)mco_seq_sql_##OP, 2)

SQL_SEQ_ANY_BIN_OP(and, tpUInt1, true);
SQL_SEQ_ANY_BIN_OP(xor, tpUInt1, true);
SQL_SEQ_ANY_BIN_OP(or, tpUInt1, true);
SQL_SEQ_ANY_BIN_OP(filter_search, tpUInt8, true);
SQL_SEQ_ANY_BIN_OP(group_agg_approxdc, tpUInt4, false);

#define SQL_SEQ_ANY_UNARY_OP(OP, SEQ_TYPE, REENTRANT)                   \
    static McoSql::Value* mco_seq_sql_##OP(McoGenericSequence* seq)     \
    {                                                                   \
        if (seq->type() != tpSequence) {                        \
            MCO_THROW RuntimeError("seq_" #OP ": illegal type");      \
        }                                                               \
        McoGenericSequence* result = new (SEQ_TYPE, seq->allocator) McoGenericSequence(seq->allocator, SEQ_TYPE, REENTRANT); \
        mco_seq_iterator_t& in = seq->getIterator();                    \
        McoDatabase::checkStatus(mco_seq_##OP(&result->iterator, &in)); \
        return result;                                                  \
    }                                                                   \
    static SqlFunctionDeclaration OP##_decl(tpSequence, "seq_"#OP, (void*)mco_seq_sql_##OP, 1)

SQL_SEQ_ANY_UNARY_OP(not, tpUInt1, true);
SQL_SEQ_ANY_UNARY_OP(group_agg_count, tpUInt8, false);
SQL_SEQ_ANY_UNARY_OP(filter_pos, tpUInt8, true);

static McoSql::Value* mco_seq_sql_filter_first_pos(McoGenericSequence* seq, McoSql::Value* n)
{
    if (seq->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_filter_pos: illegal type");
    }
    McoGenericSequence* result = new (tpUInt8, seq->allocator) McoGenericSequence(seq->allocator, tpUInt8);
    mco_seq_iterator_t& in = seq->getIterator();
    McoDatabase::checkStatus(mco_seq_filter_first_pos(&result->iterator, &in, n->intValue()));
    return result->getParallelSequence();
}
static SqlFunctionDeclaration filter_first_pos_decl(tpSequence, "seq_filter_first_pos", (void*)mco_seq_sql_filter_first_pos, 2);


#define SQL_SEQ_HASH_GROUP_BY_OP(FUNC) \
    static McoSql::Value* mco_seq_sql_##FUNC(McoGenericSequence* left, McoGenericSequence* right)  \
    {                                                                   \
        if (left->type() != tpSequence || right->type() != tpSequence) { \
            MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");      \
        }                                                               \
        mco_seq_iterator_t& l = left->getIterator();                    \
        mco_seq_iterator_t& r = right->getIterator();                   \
        McoGenericSequence* result = left->create();                    \
        result->groupBy = right->create();                              \
        switch (left->elemType) {                                       \
          case tpInt1:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int1(&result->iterator, &result->groupBy->iterator, &l, &r, 0)); \
              break;                                                    \
          case tpInt2:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int2(&result->iterator, &result->groupBy->iterator, &l, &r, 0)); \
              break;                                                    \
          case tpInt4:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int4(&result->iterator, &result->groupBy->iterator, &l, &r, 0)); \
              break;                                                    \
          case tpInt8:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int8(&result->iterator, &result->groupBy->iterator, &l, &r, 0)); \
              break;                                                    \
          case tpUInt1:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint1(&result->iterator, &result->groupBy->iterator, &l, &r, 0)); \
              break;                                                    \
          case tpUInt2:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint2(&result->iterator, &result->groupBy->iterator, &l, &r, 0)); \
              break;                                                    \
          case tpUInt4:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint4(&result->iterator, &result->groupBy->iterator, &l, &r, 0)); \
              break;                                                    \
          case tpUInt8:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint8(&result->iterator, &result->groupBy->iterator, &l, &r, 0)); \
              break;                                                    \
          case tpReal4:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_float(&result->iterator, &result->groupBy->iterator, &l, &r, 0)); \
              break;                                                    \
          case tpReal8:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_double(&result->iterator, &result->groupBy->iterator, &l, &r, 0)); \
              break;                                                    \
          default:                                                      \
            MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");      \
        }                                                               \
        result->updateType();                                           \
        return result->getParallelSequence();                           \
    }                                                                   \
    static SqlFunctionDeclaration FUNC##_decl(tpSequence, "seq_"#FUNC, (void*)mco_seq_sql_##FUNC, 2)

SQL_SEQ_HASH_GROUP_BY_OP(hash_agg_min);
SQL_SEQ_HASH_GROUP_BY_OP(hash_agg_max);
SQL_SEQ_HASH_GROUP_BY_OP(hash_agg_sum);
SQL_SEQ_HASH_GROUP_BY_OP(hash_agg_avg);

static McoSql::Value* mco_seq_sql_hash_agg_count(McoGenericSequence* groupBy)
{
    if (groupBy->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_hash_agg_count: illegal type");
    }
    McoGenericSequence* result = new (tpUInt8, groupBy->allocator) McoGenericSequence(groupBy->allocator, tpUInt8);
    result->groupBy = groupBy->create();
    McoDatabase::checkStatus(mco_seq_hash_agg_count(&result->iterator, &result->groupBy->iterator, &groupBy->getIterator(), 0));
    return result->getParallelSequence();
}
static SqlFunctionDeclaration hash_agg_count_decl(tpSequence, "seq_hash_agg_count", (void*)mco_seq_sql_hash_agg_count, 1);

static McoSql::Value* mco_seq_sql_hash_agg_approxdc(McoGenericSequence* input, McoGenericSequence* groupBy)
{
    if (input->type() != tpSequence || groupBy->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_hash_agg_approxdc: illegal type");
    }
    McoGenericSequence* result = new (tpUInt4, groupBy->allocator) McoGenericSequence(groupBy->allocator, tpUInt4);
    result->groupBy = groupBy->create();
    McoDatabase::checkStatus(mco_seq_hash_agg_approxdc(&result->iterator, &result->groupBy->iterator, &input->getIterator(), &groupBy->getIterator(), 0));
    return result->getParallelSequence();
}
static SqlFunctionDeclaration hash_agg_approxdc_decl(tpSequence, "seq_hash_agg_approxdc", (void*)mco_seq_sql_hash_agg_approxdc, 2);

static McoSql::Value* mco_seq_sql_hash_agg_dup_count(McoGenericSequence* input, McoGenericSequence* groupBy, McoSql::Value* minOcc)
{
    if (input->type() != tpSequence || groupBy->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_hash_agg_dup_count: illegal type");
    }
    McoGenericSequence* result = new (tpUInt8, groupBy->allocator) McoGenericSequence(groupBy->allocator, tpUInt8);
    result->groupBy = groupBy->create();
    McoDatabase::checkStatus(mco_seq_hash_agg_dup_count(&result->iterator, &result->groupBy->iterator, &input->getIterator(), &groupBy->getIterator(), 0, 0, (mco_size_t)minOcc->intValue()));
    return result->getParallelSequence();
}
static SqlFunctionDeclaration hash_agg_dup_count_decl(tpSequence, "seq_hash_agg_dup_count", (void*)mco_seq_sql_hash_agg_dup_count, 3);

static McoSql::Value* mco_seq_sql_hash_group_by(McoGenericSequence* agg)
{
    if (agg == NULL || agg->type() != tpSequence || agg->groupBy == NULL) {
        MCO_THROW RuntimeError("seq_hash_group_by: illegal type");
    }
    return agg->groupBy;
}
static SqlFunctionDeclaration hash_group_by_decl(tpSequence, "seq_hash_group_by", (void*)mco_seq_sql_hash_group_by, 1);


static McoSql::Value* mco_seq_sql_sort(McoGenericSequence* seq, McoSql::Value* ord)
{
    if (seq->type() != tpSequence || ord->type() != tpString) {
        MCO_THROW RuntimeError("seq_sort: illegal type");
    }
    McoGenericSequence* result = seq->create();
    mco_size_t count = (mco_size_t)seq->count();
    mco_size_t n = count;
    seq->reset();
    mco_seq_order_t order;
    if (((String*)ord)->compare("asc") == 0) {
        order = MCO_SEQ_ASC_ORDER;
    } else if (((String*)ord)->compare("desc") == 0) {
        order = MCO_SEQ_DESC_ORDER;
    } else {
        MCO_THROW RuntimeError("seq_sort: incorrect order");
    }
    result->permutation = (mco_seq_no_t*)mco_seq_sql_malloc(count*sizeof(mco_seq_no_t));
    if (!result->permutation) {
        MCO_THROW NotEnoughMemory(0, 0, 0);
    }
    result->n_elems = count;
    result->groupBy = seq->groupBy;
    mco_seq_iterator_t& iterator = seq->getIterator();

    switch (seq->elemType) {
      case tpInt1:
      {
          int1* data = (int1*)mco_seq_sql_malloc(count*sizeof(int1));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_int1(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_sort_int1(data, result->permutation, count, order));
          McoDatabase::checkStatus(mco_seq_order_by_int1(&result->iterator, &iterator, result->permutation, count, data));
          break;
      }
      case tpInt2:
      {
          int2* data = (int2*)mco_seq_sql_malloc(count*sizeof(int2));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_int2(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_sort_int2(data, result->permutation, count, order));
          McoDatabase::checkStatus(mco_seq_order_by_int2(&result->iterator, &iterator, result->permutation, count, data));
          break;
      }
      case tpInt4:
      {
          int4* data = (int4*)mco_seq_sql_malloc(count*sizeof(int4));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_int4(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_sort_int4(data, result->permutation, count, order));
          McoDatabase::checkStatus(mco_seq_order_by_int4(&result->iterator, &iterator, result->permutation, count, data));
          break;
      }
      case tpInt8:
      {
          mco_int8* data = (mco_int8*)mco_seq_sql_malloc(count*sizeof(mco_int8));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_int8(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_sort_int8(data, result->permutation, count, order));
          McoDatabase::checkStatus(mco_seq_order_by_int8(&result->iterator, &iterator, result->permutation, count, data));
          break;
      }
      case tpUInt1:
      {
          uint1* data = (uint1*)mco_seq_sql_malloc(count*sizeof(uint1));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_uint1(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_sort_uint1(data, result->permutation, count, order));
          McoDatabase::checkStatus(mco_seq_order_by_uint1(&result->iterator, &iterator, result->permutation, count, data));
          break;
      }
      case tpUInt2:
      {
          uint2* data = (uint2*)mco_seq_sql_malloc(count*sizeof(uint2));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_uint2(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_sort_uint2(data, result->permutation, count, order));
          McoDatabase::checkStatus(mco_seq_order_by_uint2(&result->iterator, &iterator, result->permutation, count, data));
          break;
      }
      case tpUInt4:
      {
          uint4* data = (uint4*)mco_seq_sql_malloc(count*sizeof(uint4));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_uint4(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_sort_uint4(data, result->permutation, count, order));
          McoDatabase::checkStatus(mco_seq_order_by_uint4(&result->iterator, &iterator, result->permutation, count, data));
          break;
      }
      case tpUInt8:
      {
          uint8* data = (uint8*)mco_seq_sql_malloc(count*sizeof(uint8));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_uint8(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_sort_uint8(data, result->permutation, count, order));
          McoDatabase::checkStatus(mco_seq_order_by_uint8(&result->iterator, &iterator, result->permutation, count, data));
          break;
      }
      case tpReal4:
      {
          float* data = (float*)mco_seq_sql_malloc(count*sizeof(float));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_float(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_sort_float(data, result->permutation, count, order));
          McoDatabase::checkStatus(mco_seq_order_by_float(&result->iterator, &iterator, result->permutation, count, data));
          break;
      }
      case tpReal8:
      {
          double* data = (double*)mco_seq_sql_malloc(count*sizeof(double));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_double(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_sort_double(data, result->permutation, count, order));
          McoDatabase::checkStatus(mco_seq_order_by_double(&result->iterator, &iterator, result->permutation, count, data));
          break;
      }
      default:
        MCO_THROW RuntimeError("seq_sort: illegal type");
    }
    return result;
}
static SqlFunctionDeclaration seq_sort_decl(tpSequence, "seq_sort", (void*)mco_seq_sql_sort, 2);


static McoSql::Value* mco_seq_sql_order_by(McoGenericSequence* seq, McoGenericSequence* sort)
{
    if (seq->type() != tpSequence || sort == NULL || sort->type() != tpSequence || !sort->permutation) {
        MCO_THROW RuntimeError("seq_order_by: illegal type");
    }
    McoGenericSequence* result = seq->create();
    mco_seq_iterator_t& iterator = seq->getIterator();
    switch (seq->elemType) {
      case tpInt1:
        McoDatabase::checkStatus(mco_seq_order_by_int1(&result->iterator, &iterator, sort->permutation, sort->n_elems, NULL));
        break;
      case tpInt2:
        McoDatabase::checkStatus(mco_seq_order_by_int2(&result->iterator, &iterator, sort->permutation, sort->n_elems, NULL));
        break;
      case tpInt4:
        McoDatabase::checkStatus(mco_seq_order_by_int4(&result->iterator, &iterator, sort->permutation, sort->n_elems, NULL));
        break;
      case tpInt8:
        McoDatabase::checkStatus(mco_seq_order_by_int8(&result->iterator, &iterator, sort->permutation, sort->n_elems, NULL));
        break;
      case tpUInt1:
        McoDatabase::checkStatus(mco_seq_order_by_uint1(&result->iterator, &iterator, sort->permutation, sort->n_elems, NULL));
        break;
      case tpUInt2:
        McoDatabase::checkStatus(mco_seq_order_by_uint2(&result->iterator, &iterator, sort->permutation, sort->n_elems, NULL));
        break;
      case tpUInt4:
        McoDatabase::checkStatus(mco_seq_order_by_uint4(&result->iterator, &iterator, sort->permutation, sort->n_elems, NULL));
        break;
      case tpUInt8:
        McoDatabase::checkStatus(mco_seq_order_by_uint8(&result->iterator, &iterator, sort->permutation, sort->n_elems, NULL));
        break;
      case tpReal4:
        McoDatabase::checkStatus(mco_seq_order_by_float(&result->iterator, &iterator, sort->permutation, sort->n_elems, NULL));
        break;
      case tpReal8:
        McoDatabase::checkStatus(mco_seq_order_by_double(&result->iterator, &iterator, sort->permutation, sort->n_elems, NULL));
        break;
      case tpString:
        McoDatabase::checkStatus(mco_seq_order_by_char(&result->iterator, &iterator, sort->permutation, sort->n_elems, NULL));
        break;
      default:
        MCO_THROW RuntimeError("Illegal type");
    }
    return result;
}
static SqlFunctionDeclaration order_by_decl(tpSequence, "seq_order_by", (void*)mco_seq_sql_order_by, 2);

static McoSql::Value* mco_seq_sql_dup(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_dup: illegal type");
    }
    return seq->clone(seq->allocator);
}
static SqlFunctionDeclaration seq_dup_decl(tpSequence, "seq_dup", (void*)mco_seq_sql_dup, 1);

static McoSql::Value* mco_seq_sql_materialize(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_materialize: illegal type");
    }
    McoGenericSequence* result = seq->create();
    mco_size_t count = (mco_size_t)seq->count();
    mco_size_t n = count;
    seq->reset();

    mco_seq_iterator_t& iterator = seq->getIterator();

    switch (seq->elemType) {
      case tpInt1:
      {
          int1* data = (int1*)mco_seq_sql_malloc(count*sizeof(int1));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_int1(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_unget_int1(&result->iterator, data, count));
          break;
      }
      case tpInt2:
      {
          int2* data = (int2*)mco_seq_sql_malloc(count*sizeof(int2));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_int2(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_unget_int2(&result->iterator, data, count));
          break;
      }
      case tpInt4:
      {
          int4* data = (int4*)mco_seq_sql_malloc(count*sizeof(int4));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_int4(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_unget_int4(&result->iterator, data, count));
          break;
      }
      case tpInt8:
      {
          mco_int8* data = (mco_int8*)mco_seq_sql_malloc(count*sizeof(mco_int8));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_int8(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_unget_int8(&result->iterator, data, count));
          break;
      }
      case tpUInt1:
      {
          uint1* data = (uint1*)mco_seq_sql_malloc(count*sizeof(uint1));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_uint1(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_unget_uint1(&result->iterator, data, count));
          break;
      }
      case tpUInt2:
      {
          uint2* data = (uint2*)mco_seq_sql_malloc(count*sizeof(uint2));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_uint2(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_unget_uint2(&result->iterator, data, count));
          break;
      }
      case tpUInt4:
      {
          uint4* data = (uint4*)mco_seq_sql_malloc(count*sizeof(uint4));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_uint4(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_unget_uint4(&result->iterator, data, count));
          break;
      }
      case tpUInt8:
      {
          uint8* data = (uint8*)mco_seq_sql_malloc(count*sizeof(uint8));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_uint8(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_unget_uint8(&result->iterator, data, count));
          break;
      }
      case tpReal4:
      {
          float* data = (float*)mco_seq_sql_malloc(count*sizeof(float));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_float(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_unget_float(&result->iterator, data, count));
          break;
      }
      case tpReal8:
      {
          double* data = (double*)mco_seq_sql_malloc(count*sizeof(double));
          if (!data) {
              MCO_THROW NotEnoughMemory(0, 0, 0);
          }
          McoDatabase::checkStatus(mco_seq_get_double(&iterator, data, &n));
          if (n != count) {
              MCO_THROW RuntimeError("Failed to extract sequence");
          }
          McoDatabase::checkStatus(mco_seq_unget_double(&result->iterator, data, count));
          break;
      }
      default:
        MCO_THROW RuntimeError("seq_materialize: illegal type");
    }
    return result;
}
static SqlFunctionDeclaration seq_materialize_decl(tpSequence, "seq_materialize", (void*)mco_seq_sql_materialize, 1);


static McoSql::Value* mco_seq_sql_internal_materialize(McoGenericSequence* seq, Array* arr, IntValue* frameMark)
{
    if (seq->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_materialize: illegal type");
    }
    AbstractAllocator* allocator = seq->allocator;
    mco_seq_iterator_t& iterator = seq->getIterator();
    mco_size_t mark = (mco_size_t)frameMark->intValue();
    void* buf = arr->pointer();
    mco_size_t bufSize = arr->size();
    mco_size_t count;
    McoGenericSequence* result = NULL;

    switch (seq->elemType) {
      case tpInt1:
          count = bufSize;
          McoDatabase::checkStatus(mco_seq_get_int1(&iterator, (int1*)buf, &count));
          if (count == bufSize) { // buffer overflow
              seq->reset();
              bufSize = count = (mco_size_t)seq->count();
              buf = mco_seq_sql_malloc(bufSize);
              if (!buf) {
                  MCO_THROW NotEnoughMemory(0, 0, 0);
              }
              seq->reset();
              McoDatabase::checkStatus(mco_seq_get_int1(&iterator, (int1*)buf, &count));
              if (count != bufSize) {
                  MCO_THROW RuntimeError("Failed to extract sequence");
              }
          } else {
              allocator->reset(mark);
          }
          result = new (allocator) McoGenericSequence(allocator, tpInt1);
          McoDatabase::checkStatus(mco_seq_unget_int1(&result->iterator, (int1*)buf, count));
          break;
      case tpInt2:
          count = bufSize /= 2;
          McoDatabase::checkStatus(mco_seq_get_int2(&iterator, (int2*)buf, &count));
          if (count == bufSize) { // buffer overflow
              seq->reset();
              bufSize = count = (mco_size_t)seq->count();
              buf = mco_seq_sql_malloc(bufSize*2);
              if (!buf) {
                  MCO_THROW NotEnoughMemory(0, 0, 0);
              }
              seq->reset();
              McoDatabase::checkStatus(mco_seq_get_int2(&iterator, (int2*)buf, &count));
              if (count != bufSize) {
                  MCO_THROW RuntimeError("Failed to extract sequence");
              }
          } else {
              allocator->reset(mark);
          }
          result = new (allocator) McoGenericSequence(allocator, tpInt2);
          McoDatabase::checkStatus(mco_seq_unget_int2(&result->iterator, (int2*)buf, count));
          break;
      case tpInt4:
          count = bufSize /= 4;
          McoDatabase::checkStatus(mco_seq_get_int4(&iterator, (int4*)buf, &count));
          if (count == bufSize) { // buffer overflow
              seq->reset();
              bufSize = count = (mco_size_t)seq->count();
              buf = mco_seq_sql_malloc(bufSize*4);
              if (!buf) {
                  MCO_THROW NotEnoughMemory(0, 0, 0);
              }
              seq->reset();
              McoDatabase::checkStatus(mco_seq_get_int4(&iterator, (int4*)buf, &count));
              if (count != bufSize) {
                  MCO_THROW RuntimeError("Failed to extract sequence");
              }
          } else {
              allocator->reset(mark);
          }
          result = new (allocator) McoGenericSequence(allocator, tpInt4);
          McoDatabase::checkStatus(mco_seq_unget_int4(&result->iterator, (int4*)buf, count));
          break;
      case tpInt8:
          count = bufSize /= 8;
          McoDatabase::checkStatus(mco_seq_get_int8(&iterator, (mco_int8*)buf, &count));
          if (count == bufSize) { // buffer overflow
              seq->reset();
              bufSize = count = (mco_size_t)seq->count();
              buf = mco_seq_sql_malloc(bufSize*8);
              if (!buf) {
                  MCO_THROW NotEnoughMemory(0, 0, 0);
              }
              seq->reset();
              McoDatabase::checkStatus(mco_seq_get_int8(&iterator, (mco_int8*)buf, &count));
              if (count != bufSize) {
                  MCO_THROW RuntimeError("Failed to extract sequence");
              }
          } else {
              allocator->reset(mark);
          }
          result = new (allocator) McoGenericSequence(allocator, tpInt8);
          McoDatabase::checkStatus(mco_seq_unget_int8(&result->iterator, (mco_int8*)buf, count));
          break;
      case tpUInt1:
          count = bufSize;
          McoDatabase::checkStatus(mco_seq_get_uint1(&iterator, (uint1*)buf, &count));
          if (count == bufSize) { // buffer overflow
              seq->reset();
              bufSize = count = (mco_size_t)seq->count();
              buf = mco_seq_sql_malloc(bufSize);
              if (!buf) {
                  MCO_THROW NotEnoughMemory(0, 0, 0);
              }
              seq->reset();
              McoDatabase::checkStatus(mco_seq_get_uint1(&iterator, (uint1*)buf, &count));
              if (count != bufSize) {
                  MCO_THROW RuntimeError("Failed to extract sequence");
              }
          } else {
              allocator->reset(mark);
          }
          result = new (allocator) McoGenericSequence(allocator, tpUInt1);
          McoDatabase::checkStatus(mco_seq_unget_uint1(&result->iterator, (uint1*)buf, count));
          break;
      case tpUInt2:
          count = bufSize /= 2;
          McoDatabase::checkStatus(mco_seq_get_uint2(&iterator, (uint2*)buf, &count));
          if (count == bufSize) { // buffer overflow
              seq->reset();
              bufSize = count = (mco_size_t)seq->count();
              buf = mco_seq_sql_malloc(bufSize*2);
              if (!buf) {
                  MCO_THROW NotEnoughMemory(0, 0, 0);
              }
              seq->reset();
              McoDatabase::checkStatus(mco_seq_get_uint2(&iterator, (uint2*)buf, &count));
              if (count != bufSize) {
                  MCO_THROW RuntimeError("Failed to extract sequence");
              }
          } else {
              allocator->reset(mark);
          }
          result = new (allocator) McoGenericSequence(allocator, tpUInt2);
          McoDatabase::checkStatus(mco_seq_unget_uint2(&result->iterator, (uint2*)buf, count));
          break;
      case tpUInt4:
          count = bufSize /= 4;
          McoDatabase::checkStatus(mco_seq_get_uint4(&iterator, (uint4*)buf, &count));
          if (count == bufSize) { // buffer overflow
              seq->reset();
              bufSize = count = (mco_size_t)seq->count();
              buf = mco_seq_sql_malloc(bufSize*4);
              if (!buf) {
                  MCO_THROW NotEnoughMemory(0, 0, 0);
              }
              seq->reset();
              McoDatabase::checkStatus(mco_seq_get_uint4(&iterator, (uint4*)buf, &count));
              if (count != bufSize) {
                  MCO_THROW RuntimeError("Failed to extract sequence");
              }
          } else {
              allocator->reset(mark);
          }
          result = new (allocator) McoGenericSequence(allocator, tpUInt4);
          McoDatabase::checkStatus(mco_seq_unget_uint4(&result->iterator, (uint4*)buf, count));
          break;
      case tpUInt8:
          count = bufSize /= 8;
          McoDatabase::checkStatus(mco_seq_get_uint8(&iterator, (uint8*)buf, &count));
          if (count == bufSize) { // buffer overflow
              seq->reset();
              bufSize = count = (mco_size_t)seq->count();
              buf = mco_seq_sql_malloc(bufSize*8);
              if (!buf) {
                  MCO_THROW NotEnoughMemory(0, 0, 0);
              }
              seq->reset();
              McoDatabase::checkStatus(mco_seq_get_uint8(&iterator, (uint8*)buf, &count));
              if (count != bufSize) {
                  MCO_THROW RuntimeError("Failed to extract sequence");
              }
          } else {
              allocator->reset(mark);
          }
          result = new (allocator) McoGenericSequence(allocator, tpUInt8);
          McoDatabase::checkStatus(mco_seq_unget_uint8(&result->iterator, (uint8*)buf, count));
          break;
      case tpReal4:
          count = bufSize /= 4;
          McoDatabase::checkStatus(mco_seq_get_float(&iterator, (float*)buf, &count));
          if (count == bufSize) { // buffer overflow
              seq->reset();
              bufSize = count = (mco_size_t)seq->count();
              buf = mco_seq_sql_malloc(bufSize*4);
              if (!buf) {
                  MCO_THROW NotEnoughMemory(0, 0, 0);
              }
              seq->reset();
              McoDatabase::checkStatus(mco_seq_get_float(&iterator, (float*)buf, &count));
              if (count != bufSize) {
                  MCO_THROW RuntimeError("Failed to extract sequence");
              }
          } else {
              allocator->reset(mark);
          }
          result = new (allocator) McoGenericSequence(allocator, tpReal4);
          McoDatabase::checkStatus(mco_seq_unget_float(&result->iterator, (float*)buf, count));
          break;
      case tpReal8:
          count = bufSize /= 8;
          McoDatabase::checkStatus(mco_seq_get_double(&iterator, (double*)buf, &count));
          if (count == bufSize) { // buffer overflow
              seq->reset();
              bufSize = count = (mco_size_t)seq->count();
              buf = mco_seq_sql_malloc(bufSize*8);
              if (!buf) {
                  MCO_THROW NotEnoughMemory(0, 0, 0);
              }
              seq->reset();
              McoDatabase::checkStatus(mco_seq_get_double(&iterator, (double*)buf, &count));
              if (count != bufSize) {
                  MCO_THROW RuntimeError("Failed to extract sequence");
              }
          } else {
              allocator->reset(mark);
          }
          result = new (allocator) McoGenericSequence(allocator, tpReal8);
          McoDatabase::checkStatus(mco_seq_unget_double(&result->iterator, (double*)buf, count));
          break;
      default:
        MCO_THROW RuntimeError("seq_materialize: illegal type");
    }
    result->isIgnored = seq->isIgnored;
    return result;
}
static SqlFunctionDeclaration seq_internal_materialize_decl(tpSequence, "seq_internal_materialize", (void*)mco_seq_sql_internal_materialize, 3);



McoSql::Value* mco_seq_sql_agg_count(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_count: illegal type");
    }
    return new (seq->allocator) IntValue((mco_int8)seq->count());
}
static SqlFunctionDeclaration agg_count_decl(tpInt8, "seq_count", (void*)mco_seq_sql_agg_count, 1);

McoSql::Value* mco_seq_sql_approxdc(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_approxdc: illegal type");
    }
    McoGenericSequence result;
    mco_seq_iterator_t* in = &seq->getIterator();
    mco_seq_iterator_t* out = &result.iterator;
    uint4 dc;
    McoDatabase::checkStatus(mco_seq_agg_approxdc(out, in));
    out = &result.getParallelSequence()->getIterator();
    McoDatabase::checkStatus(mco_seq_next_uint4(out, &dc));
    return new (seq->allocator) IntValue(dc);
}
static SqlFunctionDeclaration approxdc_decl(tpInt8, "seq_approxdc", (void*)mco_seq_sql_approxdc, 1);

McoSql::Value* mco_seq_sql_empty(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_empty: illegal type");
    }
    seq->getIterator();
    return BoolValue::create(seq->next() == NULL);
}
static SqlFunctionDeclaration empty_decl(tpBool, "seq_empty", (void*)mco_seq_sql_empty, 1);

McoSql::Value* mco_seq_sql_first_real(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_first_real: illegal type");
    }
    seq->getIterator();
    McoSql::Value* value = seq->next();
    return value != NULL ? value->type() != tpReal ? (McoSql::Value*)new (seq->allocator) RealValue(value->realValue()) : value : &Null;
}
static SqlFunctionDeclaration first_real_decl(tpReal, "seq_first_real", (void*)mco_seq_sql_first_real, 1);

McoSql::Value* mco_seq_sql_first_int(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_first_int: illegal type");
    }
    seq->getIterator();
    McoSql::Value* value = seq->next();
    return value != NULL ? value->type() != tpInt ? (McoSql::Value*)new (seq->allocator) IntValue(value->intValue()) : value : &Null;
}
static SqlFunctionDeclaration first_decl(tpInt, "seq_first_int", (void*)mco_seq_sql_first_int, 1);

McoSql::Value* mco_seq_sql_last_real(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_last_real: illegal type");
    }
    if (seq->isField()) {
        switch (seq->elemType) {
          case tpInt1:
            return new (seq->allocator) RealValue(((McoSequence<int1>*)seq)->last());
          case tpInt2:
            return new (seq->allocator) RealValue(((McoSequence<int2>*)seq)->last());
          case tpInt4:
            return new (seq->allocator) RealValue(((McoSequence<int4>*)seq)->last());
          case tpInt8:
            return new (seq->allocator) RealValue(((McoSequence<mco_int8>*)seq)->last());
          case tpUInt1:
            return new (seq->allocator) RealValue(((McoSequence<uint1>*)seq)->last());
          case tpUInt2:
            return new (seq->allocator) RealValue(((McoSequence<uint2>*)seq)->last());
          case tpUInt4:
            return new (seq->allocator) RealValue(((McoSequence<uint4>*)seq)->last());
          case tpUInt8:
            return new (seq->allocator) RealValue(((McoSequence<uint8>*)seq)->last());
          case tpReal4:
            return new (seq->allocator) RealValue(((McoSequence<float>*)seq)->last());
          case tpReal8:
            return new (seq->allocator) RealValue(((McoSequence<double>*)seq)->last());
          default:
            MCO_THROW RuntimeError("seq_last_real: illegal type");
        }
    } else {
        MCO_RET rc;
        int tile_size = 0;
        seq->getIterator();
        while ((rc = seq->iterator.next(&seq->iterator)) == MCO_S_OK) {
            tile_size = seq->iterator.tile_size;
        }
        if (rc != MCO_S_CURSOR_END) {
            McoDatabase::checkStatus(rc);
        }
        if (tile_size == 0) {
            return &Null;
        }
        switch (seq->elemType) {
          case tpInt1:
            return new (seq->allocator) RealValue(seq->iterator.tile.arr_int1[tile_size-1]);
          case tpInt2:
            return new (seq->allocator) RealValue(seq->iterator.tile.arr_int2[tile_size-1]);
          case tpInt4:
            return new (seq->allocator) RealValue(seq->iterator.tile.arr_int4[tile_size-1]);
          case tpInt8:
            return new (seq->allocator) RealValue(seq->iterator.tile.arr_int8[tile_size-1]);
          case tpUInt1:
            return new (seq->allocator) RealValue(seq->iterator.tile.arr_uint1[tile_size-1]);
          case tpUInt2:
            return new (seq->allocator) RealValue(seq->iterator.tile.arr_uint2[tile_size-1]);
          case tpUInt4:
            return new (seq->allocator) RealValue(seq->iterator.tile.arr_uint4[tile_size-1]);
          case tpUInt8:
            return new (seq->allocator) RealValue(seq->iterator.tile.arr_uint8[tile_size-1]);
          case tpReal4:
            return new (seq->allocator) RealValue(seq->iterator.tile.arr_float[tile_size-1]);
          case tpReal8:
            return new (seq->allocator) RealValue(seq->iterator.tile.arr_double[tile_size-1]);
          default:
            MCO_THROW RuntimeError("seq_last_real: illegal type");
        }
    }
}
static SqlFunctionDeclaration last_real_decl(tpReal, "seq_last_real", (void*)mco_seq_sql_last_real, 1);

McoSql::Value* mco_seq_sql_last_int(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_last_int: illegal type");
    }
    if (seq->isField()) {
        switch (seq->elemType) {
          case tpInt1:
            return new (seq->allocator) IntValue(((McoSequence<int1>*)seq)->last());
          case tpInt2:
            return new (seq->allocator) IntValue(((McoSequence<int2>*)seq)->last());
          case tpInt4:
            return new (seq->allocator) IntValue(((McoSequence<int4>*)seq)->last());
          case tpInt8:
            return new (seq->allocator) IntValue(((McoSequence<mco_int8>*)seq)->last());
          case tpUInt1:
            return new (seq->allocator) IntValue(((McoSequence<uint1>*)seq)->last());
          case tpUInt2:
            return new (seq->allocator) IntValue(((McoSequence<uint2>*)seq)->last());
          case tpUInt4:
            return new (seq->allocator) IntValue(((McoSequence<uint4>*)seq)->last());
          case tpUInt8:
            return new (seq->allocator) IntValue(((McoSequence<uint8>*)seq)->last());
          case tpReal4:
            return new (seq->allocator) IntValue((int64_t)((McoSequence<float>*)seq)->last());
          case tpReal8:
            return new (seq->allocator) IntValue((int64_t)((McoSequence<double>*)seq)->last());
          default:
            MCO_THROW RuntimeError("seq_last_int: illegal type");
        }
    } else {
        MCO_RET rc;
        int tile_size = 0;
        seq->getIterator();
        while ((rc = seq->iterator.next(&seq->iterator)) == MCO_S_OK) {
            tile_size = seq->iterator.tile_size;
        }
        if (rc != MCO_S_CURSOR_END) {
            McoDatabase::checkStatus(rc);
        }
        if (tile_size == 0) {
            return &Null;
        }
        switch (seq->elemType) {
          case tpInt1:
            return new (seq->allocator) IntValue(seq->iterator.tile.arr_int1[tile_size-1]);
          case tpInt2:
            return new (seq->allocator) IntValue(seq->iterator.tile.arr_int2[tile_size-1]);
          case tpInt4:
            return new (seq->allocator) IntValue(seq->iterator.tile.arr_int4[tile_size-1]);
          case tpInt8:
            return new (seq->allocator) IntValue(seq->iterator.tile.arr_int8[tile_size-1]);
          case tpUInt1:
            return new (seq->allocator) IntValue(seq->iterator.tile.arr_uint1[tile_size-1]);
          case tpUInt2:
            return new (seq->allocator) IntValue(seq->iterator.tile.arr_uint2[tile_size-1]);
          case tpUInt4:
            return new (seq->allocator) IntValue(seq->iterator.tile.arr_uint4[tile_size-1]);
          case tpUInt8:
            return new (seq->allocator) IntValue(seq->iterator.tile.arr_uint8[tile_size-1]);
          case tpReal4:
            return new (seq->allocator) IntValue((int64_t)seq->iterator.tile.arr_float[tile_size-1]);
          case tpReal8:
            return new (seq->allocator) IntValue((int64_t)seq->iterator.tile.arr_double[tile_size-1]);
          default:
            MCO_THROW RuntimeError("seq_last_int: illegal type");
        }
    }
}
static SqlFunctionDeclaration last_int_decl(tpInt, "seq_last_int", (void*)mco_seq_sql_last_int, 1);


static McoSql::Value* mco_seq_sql_subseq(McoGenericSequence* seq, McoSql::Value* from, McoSql::Value* till)
{
    if (seq->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_subseq: illegal type");
    }
    mco_seq_no_t low = from->isNull() ? 0 : (mco_seq_no_t)from->intValue();
    mco_seq_no_t high = till->isNull() ? MCO_SEQ_INFINITY : (mco_seq_no_t)till->intValue();
    McoGenericSequence* result = seq->create();
    seq->subseq(result->iterator, low, high);
    if (seq->isField()) {
        result->projection = seq;
        result->isScatterable = true;
    }
    return result;
}
static SqlFunctionDeclaration subseq_decl(tpSequence, "seq_subseq", (void*)mco_seq_sql_subseq, 3);

static McoSql::Value* mco_seq_sql_map(McoGenericSequence* seq, McoGenericSequence* map)
{
    if (seq->type() != tpSequence || map == NULL || map->type() != tpSequence) {
        MCO_THROW RuntimeError("Illegal type");
    }
    McoGenericSequence* result = seq->create();
    map->reset(); // make it possible to use positions multiple times
    if (seq->projection != NULL) {
        seq->projection->map(result->iterator, map->getIterator());
        result->iterator.first_seq_no = seq->iterator.first_seq_no;
        result->isReentrant = true;
    } else {
        result->isReentrant = false;
        seq->map(result->iterator, map->getIterator());
    }
    result->updateType();
    return result;
}
static SqlFunctionDeclaration map_decl(tpSequence, "seq_map", (void*)mco_seq_sql_map, 2);

static McoSql::Value* mco_seq_sql_join(McoGenericSequence* seq, McoGenericSequence* join)
{
    if (seq->type() != tpSequence || join == NULL || join->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_join: illegal type");
    }
    McoGenericSequence* result = seq->create();
    result->isReentrant = seq->isField();
    seq->join(result->iterator, join->getIterator());
    result->updateType();
    return result;
}
static SqlFunctionDeclaration join_decl(tpSequence, "seq_join", (void*)mco_seq_sql_join, 2);

static McoSql::Value* mco_seq_sql_search(McoGenericSequence* seq, McoSql::Value* from, McoSql::Value* till)
{
    if (seq->type() != tpSequence || !seq->isField()) {
        MCO_THROW RuntimeError("seq_search: illegal type");
    }
    McoGenericSequence* result = seq->create(true);
    result->isSearchResult = true;
    result->isScatterable = true;
    result->projection = seq;
    switch (seq->elemType) {
      case tpInt1:
        ((McoSequence<int1>*)seq)->search(result->iterator,
                                          from->isNull() ? 0 : (int1)from->intValue(),
                                          from->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE,
                                          till->isNull() ? 0 : (int1)till->intValue(),
                                          till->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE);
        break;
      case tpInt2:
        ((McoSequence<int2>*)seq)->search(result->iterator,
                                          from->isNull() ? 0 : (int2)from->intValue(),
                                          from->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE,
                                          till->isNull() ? 0 : (int2)till->intValue(),
                                          till->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE);
        break;
      case tpInt4:
        ((McoSequence<int4>*)seq)->search(result->iterator,
                                          from->isNull() ? 0 : (int4)from->intValue(),
                                          from->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE,
                                          till->isNull() ? 0 : (int4)till->intValue(),
                                          till->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE);
        break;
      case tpInt8:
        ((McoSequence<mco_int8>*)seq)->search(result->iterator,
                                          from->isNull() ? 0 : from->intValue(),
                                          from->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE,
                                          till->isNull() ? 0 : till->intValue(),
                                          till->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE);
        break;
      case tpUInt1:
        ((McoSequence<uint1>*)seq)->search(result->iterator,
                                           from->isNull() ? 0 : (uint1)from->intValue(),
                                           from->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE,
                                           till->isNull() ? 0 : (uint1)till->intValue(),
                                           till->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE);
        break;
      case tpUInt2:
        ((McoSequence<uint2>*)seq)->search(result->iterator,
                                           from->isNull() ? 0 : (uint2)from->intValue(),
                                           from->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE,
                                           till->isNull() ? 0 : (uint2)till->intValue(),
                                           till->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE);
        break;
      case tpUInt4:
        ((McoSequence<uint4>*)seq)->search(result->iterator,
                                           from->isNull() ? 0 : (uint4)from->intValue(),
                                           from->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE,
                                           till->isNull() ? 0 : (uint4)till->intValue(),
                                           till->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE);
        break;
      case tpUInt8:
        ((McoSequence<uint8>*)seq)->search(result->iterator,
                                           from->isNull() ? 0 : (uint8)from->intValue(),
                                           from->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE,
                                           till->isNull() ? 0 : (uint8)till->intValue(),
                                           till->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE);
        break;
      case tpReal4:
        ((McoSequence<float>*)seq)->search(result->iterator,
                                           from->isNull() ? 0 : (float)from->realValue(),
                                           from->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE,
                                           till->isNull() ? 0 : (float)till->realValue(),
                                           till->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE);
        break;
      case tpReal8:
        ((McoSequence<double>*)seq)->search(result->iterator,
                                            from->isNull() ? 0 : from->realValue(),
                                            from->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE,
                                            till->isNull() ? 0 : till->realValue(),
                                            till->isNull() ? MCO_SEQ_BOUNDARY_OPEN : MCO_SEQ_BOUNDARY_INCLUSIVE);
        break;
      default:
        MCO_THROW RuntimeError("seq_search: illegal type");
    }
    return result;
}
static SqlFunctionDeclaration search_decl(tpSequence, "seq_search", (void*)mco_seq_sql_search, 3);

static McoSql::Value* mco_seq_sql_search_last(McoGenericSequence* seq, McoSql::Value* till, McoSql::Value* limit)
{
    McoGenericSequence* result =  (McoGenericSequence*)mco_seq_sql_search(seq, &Null, till);
    int64_t n = limit->intValue();
    if (result->iterator.first_seq_no + n <= result->iterator.last_seq_no) {
        result->iterator.first_seq_no = result->iterator.next_seq_no = result->iterator.last_seq_no - n + 1;
    }
    return result;
}
static SqlFunctionDeclaration search_last_decl(tpSequence, "seq_search_last", (void*)mco_seq_sql_search_last, 3);

static McoSql::Value* mco_seq_sql_search_first(McoGenericSequence* seq, McoSql::Value* from, McoSql::Value* limit)
{
    McoGenericSequence* result =  (McoGenericSequence*)mco_seq_sql_search(seq, from, &Null);
    int64_t n = limit->intValue();
    if (result->iterator.first_seq_no + n <= result->iterator.last_seq_no) {
        result->iterator.last_seq_no = result->iterator.first_seq_no + n - 1;
    }
    return result;
}
static SqlFunctionDeclaration search_first_decl(tpSequence, "seq_search_first", (void*)mco_seq_sql_search_first, 3);



static McoSql::Value* mco_seq_sql_project(McoGenericSequence* seq, McoGenericSequence* projection)
{
    if (seq->type() != tpSequence || projection->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_project: illegal type");
    }
    McoGenericSequence* result = seq->create(true);
    seq->project(result->iterator, projection->iterator);
    result->projection = seq->projection != NULL ? seq->projection : seq;
    result->isScatterable = true;
    return result;
}
static SqlFunctionDeclaration project_decl(tpSequence, "seq_project", (void*)mco_seq_sql_project, 2);

static McoSql::Value* mco_seq_sql_at(McoGenericSequence* seq, McoGenericSequence* map)
{
    if (seq->type() == tpSequence && map->type() == tpSequence) {
        return map->permutation
            ? mco_seq_sql_order_by(seq, map)
            : map->isSearchResult
                ? mco_seq_sql_project(seq, map)
                : mco_seq_sql_map(seq, map);
    }
    MCO_THROW RuntimeError("seq_at: illegal type");
}
static SqlFunctionDeclaration at_decl(tpSequence, "seq_at", (void*)mco_seq_sql_at, 2);



#define SEQ_STRETCH_DEF(OP,ARGS)                                      \
    static McoSql::Value* mco_seq_sql_##OP(McoGenericSequence* ts1, McoGenericSequence* ts2, McoGenericSequence* values) \
    {                                                                   \
        if (ts1->type() != tpSequence || ts2->type() != tpSequence || values->type() != tpSequence) { \
            MCO_THROW RuntimeError("seq_" #OP ": illegal type");        \
        }                                                               \
        mco_seq_iterator_t& ti1 = ts1->getIterator();                   \
        mco_seq_iterator_t& ti2 = ts2->getIterator();                   \
        mco_seq_iterator_t& vi = values->getIterator();                 \
        McoGenericSequence* result = values->create();                  \
        if (ts1->elemType != ts2->elemType) {                           \
            MCO_THROW RuntimeError("Incompatible timeseries type");     \
        }                                                               \
        switch (ts1->elemType) {                                        \
          case tpUInt4:                                         \
            switch (values->elemType) {                                 \
              case tpInt1:                                      \
                McoDatabase::checkStatus(mco_seq_##OP##_uint4_int1 ARGS); \
                break;                                                  \
              case tpInt2:                                      \
                McoDatabase::checkStatus(mco_seq_##OP##_uint4_int2 ARGS); \
                break;                                                  \
              case tpInt4:                                      \
                McoDatabase::checkStatus(mco_seq_##OP##_uint4_int4 ARGS); \
                break;                                                  \
              case tpInt8:                                      \
                McoDatabase::checkStatus(mco_seq_##OP##_uint4_int8 ARGS); \
                break;                                                  \
              case tpUInt1:                                     \
                McoDatabase::checkStatus(mco_seq_##OP##_uint4_uint1 ARGS); \
                break;                                                  \
              case tpUInt2:                                     \
                McoDatabase::checkStatus(mco_seq_##OP##_uint4_uint2 ARGS); \
                break;                                                  \
              case tpUInt4:                                     \
                McoDatabase::checkStatus(mco_seq_##OP##_uint4_uint4 ARGS); \
                break;                                                  \
              case tpUInt8:                                     \
                McoDatabase::checkStatus(mco_seq_##OP##_uint4_uint8 ARGS); \
                break;                                                  \
              case tpReal4:                                     \
                McoDatabase::checkStatus(mco_seq_##OP##_uint4_float ARGS); \
                break;                                                  \
              case tpReal8:                                     \
                McoDatabase::checkStatus(mco_seq_##OP##_uint4_double ARGS); \
                break;                                                  \
              default:                                                  \
                MCO_THROW RuntimeError("seq_" #OP ": illegal type");                     \
            }                                                           \
            break;                                                      \
          case tpUInt8:                                         \
            switch (values->elemType) {                                 \
              case tpInt1:                                      \
                McoDatabase::checkStatus(mco_seq_##OP##_uint8_int1 ARGS); \
                break;                                                  \
              case tpInt2:                                      \
                McoDatabase::checkStatus(mco_seq_##OP##_uint8_int2 ARGS); \
                break;                                                  \
              case tpInt4:                                      \
                McoDatabase::checkStatus(mco_seq_##OP##_uint8_int4 ARGS); \
                break;                                                  \
              case tpInt8:                                      \
                McoDatabase::checkStatus(mco_seq_##OP##_uint8_int8 ARGS); \
                break;                                                  \
              case tpUInt1:                                     \
                McoDatabase::checkStatus(mco_seq_##OP##_uint8_uint1 ARGS); \
                break;                                                  \
              case tpUInt2:                                     \
                McoDatabase::checkStatus(mco_seq_##OP##_uint8_uint2 ARGS); \
                break;                                                  \
              case tpUInt4:                                     \
                McoDatabase::checkStatus(mco_seq_##OP##_uint8_uint4 ARGS); \
                break;                                                  \
              case tpUInt8:                                     \
                McoDatabase::checkStatus(mco_seq_##OP##_uint8_uint8 ARGS); \
                break;                                                  \
              case tpReal4:                                     \
                McoDatabase::checkStatus(mco_seq_##OP##_uint8_float ARGS); \
                break;                                                  \
              case tpReal8:                                     \
                McoDatabase::checkStatus(mco_seq_##OP##_uint8_double ARGS); \
                break;                                                  \
              default:                                                  \
                MCO_THROW RuntimeError("seq_" #OP ": illegal type");    \
            }                                                           \
            break;                                                      \
          default:                                                      \
            MCO_THROW RuntimeError("seq_" #OP ": unsupported timeseries type"); \
        }                                                               \
        return result;                                                  \
    }                                                                   \
    static SqlFunctionDeclaration OP##_decl(tpSequence, "seq_" #OP, (void*)mco_seq_sql_##OP, 3)

SEQ_STRETCH_DEF(stretch, (&result->iterator, &ti1, &ti2, &vi, 1));
SEQ_STRETCH_DEF(stretch0, (&result->iterator, &ti1, &ti2, &vi, 0));
SEQ_STRETCH_DEF(asof_join, (&result->iterator, &ti1, &ti2, &vi));

#define SQL_SEQ_TENARY_OP(FUNC) \
    static McoSql::Value* mco_seq_sql_##FUNC(McoGenericSequence* s1, McoGenericSequence* s2, McoGenericSequence* s3) \
    {                                                                   \
        if (s1->type() != tpSequence || (s2->type() != tpSequence && s3->type() != tpSequence)) { \
            MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");        \
        }                                                               \
        mco_seq_iterator_t& i1 = s1->getIterator();                     \
        s2 = mco_seq_sql_propagate_operand_type(s3, s2);                \
        s3 = mco_seq_sql_propagate_operand_type(s2, s3);                \
        McoGenericSequence* result = s2->create();                      \
        mco_seq_iterator_t& i2 = s2->getIterator();                     \
        mco_seq_iterator_t& i3 = s3->getIterator();                     \
        switch (s2->elemType) {                                         \
          case tpInt1:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int1(&result->iterator, &i1, &i2, &i3)); \
              break;                                                    \
          case tpInt2:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int2(&result->iterator, &i1, &i2, &i3)); \
              break;                                                    \
          case tpInt4:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int4(&result->iterator, &i1, &i2, &i3)); \
              break;                                                    \
          case tpInt8:                                          \
              McoDatabase::checkStatus(mco_seq_##FUNC##_int8(&result->iterator, &i1, &i2, &i3)); \
              break;                                                    \
          case tpUInt1:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint1(&result->iterator, &i1, &i2, &i3)); \
              break;                                                    \
          case tpUInt2:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint2(&result->iterator, &i1, &i2, &i3)); \
              break;                                                    \
          case tpUInt4:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint4(&result->iterator, &i1, &i2, &i3)); \
              break;                                                    \
          case tpUInt8:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_uint8(&result->iterator, &i1, &i2, &i3)); \
              break;                                                    \
          case tpReal4:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_float(&result->iterator, &i1, &i2, &i3)); \
              break;                                                    \
          case tpReal8:                                         \
              McoDatabase::checkStatus(mco_seq_##FUNC##_double(&result->iterator, &i1, &i2, &i3)); \
              break;                                                    \
          case tpString:                                        \
              McoDatabase::checkStatus(mco_seq_##FUNC##_char(&result->iterator, &i1, &i2, &i3)); \
              break;                                                    \
          default:                                                      \
            MCO_THROW RuntimeError("seq_" #FUNC ": illegal type");                         \
        }                                                               \
        result->updateType();                                           \
        return result;                                                  \
    }                                                                   \
    static SqlFunctionDeclaration FUNC##_decl(tpSequence, "seq_"#FUNC, (void*)mco_seq_sql_##FUNC, 3)

SQL_SEQ_TENARY_OP(iif);
SQL_SEQ_TENARY_OP(if);


void mco_seq_sql_reset(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
         MCO_THROW RuntimeError("Illegal type");
    }
    seq->reset();
}

McoGenericSequence* mco_seq_sql_construct_int(mco_int8* elems, mco_size_t nElems)
{
    McoGenericSequence* seq = new (tpInt) McoGenericSequence(tpInt);
    McoDatabase::checkStatus(mco_seq_unget_int8(&seq->iterator, elems, nElems));
    return seq;
}

McoGenericSequence* mco_seq_sql_construct_double(double* elems, mco_size_t nElems)
{
    McoGenericSequence* seq = new (tpReal) McoGenericSequence(tpReal);
    McoDatabase::checkStatus(mco_seq_unget_double(&seq->iterator, elems, nElems));
    return seq;
}


static McoSql::Value* mco_seq_sql_histogram(McoGenericSequence* seq, McoSql::Value* min, McoSql::Value* max, McoSql::Value* nIntervals)
{
    if (seq->type() != tpSequence || !seq->isField()) {
        MCO_THROW RuntimeError("seq_histogram: illegal type");
    }
    McoGenericSequence* result = new (tpUInt8, seq->allocator) McoGenericSequence(tpUInt8);
    mco_seq_iterator_t& iterator = seq->getIterator();
    switch (seq->elemType) {
      case tpInt1:
        McoDatabase::checkStatus(mco_seq_histogram_int1(&result->iterator, &iterator, (int1)min->intValue(), (int1)max->intValue(), (mco_size_t)nIntervals->intValue()));
        break;
      case tpInt2:
        McoDatabase::checkStatus(mco_seq_histogram_int2(&result->iterator, &iterator, (int2)min->intValue(), (int2)max->intValue(), (mco_size_t)nIntervals->intValue()));
        break;
      case tpInt4:
        McoDatabase::checkStatus(mco_seq_histogram_int4(&result->iterator, &iterator, (int4)min->intValue(), (int4)max->intValue(), (mco_size_t)nIntervals->intValue()));
        break;
      case tpInt8:
        McoDatabase::checkStatus(mco_seq_histogram_int8(&result->iterator, &iterator, min->intValue(), max->intValue(), (mco_size_t)nIntervals->intValue()));
        break;
      case tpUInt1:
        McoDatabase::checkStatus(mco_seq_histogram_uint1(&result->iterator, &iterator, (uint1)min->intValue(), (uint1)max->intValue(), (mco_size_t)nIntervals->intValue()));
        break;
      case tpUInt2:
        McoDatabase::checkStatus(mco_seq_histogram_uint2(&result->iterator, &iterator, (uint2)min->intValue(), (uint2)max->intValue(), (mco_size_t)nIntervals->intValue()));
        break;
      case tpUInt4:
        McoDatabase::checkStatus(mco_seq_histogram_uint4(&result->iterator, &iterator, (uint4)min->intValue(), (uint4)max->intValue(), (mco_size_t)nIntervals->intValue()));
        break;
      case tpUInt8:
        McoDatabase::checkStatus(mco_seq_histogram_uint8(&result->iterator, &iterator, (uint8)min->intValue(), (uint8)max->intValue(), (mco_size_t)nIntervals->intValue()));
        break;
      case tpReal4:
        McoDatabase::checkStatus(mco_seq_histogram_float(&result->iterator, &iterator, (float)min->realValue(), (float)max->realValue(), (mco_size_t)nIntervals->intValue()));
        break;
      case tpReal8:
        McoDatabase::checkStatus(mco_seq_histogram_double(&result->iterator, &iterator, min->realValue(), max->realValue(), (mco_size_t)nIntervals->intValue()));
        break;
      default:
        MCO_THROW RuntimeError("Illegal type");
    }
    result->updateType();
    return result;
}
static SqlFunctionDeclaration histogram_decl(tpSequence, "seq_histogram", (void*)mco_seq_sql_histogram, 4);

static McoSql::Value* mco_seq_print(McoGenericSequence* seq, String* format)
{
    if (seq->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_print: illegal type");
    }
    McoGenericSequence* result = new (tpString, seq->allocator) McoGenericSequence(tpString);
    McoDatabase::checkStatus(mco_seq_print_char(&result->iterator, &seq->getIterator(), MCO_SEQ_MAX_CHAR_SIZE, format->stringValue()->cstr()));
    return result;
}
static SqlFunctionDeclaration printf_decl(tpSequence, "seq_print", (void*)mco_seq_print, 2);


static McoSql::Value* mco_seq_like(McoGenericSequence* seq, String* pattern)
{
    if (seq->type() != tpSequence) {
        MCO_THROW RuntimeError("seq_like: illegal type");
    }
    McoGenericSequence* result = new (tpUInt1, seq->allocator) McoGenericSequence(tpUInt1);
    McoDatabase::checkStatus(mco_seq_match(&result->iterator, &seq->getIterator(), pattern->stringValue()->cstr()));
    return result;
}
static SqlFunctionDeclaration like_decl(tpSequence, "seq_like", (void*)mco_seq_like, 2);


static McoSql::Value* mco_sql_dmy(McoSql::Value* day, McoSql::Value* month, McoSql::Value* year)
{
    return new IntValue(DMY(day->intValue(), month->intValue(), year->intValue()));
}
static SqlFunctionDeclaration dmy_decl(tpInt, "dmy", (void*)mco_sql_dmy, 3);

static McoSql::Value* mco_sql_str2date(McoSql::Value* str)
{
    static char const* const MonthNames[12] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    char const* cstr = str->stringValue()->cstr();
    int d, m = 0, y;
    char month[8];
    if (sscanf(cstr, "%d.%d.%d", &d, &m, &y) == 3
        || sscanf(cstr, "%d/%d/%d", &m, &d, &y) == 3
        || sscanf(cstr, "%d-%[^-]-%d", &d, month, &y) == 3)
    {
        if (m == 0) {
            for (m = 0; m < 12 && strcmp(MonthNames[m], month) != 0; m++);
            if (m == 12) {
                MCO_THROW RuntimeError("Invalid month name");
            }
            m += 1;
        }
        if (y < 100) {
            y += 2000;
        }
        return new IntValue(DMY(d, m, y));
    } else {
        MCO_THROW RuntimeError("Invalid date format");
    }
}
static SqlFunctionDeclaration str2date_decl(tpInt, "str2date", (void*)mco_sql_str2date, 1);

static McoSql::Value* mco_sql_date2str(McoSql::Value* date)
{
    char buf[16];
    int ts = (int)date->intValue();
    sprintf(buf, "%02d.%02d.%d", DAY(ts), MONTH(ts), YEAR(ts));
    return String::create(buf);
}
static SqlFunctionDeclaration date2str_decl(tpString, "date2str", (void*)mco_sql_date2str, 1);

static McoSql::Value* mco_sql_day(McoSql::Value* date)
{
    return new IntValue(DAY(date->intValue()));
}
static SqlFunctionDeclaration day_decl(tpInt, "get_day", (void*)mco_sql_day, 1);


static McoSql::Value* mco_sql_month(McoSql::Value* date)
{
    return new IntValue(MONTH(date->intValue()));
}
static SqlFunctionDeclaration month_decl(tpInt, "get_month", (void*)mco_sql_month, 1);


static McoSql::Value* mco_sql_year(McoSql::Value* date)
{
    return new IntValue(YEAR(date->intValue()));
}
static SqlFunctionDeclaration year_decl(tpInt, "get_year", (void*)mco_sql_year, 1);


static McoSql::Value* mco_sql_seq_log(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
         MCO_THROW RuntimeError("seq_log: illegal type");
    }
    McoGenericSequence* result = seq->create(true);
    McoDatabase::checkStatus(mco_seq_func_double(&result->iterator, &seq->getIterator(), log));
    return result;
}
static SqlFunctionDeclaration seq_log_decl(tpSequence, "seq_log", (void*)mco_sql_seq_log, 1);

static McoSql::Value* mco_sql_seq_exp(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
         MCO_THROW RuntimeError("seq_exp: illegal type");
    }
    McoGenericSequence* result = seq->create(true);
    McoDatabase::checkStatus(mco_seq_func_double(&result->iterator, &seq->getIterator(), exp));
    return result;
}
static SqlFunctionDeclaration seq_exp_decl(tpSequence, "seq_exp", (void*)mco_sql_seq_exp, 1);

static McoSql::Value* mco_sql_seq_sqrt(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
         MCO_THROW RuntimeError("Illegal type");
    }
    McoGenericSequence* result = seq->create(true);
    McoDatabase::checkStatus(mco_seq_func_double(&result->iterator, &seq->getIterator(), sqrt));
    return result;
}
static SqlFunctionDeclaration seq_sqrt_decl(tpSequence, "seq_sqrt", (void*)mco_sql_seq_sqrt, 1);

static McoSql::Value* mco_sql_seq_sin(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
         MCO_THROW RuntimeError("seq_sin : illegal type");
    }
    McoGenericSequence* result = seq->create(true);
    McoDatabase::checkStatus(mco_seq_func_double(&result->iterator, &seq->getIterator(), sin));
    return result;
}
static SqlFunctionDeclaration seq_sin_decl(tpSequence, "seq_sin", (void*)mco_sql_seq_sin, 1);

static McoSql::Value* mco_sql_seq_cos(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
         MCO_THROW RuntimeError("seq_cos: illegal type");
    }
    McoGenericSequence* result = seq->create(true);
    McoDatabase::checkStatus(mco_seq_func_double(&result->iterator, &seq->getIterator(), cos));
    return result;
}
static SqlFunctionDeclaration seq_cos_decl(tpSequence, "seq_cos", (void*)mco_sql_seq_cos, 1);

static McoSql::Value* mco_sql_seq_tan(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
         MCO_THROW RuntimeError("seq_tan: illegal type");
    }
    McoGenericSequence* result = seq->create(true);
    McoDatabase::checkStatus(mco_seq_func_double(&result->iterator, &seq->getIterator(), tan));
    return result;
}
static SqlFunctionDeclaration seq_tan_decl(tpSequence, "seq_tan", (void*)mco_sql_seq_tan, 1);

static McoSql::Value* mco_sql_seq_atan(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
         MCO_THROW RuntimeError("seq_atan: illegal type");
    }
    McoGenericSequence* result = seq->create(true);
    McoDatabase::checkStatus(mco_seq_func_double(&result->iterator, &seq->getIterator(), atan));
    return result;
}
static SqlFunctionDeclaration seq_atan_decl(tpSequence, "seq_atan", (void*)mco_sql_seq_atan, 1);

static McoSql::Value* mco_sql_seq_asin(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
         MCO_THROW RuntimeError("Illegal type");
    }
    McoGenericSequence* result = seq->create(true);
    McoDatabase::checkStatus(mco_seq_func_double(&result->iterator, &seq->getIterator(), asin));
    return result;
}
static SqlFunctionDeclaration seq_asin_decl(tpSequence, "seq_asin", (void*)mco_sql_seq_asin, 1);

static McoSql::Value* mco_sql_seq_acos(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
         MCO_THROW RuntimeError("seq_acos: illegal type");
    }
    McoGenericSequence* result = seq->create(true);
    McoDatabase::checkStatus(mco_seq_func_double(&result->iterator, &seq->getIterator(), acos));
    return result;
}
static SqlFunctionDeclaration seq_acos_decl(tpSequence, "seq_acos", (void*)mco_sql_seq_acos, 1);

static McoSql::Value* mco_sql_seq_ceil(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
         MCO_THROW RuntimeError("seq_ceil: illegal type");
    }
    McoGenericSequence* result = seq->create(true);
    McoDatabase::checkStatus(mco_seq_func_double(&result->iterator, &seq->getIterator(), ceil));
    return result;
}
static SqlFunctionDeclaration seq_ceil_decl(tpSequence, "seq_ceil", (void*)mco_sql_seq_ceil, 1);

static McoSql::Value* mco_sql_seq_floor(McoGenericSequence* seq)
{
    if (seq->type() != tpSequence) {
         MCO_THROW RuntimeError("seq_floor: illegal type");
    }
    McoGenericSequence* result = seq->create(true);
    McoDatabase::checkStatus(mco_seq_func_double(&result->iterator, &seq->getIterator(), floor));
    return result;
}
static SqlFunctionDeclaration seq_floor_decl(tpSequence, "seq_floor", (void*)mco_sql_seq_floor, 1);

static McoSql::Value* mco_sql_seq_pow(McoGenericSequence* seq, McoGenericSequence* exp)
{
    if (seq->type() != tpSequence) {
         MCO_THROW RuntimeError("seq_pos: illegal type");
    }
    exp = mco_seq_sql_propagate_operand_type(seq, exp);
    McoGenericSequence* result = new (tpReal8, seq->allocator) McoGenericSequence(tpReal8, true);
    McoDatabase::checkStatus(mco_seq_func2_double(&result->iterator, &seq->getIterator(), &exp->getIterator(), pow));
    return result;
}
static SqlFunctionDeclaration seq_pow_decl(tpSequence, "seq_pow", (void*)mco_sql_seq_pow, 2);

#endif

#ifdef UNICODE_SUPPORT
    static void convertNcharToWchar(UnicodeString* str)
    {
        if (sizeof(wchar_t) != sizeof(nchar_t))
        {
            wchar_t* body = str->body();
            int len = str->size();
            wchar_t* dst = body + len;
            nchar_t* src = (nchar_t*)body + len;
            *dst = 0;
            while (--len >= 0)
            {
                *--dst = (wchar_t)* --src;
            }
        }
    }
    static nchar_t* convertWcharToNchar(UnicodeString* str)
    {
        if (sizeof(wchar_t) != sizeof(nchar_t))
        {
            int len = str->size();
            String* tmp = String::create(len*2);
            nchar_t* buf = (nchar_t*)tmp->body();
            wchar_t* src = str->body();
            nchar_t* dst = buf;
            while (--len >= 0)
            {
                *dst++ = * src++;
            }
            *dst = 0;
            return buf;
        }
        else
        {
            return (nchar_t*)str->body();
        }
    }
#endif

void McoDatabase::checkStatus(MCO_RET rc)
{
    switch (rc)
    {
        case MCO_S_OK:
             /* success  */
            return ;
        case MCO_S_NOTFOUND:
             /* search operation failed */
        case MCO_S_CURSOR_END:
             /* cursor cannot be moved */
        case MCO_S_CURSOR_EMPTY:
             /* no objects in index */
            MCO_THROW NoMoreElements();
        case MCO_S_DUPLICATE:
             /* index restriction violated */
            MCO_THROW NotUnique();
        case MCO_E_INVALID_HANDLE:
             /* normally invalid handle produces fatal error. This code returned by special checks. */
            MCO_THROW RuntimeError("Invalid handle");
        case MCO_E_NOMEM:
            MCO_THROW NotEnoughMemory(0, 0, 0);
        case MCO_E_ACCESS:
             /* trying to use read only transaction for write */
            MCO_THROW RuntimeError("Trying to use read only transaction for write");
        case MCO_E_TRANSACT:
             /* transaction is in error state */
            MCO_THROW RuntimeError("Transaction is in error state");
        case MCO_E_INDEXLIMIT:
             /* vector index out of bounds */
        case MCO_E_EMPTYVECTOREL:
             /* vector element was not set (at given index) */
            MCO_THROW IndexOutOfBounds( - 1,  - 1);
        case MCO_E_UNSUPPORTED:
             /* resize vector, blob update etc. */
            MCO_THROW InvalidOperation("???");
        case MCO_E_EMPTYOPTIONAL:
             /* optional was not set */
            MCO_THROW RuntimeError("Optional was not set");
        case MCO_E_EMPTYBLOB:
             /* trying to read blob, which was not written */
            MCO_THROW RuntimeError("Empty BLOB");
        case MCO_E_CURSOR_INVALID:
            MCO_THROW RuntimeError("Cursor is not valid");
        case MCO_E_ILLEGAL_TYPE:
            MCO_THROW RuntimeError("Illegal type");
        case MCO_E_ILLEGAL_PARAM:
            MCO_THROW RuntimeError("Illegal parameter");
        case MCO_E_CURSOR_MISMATCH:
            MCO_THROW RuntimeError("Cursor type and object type are incompatible");
        case MCO_E_DELETED:
            MCO_THROW RuntimeError("Trying to update object, deleted in current transaction");
        case MCO_E_LONG_TRANSACTON:
            MCO_THROW RuntimeError("Transaction length is more then MCO_TRN_MAXLENGTH");
        case MCO_E_INSTANCE_DUPLICATE:
            MCO_THROW RuntimeError("Attempt to create db instance with duplicate name");
        case MCO_E_UPGRADE_FAILED:
            MCO_THROW UpgradeNotPossible();
        case MCO_E_NOINSTANCE:
            MCO_THROW RuntimeError("Database instance not found");
        case MCO_E_OPENED_SESSIONS:
            MCO_THROW RuntimeError("Failed to close db: have opened sessions");
        case MCO_E_PAGESIZE:
            MCO_THROW RuntimeError("Page size is not acceptable");
        case MCO_E_WRITE_STREAM:
            MCO_THROW RuntimeError("Write stream failure");
        case MCO_E_READ_STREAM:
            MCO_THROW RuntimeError("Read stream failure ");
        case MCO_E_LOAD_DICT:
            MCO_THROW RuntimeError("Database load: dictionary differs");
        case MCO_E_LOAD_DATA:
            MCO_THROW RuntimeError("Error data when loading db");
        case MCO_E_VERS_MISMATCH:
            MCO_THROW RuntimeError("Version mismatch");
        case MCO_E_VOLUNTARY_NOT_EXIST:
            MCO_THROW RuntimeError("Voluntary index is not created");
        case MCO_E_EXCLUSIVE_MODE:
            MCO_THROW RuntimeError("Database is in the exclusive mode; try later");
        case MCO_E_MAXEXTENDS:
            MCO_THROW RuntimeError("Maximum number of extends reached");
        case MCO_E_HIST_OBJECT:
            MCO_THROW RuntimeError("Operation is illegal for old version of the object");
        case MCO_E_SHM_ERROR:
            MCO_THROW RuntimeError("Failed to create/attach to shared memory");
        case MCO_E_NOTINIT:
            MCO_THROW RuntimeError("Runtime was not initialized");
        case MCO_E_SESLIMIT:
            MCO_THROW RuntimeError("Sessions number limit reached");
        case MCO_E_INSTANCES_LIMIT:
            MCO_THROW RuntimeError("Too many instances");
        case MCO_E_EVAL:
            MCO_THROW RuntimeError("Evaluation version limitation");
        case MCO_E_HA:
            MCO_THROW RuntimeError("HA base error code");
        case MCO_E_NW:
            MCO_THROW RuntimeError("HAFRAMEWORK base error code");
        case MCO_E_CONFLICT:
            MCO_THROW TransactionConflict();
        case MCO_E_DISK_SPACE_EXHAUSTED:
            MCO_THROW RuntimeError("Disk space exhausted");
        case MCO_E_SEQ_OUT_OF_ORDER:
            MCO_THROW RuntimeError("Sequence item is out of order");
        case MCO_E_SEQ_LENGTH_MISMATCH:
            MCO_THROW RuntimeError("Sequences have different length");
        case MCO_E_SEQ_BOUNDED:
            MCO_THROW RuntimeError("Sequence iterator was already bounded");
        case MCO_E_HA_NOWRITETXN:
            MCO_THROW RuntimeError(MCO_HA_NOT_MASTER_ERROR_MESSAGE);            
        case MCO_E_DDL_FIELD_NOT_FOUND:
            MCO_THROW RuntimeError("DDL field not found");
        case MCO_E_DDL_NOMEM:
            MCO_THROW RuntimeError("DDL no memory");
        case MCO_E_DDL_TOO_MUCH_CLASSES:
            MCO_THROW RuntimeError("DDL too much classes");
        case MCO_E_DDL_TOO_MUCH_INDEXES:
            MCO_THROW RuntimeError("DDL too much indexes");
        case MCO_E_DDL_TOO_MUCH_EVENTS:
            MCO_THROW RuntimeError("DDL too much events");
        case MCO_E_DDL_UNDEFINED_STRUCT:
            MCO_THROW RuntimeError("DDL undefined struct");
        case MCO_E_DDL_INVALID_TYPE:
            MCO_THROW RuntimeError("DDL invalid type");
        case MCO_E_DDL_INTERNAL_ERROR:
            MCO_THROW RuntimeError("DDL internal error");
       case MCO_E_DDL_MCOCOMP_INCOMPATIBILITY:
            MCO_THROW RuntimeError("Dictionary was generated without -nosort option");
    default:
        MCO_THROW RuntimeError(String::format("Unknown error code %d", rc)->cstr());
    }
}


Reference* McoDatabase::createReference(int64_t id)
{
    return new McoReference(NULL, NULL, id);
}

void McoDatabase::registerExternalTables(Table** tables,  size_t nTables)
{
    nExternalTables = nTables;
    externalTables = tables;
}

Iterator < Table > * McoDatabase::tables()
{
    if (_tables == NULL)
    {
        int i, j;
        int n = MCO_DICT(db)->n_class_codes;
		_tables = Vector < Table > ::create(n + nExternalTables);

        for (i = 0; i < n; i++)
        {
            _tables->items[i] = new McoTable(this);
        }
        for (i = 0; i < n; i++)
        {
            ((McoTable*)_tables->items[i])->load(i + 1, db);
        }
        for (j = 0; j < (int)nExternalTables; j++)
        {
            _tables->items[i+j] = externalTables[j];
        }
    }
    return _tables->iterator();
}

int McoDatabase::getSchemaVersion()
{
    if (schema_version != db->dict_version) { 
        _tables = NULL; // has to be realoaded
        schema_version = db->dict_version;
    }
    return schema_version;
}

Transaction* McoDatabase::beginTransaction(Transaction::Mode mode, int priority, Transaction::IsolationLevel level)
{
    return new McoTransaction(this, con, mode == Transaction::ReadOnly ? MCO_READ_ONLY
                              : mode == Transaction::Update ? MCO_UPDATE : MCO_READ_WRITE,
                              (MCO_TRANS_PRIORITY)priority, (MCO_TRANS_ISOLATION_LEVEL)level);
}


Table* McoDatabase::createTable(Transaction* trans, Table* table)
{
#if MCO_CFG_DDL_SUPPORT
    static mco_dict_type_t sql2uda[] = {
        (mco_dict_type_t)-1, // tpNull,
        MCO_DD_BOOL,  // tpBool,
        MCO_DD_INT1,  // tpInt1,
        MCO_DD_UINT1, // tpUInt1,
        MCO_DD_INT2,  // tpInt2,
        MCO_DD_UINT2, // tpUInt2,
        MCO_DD_INT4,  // tpInt4,
        MCO_DD_UINT4, // tpUInt4,
        MCO_DD_INT8,  // tpInt8,
        MCO_DD_UINT8, // tpUInt8,
        MCO_DD_FLOAT, // tpReal4,
        MCO_DD_DOUBLE,// tpReal8,
        MCO_DD_DATETIME,// tpDateTime,
        MCO_DD_INT8, // tpNumeric,
        MCO_DD_NCHAR_STRING, // tpUnicode,
        MCO_DD_STRING, // tpString,
        (mco_dict_type_t)-1, // tpRaw,
        MCO_DD_REF,   // tpReference,
        (mco_dict_type_t)-1, // tpArray,
        MCO_DD_STRUCT,// tpStruct,
        MCO_DD_BLOB,  // tpBlob,
        (mco_dict_type_t)-1, // tpDataSource,
        (mco_dict_type_t)-1, // tpList,
        (mco_dict_type_t)-1 // tpSequence
    };
    size_t mark = MemoryManager::allocator->mark();
    mco_db_h dbh = (mco_db_h)((McoTransaction*)trans)->transaction;
    mco_size_t dict_buf_size = 64*1024;
    mco_size_t dict_size;
    char* dict_buf = NULL;
    MCO_RET rc;
    if (dict_buf_size < db->dict_reserved_size) { 
        dict_buf_size = db->dict_reserved_size;
    }
    do {
        delete[] dict_buf;
        dict_buf_size <<= 1;
        dict_buf = new char[dict_buf_size];
    } while ((rc = mco_ddl_get_dictionary(dbh, dict_buf, dict_buf_size, &dict_size)) == MCO_E_DDL_NOMEM);

    checkStatus(rc);

    char* old_dict = new char[dict_size];
    memcpy(old_dict, dict_buf, dict_size);

    mco_ddl_dictionary_t* dict = (mco_ddl_dictionary_t*)dict_buf;

    mco_ddl_class_t* new_class = &dict->classes[dict->n_classes++];
    Iterator<Constraint>* constraint_iterator = table->constraints();
    mco_size_t n_indexes = 0;
    mco_size_t n_constraints = 0;
    mco_size_t n_nullable_fields = 0;
    mco_size_t n_index_keys = 0;
    while (constraint_iterator->hasNext()) {
        Constraint* constraint = constraint_iterator->next();
        if (constraint->type() == Constraint::MAY_BE_NULL) {
            n_nullable_fields += 1;
        }
        n_constraints += 1;
    }
    Field** constraint_fields = new Field*[n_constraints];
    Constraint::ConstraintType* constraint_types = new Constraint::ConstraintType[n_constraints];
    constraint_iterator = table->constraints();
    int n_list_indexes = 0;
    for (mco_size_t i = 0; i < n_constraints; i++) {
        Constraint* constraint = constraint_iterator->next();
        Iterator<Field>* cf = constraint->fields();
        int n_keys = 1;
        constraint_fields[i] = cf->next();
        while (cf->hasNext()) { // multifield constraint
            constraint_fields[i] = NULL;
            n_keys += 1;
            cf->next();
        }
        constraint_types[i] = constraint->type();
        switch (constraint_types[i]) {
          case Constraint::PRIMARY_KEY:
          case Constraint::UNIQUE:
          case Constraint::FOREIGN_KEY:
          case Constraint::USING_INDEX:
          case Constraint::USING_HASH_INDEX:
          case Constraint::USING_RTREE_INDEX:
            n_list_indexes += 1;
            // no break
          case Constraint::USING_TRIGRAM_INDEX:
            n_indexes += 1;
            n_index_keys += n_keys;
            if (constraint_fields[i] != NULL) {
                for (mco_size_t j = 0; j < i; j++) {
                    if (constraint_fields[j] == constraint_fields[i]) {
                        switch (constraint_types[j]) {
                          case Constraint::PRIMARY_KEY:
                          case Constraint::UNIQUE:
                          case Constraint::FOREIGN_KEY:
                          case Constraint::USING_INDEX:
                          case Constraint::USING_HASH_INDEX:
                          case Constraint::USING_RTREE_INDEX:
                          case Constraint::USING_TRIGRAM_INDEX:
                            break;
                          default:
                            continue;
                        }
                        n_indexes -= 1; // already have index for this field
                        n_index_keys -= n_keys;
                        break;
                    }
                }
            }
          default:
            break;
        }
    }
    new_class->structure = &dict->structs[dict->n_structs++];
    new_class->indices = new mco_ddl_index_t[n_indexes];
    new_class->n_indices = n_indexes;
    new_class->events = NULL;
    new_class->n_events = 0;
    new_class->flags = n_list_indexes == 0 ? MCO_CLASS_FLAGS_HAS_LIST_INDEX  : 0;
    if (mco_is_disk_database(dbh) && !table->temporary()) {
        new_class->flags |= MCO_CLASS_FLAGS_PERSISTENT;
    }
    new_class->structure->name = table->name()->cstr();
    int n_fields = table->nFields();
    new_class->structure->n_fields = n_fields;
    new_class->structure->fields = new mco_ddl_field_t[n_fields];
    Field** fields = new Field*[n_fields];
    Iterator<Field>* field_iterator = table->fields();
    for (int i = 0, j = 0; i < n_fields; i++, j++) {
        Field* field = field_iterator->next();
        mco_ddl_field_t* ddl_field = &new_class->structure->fields[j];
        int width = field->width();
        fields[i] = field;
        ddl_field->name = field->name()->cstr();
        ddl_field->precision = field->precision();
        ddl_field->type = sql2uda[field->type()];
        ddl_field->flags = 0;
        ddl_field->structure = NULL;
        ddl_field->init_value = NULL;

        switch (field->type()) {
          case tpReference:
            if (field->name()->compare("autoid") == 0) { 
                new_class->structure->n_fields -= 1;
                j -= 1;
                new_class->flags |= MCO_CLASS_FLAGS_HAS_AUTOID_INDEX;
                continue;
            }
            break;
          case tpArray:
            ddl_field->n_elements = field->fixedSize();
            ddl_field->type = sql2uda[field->elementType()];
            ddl_field->size = field->element() != NULL ? field->element()->fixedSize() : -1;
            if (field->elementType() == tpNumeric) { 
                if (width > 0) {
                    if (width <= 2) {
                        ddl_field->type = MCO_DD_INT1;
                    } else if (width <= 4) {
                        ddl_field->type = MCO_DD_INT2;
                    } else if (width <= 9) {
                        ddl_field->type = MCO_DD_INT4;
                    }
                }
                ddl_field->flags |= MCO_FIELD_FLAGS_NUMERIC;
            }
            if (ddl_field->size > 0) { 
                if (ddl_field->type == MCO_DD_STRING) { 
                    ddl_field->type = MCO_DD_CHAR;
                } else if (ddl_field->type == MCO_DD_NCHAR_STRING) { 
                    ddl_field->type = MCO_DD_NCHAR_CHAR;
                }
            }
            break;
          case tpString:
            ddl_field->n_elements = 1;
            ddl_field->size = field->fixedSize();
            if (ddl_field->size > 0) {
                ddl_field->type = MCO_DD_CHAR;
            }
            break;
          case tpUnicode:
            ddl_field->n_elements = 1;
            ddl_field->size = field->fixedSize();
            if (ddl_field->size > 0) {
                ddl_field->type = MCO_DD_NCHAR_CHAR;
            }
            break;
          case tpSequence:
          {
              mco_dict_type_t elemType = sql2uda[field->elementType()];
              ddl_field->size = 0;
              switch (elemType) {
                case MCO_DD_BOOL:
                  ddl_field->type = MCO_DD_SEQUENCE_UINT1;
                  break;
                case MCO_DD_DATE:
                case MCO_DD_TIME:
                  ddl_field->type = MCO_DD_SEQUENCE_UINT4;
                  break;
                case MCO_DD_DATETIME:
                  ddl_field->type = MCO_DD_SEQUENCE_UINT8;
                  break;
                case MCO_DD_CHAR:
                case MCO_DD_STRING:
                  ddl_field->type = MCO_DD_SEQUENCE_CHAR;
                  ddl_field->size = field->fixedSize();
                  if (ddl_field->size == 0) {
                      MCO_THROW RuntimeError("Sequence element type should have fixed size");
                  }
                  break;
                default:
                  if (elemType > MCO_DD_DOUBLE) {
                      MCO_THROW RuntimeError("Invalid sequence element type");
                  }
                  ddl_field->type = (mco_dict_type_t)((int)MCO_DD_SEQUENCE_UINT1 + (int)elemType);
              }
              ddl_field->n_elements = 1;
              if (field->order() == ASCENT_ORDER) {
                  ddl_field->flags |= MCO_FIELD_FLAGS_ASC_SEQUENCE;
              } else if (field->order() == DESCENT_ORDER) {
                  ddl_field->flags |= MCO_FIELD_FLAGS_DESC_SEQUENCE;
              }
              break;
          }
          case tpNumeric:
            ddl_field->flags |= MCO_FIELD_FLAGS_NUMERIC;
            if (width > 0) {
                if (width <= 2) {
                    ddl_field->type = MCO_DD_INT1;
                } else if (width <= 4) {
                    ddl_field->type = MCO_DD_INT2;
                } else if (width <= 9) {
                    ddl_field->type = MCO_DD_INT4;
                }
            }
            // no break
          default:
            ddl_field->n_elements = 1;
            ddl_field->size = 0;
        }
        if (n_nullable_fields != 0) {
            for (mco_size_t j = 0; j < n_constraints; j++) {
                if (constraint_fields[j] == field && constraint_types[j] == Constraint::MAY_BE_NULL) {
                    ddl_field->flags |= MCO_FIELD_FLAGS_NULLABLE;
                    break;
                }
            }
        }
    }
    mco_ddl_key_t* keys = new mco_ddl_key_t[n_index_keys];
    mco_ddl_field_t** field_path = new mco_ddl_field_t*[n_index_keys];
    mco_size_t index_no = 0;
    mco_size_t key_no = 0;
    constraint_iterator = table->constraints();
    for (mco_size_t i = 0; i < n_constraints; i++) {
        Constraint* constraint = constraint_iterator->next();
        int flags = 0;
        mco_ddl_index_type_t index_type = (mco_ddl_index_type_t)-1;
        if (constraint_fields[i] != NULL) {
            for (mco_size_t j = 0; j < n_constraints; j++) {
                if (constraint_fields[j] == constraint_fields[i]) {
                    switch (constraint_types[j]) {
                      case Constraint::PRIMARY_KEY:
                      case Constraint::UNIQUE:
                        flags = MCO_INDEX_FLAGS_UNIQUE;
                        // no break;
                      case Constraint::FOREIGN_KEY:
                      case Constraint::USING_INDEX:
                      case Constraint::USING_HASH_INDEX:
                      case Constraint::USING_RTREE_INDEX:
                      case Constraint::USING_TRIGRAM_INDEX:
                        if (j < i) { // already have index for this field
                            goto next_constraint;
                        }
                        if (index_type != MCO_INDEX_HASHTABLE && index_type != MCO_INDEX_RTREE && index_type != MCO_INDEX_TRIGRAM) {
                            switch (constraint_types[j]) {
                            case Constraint::USING_HASH_INDEX:
                                index_type = MCO_INDEX_HASHTABLE;
                                break;
                            case Constraint::USING_RTREE_INDEX:
                                index_type = MCO_INDEX_RTREE;
                                break;
                            case Constraint::USING_TRIGRAM_INDEX:
                                index_type = MCO_INDEX_TRIGRAM;
                                break;
                            default:
                                index_type = MCO_INDEX_BTREE;
                            }
                        }
                        break;
                      default:
                        if (i == j) { 
                            goto next_constraint;
                        }
                        break;
                    }
                }
            }
        } else { // multifields constraint
            switch (constraint_types[i]) {
              case Constraint::PRIMARY_KEY:
              case Constraint::UNIQUE:
                flags = MCO_INDEX_FLAGS_UNIQUE;
                // no break;
              case Constraint::FOREIGN_KEY:
                index_type = MCO_INDEX_BTREE;
                break;
              default:
                break;
            }
        }
        if (index_type != (mco_ddl_index_type_t)-1) {
            new_class->indices[index_no].name = constraint->name()->cstr();
            new_class->indices[index_no].n_keys = 0;
            new_class->indices[index_no].index_type = index_type;
            new_class->indices[index_no].keys = &keys[key_no];
            new_class->indices[index_no].flags = flags;
            new_class->indices[index_no].init_size = HASH_TABLE_INIT_SIZE;

            Iterator<Field>* cf = constraint->fields();
            while (cf->hasNext()) {
                Field* f = cf->next();
                new_class->indices[index_no].n_keys += 1;
                keys[key_no].flags = 0;
                keys[key_no].field_path_len = 1;
                keys[key_no].field_path = NULL;
                for (int j = 0; j < n_fields; j++) {
                    if (fields[j] == f) {
                        field_path[key_no] = &new_class->structure->fields[j];
                        keys[key_no].field_path = &field_path[key_no];
                        break;
                    }
                }
                assert(keys[key_no].field_path != NULL);
                key_no += 1;
            }
            index_no += 1;
        }
      next_constraint:;
    }
    assert(index_no == n_indexes);
    assert(key_no == n_index_keys);
    MemoryManager::allocator->reset(mark);

    McoTable* mcoTable = NULL;
    rc = mco_db_schema_evolution(dbh, dict, 0);
    if (rc != MCO_S_OK) { 
        mco_db_schema_evolution(dbh, (mco_ddl_dictionary_t*)old_dict, 0);
    } else {  
        mcoTable = new McoTable(this);
        int nTables = _tables->length;
        int newTableId = nTables - nExternalTables;
        mcoTable->load(newTableId+1, db);
        Vector<Table>* newTables = Vector<Table>::create(nTables + 1);
        mco_memcpy(newTables->items, _tables->items, newTableId*sizeof(Table*));
        newTables->items[newTableId] = mcoTable;
        mco_memcpy(newTables->items + newTableId+1, _tables->items + newTableId, nExternalTables*sizeof(Table*));
        _tables = newTables;
    }
    schema_version = db->dict_version;
    delete new_class->structure->fields;
    delete new_class->indices;
    delete[] constraint_fields;
    delete[] constraint_types;
    delete[] field_path;
    delete[] old_dict;
    delete[] dict_buf;
    delete[] fields;
    delete[] keys;

    checkStatus(rc);

    return mcoTable;
#else
    MCO_THROW InvalidOperation("McoDatabase::createTable");
#endif
}

Index* McoDatabase::createIndex(Transaction* trans, Index* index)
{
#if MCO_CFG_DDL_SUPPORT
    size_t mark = MemoryManager::allocator->mark();
    mco_db_h dbh = (mco_db_h)((McoTransaction*)trans)->transaction;
    mco_size_t dict_buf_size = 64*1024;
    mco_size_t dict_size;
    char* dict_buf = NULL;
    MCO_RET rc;
    if (dict_buf_size < db->dict_reserved_size) { 
        dict_buf_size = db->dict_reserved_size;
    }
    do {
        delete[] dict_buf;
        dict_buf_size <<= 1;
        dict_buf = new char[dict_buf_size];
    } while ((rc = mco_ddl_get_dictionary(dbh, dict_buf, dict_buf_size, &dict_size)) == MCO_E_DDL_NOMEM);

    checkStatus(rc);
    mco_ddl_dictionary_t* dict = (mco_ddl_dictionary_t*)dict_buf;
    Table* table = index->table();
    mco_ddl_class_t* alter_class = NULL;
    int class_code;
    for (class_code = 0; class_code < dict->n_classes; class_code++) {
        if (table->name()->compare(dict->classes[class_code].structure->name) == 0) {
            alter_class = &dict->classes[class_code];
            break;
        }
    }
    assert(alter_class != NULL);
    int n_indexes = alter_class->n_indices;
    mco_ddl_index_t* new_indexes = new mco_ddl_index_t[n_indexes + 1];
    memcpy(new_indexes, alter_class->indices, n_indexes*sizeof(mco_ddl_index_t));

    mco_ddl_index_t* new_index = &new_indexes[n_indexes];
    new_index->name = index->name()->cstr();
    int n_keys = index->nKeys();
    new_index->n_keys = n_keys;
    new_index->keys = new mco_ddl_key_t[n_keys];
    new_index->flags = index->isUnique() ? MCO_INDEX_FLAGS_UNIQUE : 0;
    new_index->index_type = index->isSpatial() ? MCO_INDEX_RTREE
        : index->isTrigram() ? MCO_INDEX_TRIGRAM : index->isOrdered() ? MCO_INDEX_BTREE : MCO_INDEX_HASHTABLE;
    new_index->init_size = HASH_TABLE_INIT_SIZE;
    Iterator<Key>* keys = index->keys();
    mco_ddl_field_t** field_path = new mco_ddl_field_t*[n_keys];

    for (int i = 0; i < n_keys; i++) {
        Key* key = keys->next();
        Field* f = key->field();
        new_index->keys[i].flags = key->order() == DESCENT_ORDER ? MCO_KEY_FLAGS_DESCENDING : 0;
        new_index->keys[i].field_path_len = 1;
        new_index->keys[i].field_path = NULL;
        for (int j = 0; j < alter_class->structure->n_fields; j++) {
            if (f->name()->compare(alter_class->structure->fields[j].name) == 0) {
                new_index->keys[i].field_path = &field_path[i];
                field_path[i] = &alter_class->structure->fields[j];
                break;
            }
        }
        assert(new_index->keys[i].field_path != NULL);
    }
    alter_class->n_indices += 1;
    alter_class->indices = new_indexes;
    MemoryManager::allocator->reset(mark);

    checkStatus(mco_db_schema_evolution(dbh, dict, class_code+1));
    schema_version = db->dict_version;

    McoTable* alter_table = new McoTable(this);
    alter_table->load(class_code+1, db);
    _tables->items[class_code] = alter_table;
    while (++class_code <  dict->n_classes) {
        McoTable* t = (McoTable*)_tables->items[class_code];
        if (t->indexCode != -1) {
            t->indexCode += 1;
        }
        for (int i = 0; i < t->_indices->length; i++) {
            t->_indices->items[i]->indexCode += 1;
        }
    }

    delete[] field_path;
    delete[] new_index->keys;
    delete[] new_indexes;
    delete[] dict_buf;

    return alter_table->_indices->items[n_indexes];
#else
    MCO_THROW InvalidOperation("McoDatabase::createIndex");
#endif
}

void McoDatabase::close()
{
    for (size_t i = 0; i < nPooledConnections; i++) {
        checkStatus(mco_db_disconnect(connectionPool[i]));
    }
    checkStatus(mco_db_disconnect(con));
}

static void mco_sql_error_handler(MCO_RET rc) 
{
    McoDatabase::checkStatus(rc);
} 
    
McoDatabase::McoDatabase()
{
#ifdef MCO_CFG_WRAPPER_SEQUENCE_SUPPORT
    mco_seq_redefine_allocator(mco_seq_sql_malloc, mco_seq_sql_free);
    mco_seq_enable_check_bindings(MCO_NO);
#endif
    mco_error_set_handler(mco_sql_error_handler);
}

void McoDatabase::createConnectionPool()
{
#ifdef MCO_CFG_WRAPPER_SEQUENCE_SUPPORT
    if (mco_int_vt_caps() != MCO_VTCAPS_INMEM_ONLY) {
        size_t nThreads = mco_seq_thread_pool.nThreads();
        connectionPool = (mco_db_h*)MemoryManager::allocator->allocate(nThreads*sizeof(mco_db_h));
        for (size_t i = 0; i < nThreads; i++) {
            checkStatus(mco_db_connect(db->name, &connectionPool[i]));
        }
        nPooledConnections = nThreads;
    }
#endif
}

void McoDatabase::destroyConnectionPool()
{
    for (size_t i = 0; i < nPooledConnections; i++) {
        checkStatus(mco_db_disconnect(connectionPool[i]));
    }
    nPooledConnections = 0;
}


void McoDatabase::connect(McoDatabase const &d, mco_db_h con0)
{
    mco_db_connection_h con = (mco_db_connection_h)con0;
    this->con = con0;
    db = con->db;
    schema_version = d.schema_version;
    _tables = d._tables;
    nExternalTables = d.nExternalTables;
    externalTables = d.externalTables;
    nPooledConnections = d.nPooledConnections;
    connectionPool = d.connectionPool;
}

void McoDatabase::open(mco_db_h con0)
{
    mco_db_connection_h con = (mco_db_connection_h)con0;
#if defined(MCO_CFG_TRLIMIT)
    mco_runtime_info_t rt;
    mco_get_runtime_info(&rt);
    if (!rt.mco_evaluation_version) {
        MCO_THROW RuntimeError("Evaluation version of xSQL can work only with evaluation version of eXtremeDB core");
    }
#endif
    this->con = con0;
    db = con->db;
    schema_version = db->dict_version;
    _tables = NULL;
    nExternalTables = 0;
    externalTables = NULL;
    nPooledConnections = 0;
    connectionPool = NULL;
}

//
// Transaction
//

bool McoTransaction::commit(int phases)
{
    MCO_RET rc = MCO_S_OK;
    size_t nExternalTables = db->nExternalTables;
    if (nExternalTables  != 0) {
        size_t i;
        if (phases & 1) {
            rc = mco_trans_commit_phase1(transaction);
            if (rc != MCO_S_OK) {
                for (i = 0; i < nExternalTables; i++) {
                    db->externalTables[i]->rollback();
                }
                return false;
            }
            for (i = 0; i < nExternalTables; i++) {
                if (!db->externalTables[i]->commit(1)) {
                    McoDatabase::checkStatus(mco_trans_rollback(transaction));
                    for (i = 0; i < nExternalTables; i++) {
                        db->externalTables[i]->rollback();
                    }
                    return false;
                }
            }
        }
        if (phases & 2) {
            McoDatabase::checkStatus(mco_trans_commit_phase2(transaction));
            for (i = 0; i < nExternalTables; i++) {
                if (!db->externalTables[i]->commit(2)) {
                    MCO_THROW RuntimeError("Second phase of commit is failed");
                }
            }
        }
    } else {
        if ((phases & (1+2)) == (1+2)) {
            rc = mco_trans_commit(transaction);
        } else if (phases & 1) {
            rc = mco_trans_commit_phase1(transaction);
        } else if (phases & 2) {
            rc = mco_trans_commit_phase2(transaction);
        }
        if (rc == MCO_E_CONFLICT/* || rc == MCO_S_DUPLICATE*/) {
            return false;
        }
        McoDatabase::checkStatus(rc);
    }
    return true;
}

void McoTransaction::checkpoint()
{
    McoDatabase::checkStatus(mco_trans_checkpoint(transaction));
}


void McoTransaction::rollback()
{
    MCO_RET rc = mco_trans_rollback(transaction);
    if (rc != MCO_E_TRANS_NOT_ACTIVE) { 
        McoDatabase::checkStatus(rc);
    }
}

void McoTransaction::start()
{
    McoDatabase::checkStatus(mco_trans_start_ex(con, mode, priority, level, &transaction));
}

mco_trans_h McoTransaction::handle()
{
    return transaction;
}

void McoTransaction::restart()
{
    // fprintf(stderr, "!!!Restart transaction\n");
    McoDatabase::checkStatus(mco_trans_commit(transaction));
    start();
}

bool McoTransaction::upgrade()
{
    if (mode == MCO_READ_ONLY)
    {
        MCO_RET rc = mco_trans_upgrade(transaction);
        if (rc == MCO_E_UPGRADE_FAILED) {
            return false;
        }
        McoDatabase::checkStatus(rc);
        mode = MCO_READ_WRITE;
    }
    return true;
}

McoTransaction::McoTransaction(McoDatabase* db, mco_db_h con, MCO_TRANS_TYPE mode, MCO_TRANS_PRIORITY priority, MCO_TRANS_ISOLATION_LEVEL level)
{
    this->db = db;
    this->con = con;
    this->mode = mode;
    this->priority = priority;
    this->level = level;
    transaction = 0;
    start();
}

//
// Table
//

McoTable::McoTable(McoDatabase* db)
{
    this->db = db;
}

void McoTable::load(int cid, mco_database_h db)
{
    int i;
    if (MCO_DICT(db)->str_class_names == NULL)
    {
        MCO_THROW RuntimeError("Dictionary doesn't contain symbolic information");
    }
    mco_dict_class_info_t* cinfo = MCO_DICT_CLASS_INFO(db, cid);
    _name = String::create(MCO_DICT_CLASS_NAME(db, cid));
    isCompact = (cinfo->flags & MCO_DB_TYPINFO_COMPACT) != 0;
    flags = cinfo->flags;
    classCode = (uint2)cid;
    #ifdef MCO_CFG_EVENTS_SUPPORTED
        event_id_new = MCO_DB_EVENT_ID_NONE;
        event_id_delete = MCO_DB_EVENT_ID_NONE;
        event_id_delete_all = MCO_DB_EVENT_ID_NONE;
        for (i = cinfo->first_event_num; i <= cinfo->last_event_num; i++)
        {
            uint2 flags = MCO_DICT_EVENT_MAP(db, i)->flags;
            if (flags & MCO_DB_EVENT_T_NEW) {
                event_id_new = i;
            } else if (flags & MCO_DB_EVENT_T_DELETE) {
                event_id_delete = i;
            } else if (flags & MCO_DB_EVENT_T_DELETE_ALL) {
                event_id_delete_all = i;
            }
        }
    #endif
    bool hasListCursor = cinfo->list_index_num >= 0;
    autoId = cinfo->autoid_index_num;
    initSize = cinfo->init_size;
    isPlain = cinfo->fixedsize + db->additional_object_field_size <= db->pagesize;
    alignment = MCO_DICT_CLASS_STRUCT(db, cid)->u_align; // we mee alignment only for fized size structures and for them u_align == c_align
    indexCode =  - 1;
    _fields = McoField::structComponents(this, db, MCO_DICT_CLASS_STRUCT(db, cid), NULL);
    int nIndices = 0;
    for (i = cinfo->first_index_num; i <= cinfo->last_index_num; i++)
    {
        mco_dict_index_t* index = MCO_DICT_INDEX_MAP(db, i);
        if ((index->flags & (MCO_DB_INDF_VSTRUCT_BASED | MCO_DB_INDF_ASTRUCT_BASED)) == 0 && index->numof_fields > 0)
        {
            nIndices += 1;
        }
    }
    _indices = Vector < McoIndex > ::create(nIndices);
    int listIndex =  - 1;
    int j = 0;
    for (i = cinfo->first_index_num; i <= cinfo->last_index_num; i++)
    {
        mco_dict_index_t* index = MCO_DICT_INDEX_MAP(db, i);
        if ((index->flags & (MCO_DB_INDF_VSTRUCT_BASED | MCO_DB_INDF_ASTRUCT_BASED)) == 0 && index->numof_fields > 0)
        {
            McoIndex* idx = new McoIndex(this, i, db);
            _indices->items[j++] = idx;
            //            if (idx->type == BTREE && listIndex < 0) {
            if (listIndex < 0 && !(index->flags & (MCO_DB_INDF_VSTRUCT_BASED | MCO_DB_INDF_ASTRUCT_BASED | MCO_DB_INDF_NULLABLE | MCO_DB_INDF_TRIGRAM)))
            {
                listIndex = i;
            }
        }
    }
    if (!hasListCursor)
    {
        if (listIndex < 0)
        {
            MCO_THROW RuntimeError("No class iterator is provided");
        }
        indexCode = (int2)listIndex;
    }
}


void McoTable::extract(Record* src, void* dst, size_t appSize, bool nullIndicators[], ExtractMode mode)
{
    if (isPlain && nullIndicators == NULL && mode == emCopyFixedSizeStrings) {
        McoRecord* rec = (McoRecord*)src;
        mco_size_t dbSize = isCompact ? rec->extractCompact((char*)dst) : rec->extract((char*)dst);
        if (MCO_ALIGN(dbSize, alignment) != appSize) {
            MCO_THROW RuntimeError(String::format(
                                   "Size of extracted record %d doesn't match with size of destination structure %d", 
                                   (int)dbSize, (int)appSize)->cstr());
        }
    } else { 
        DataSource::extract(src, dst, appSize, nullIndicators, mode);
    }
}
        
McoField* McoTable::findFieldByOffset(int offset)
{
    if (isCompact)
    {
        return findFieldByOffsetCompact(_fields, offset);
    }
    else
    {
        return findFieldByOffsetNormal(_fields, offset);
    }
}

McoField* McoTable::findFieldByOffsetCompact(Vector < McoField > * fields, int offset)
{
    for (int i = 0; i < fields->length; i++)
    {
        McoField* fd = fields->items[i];
        if (fd->sqlType == tpStruct)
        {
            if (fd->layout.c_offset <= offset && fd->layout.c_offset + fd->layout.c_size > offset)
            {
                return findFieldByOffsetCompact(fd->_components, offset - fd->layout.c_offset);
            }
        }
        else if (fd->layout.c_offset == offset)
        {
            return fd;
        }
    }
    return NULL;
}

McoField* McoTable::findFieldByOffsetNormal(Vector < McoField > * fields, int offset)
{
    for (int i = 0; i < fields->length; i++)
    {
        McoField* fd = fields->items[i];
        if (fd->sqlType == tpStruct)
        {
            if (fd->layout.u_offset <= (unsigned)offset && fd->layout.u_offset + fd->layout.u_size > (unsigned)offset)
            {
                return findFieldByOffsetNormal(fd->_components, offset - fd->layout.u_offset);
            }
        }
        else if (fd->layout.u_offset == (unsigned)offset)
        {
            return fd;
        }
    }
    return NULL;
}


int McoTable::nFields()
{
    return _fields->length;
}

Iterator < Field > * McoTable::fields()
{
    return (Iterator < Field > *)_fields->iterator();
}

Cursor* McoTable::records(Transaction* trans)
{
    return (Cursor*)new McoCursor(this, trans, classCode, indexCode);
}

bool McoTable::isNumberOfRecordsKnown()
{
    return !db->db->concurrent_write_transactions;
}

size_t McoTable::nRecords(Transaction* trans)
{
    mco_class_stat_t st;
    McoDatabase::checkStatus(mco_class_stat_get(((McoTransaction*)trans)->transaction, classCode, &st));
    return st.objects_num;
}

int McoTable::compareRID(Record* r1, Record* r2)
{
    if (autoId < 0)
    {
        MCO_THROW InvalidOperation("McoTable::compareRID");
    }
    int64_t id1, id2;
    McoDatabase::checkStatus(mco_w_b8_get2(&((McoRecord*)r1)->obj, autoidOffsetU, autoidOffsetC, autoidFieldNo, &id1));
    McoDatabase::checkStatus(mco_w_b8_get2(&((McoRecord*)r2)->obj, autoidOffsetU, autoidOffsetC, autoidFieldNo, &id2));
    return id1 < id2 ?  - 1: id1 == id2 ? 0 : 1;
}

Reference* McoTable::getRID(Record* rec)
{
    int64_t autoid;
    McoDatabase::checkStatus(mco_w_b8_get2(&((McoRecord*)rec)->obj, autoidOffsetU, autoidOffsetC, autoidFieldNo, &autoid));
    return new McoReference(((McoRecord*)rec)->trans, this, autoid);
}

bool McoTable::isRIDAvailable()
{
    return autoId >= 0;
}

String* McoTable::name()
{
    return _name;
}


bool McoTable::temporary()
{
    return (flags & MCO_DB_TYPINFO_PERSISTENT) == 0;
}
    
Iterator < Index > * McoTable::indices()
{
    return (Iterator < Index > *)_indices->iterator();
}

void McoTable::drop(Transaction* trans)
{
    deleteAllRecords(trans);
    // MCO_THROW InvalidOperation("McoTable::drop");
}

void McoTable::checkpointRecord(Transaction* , Record* rec)
{
    McoDatabase::checkStatus(mco_w_obj_checkpoint(&((McoRecord*)rec)->obj));
}

void McoTable::updateRecord(Transaction* , Record*)
{
}

void McoTable::deleteRecord(Transaction* trans, Record* rec)
{
    #ifdef MCO_CFG_EVENTS_SUPPORTED
        if (event_id_delete == MCO_DB_EVENT_ID_NONE)
        {
            McoDatabase::checkStatus(mco_w_obj_delete(&((McoRecord*)rec)->obj));
        }
        else
        {
            McoDatabase::checkStatus(mco_w_obj_delete_ev(&((McoRecord*)rec)->obj, event_id_delete));
        }
    #else
        McoDatabase::checkStatus(mco_w_obj_delete(&((McoRecord*)rec)->obj));
    #endif
}

void McoTable::updateStatistic(Transaction* trans, Record* statRec)
{
    mco_class_stat_t stat;
    McoDatabase::checkStatus(mco_class_stat_get(((McoTransaction*)trans)->transaction, classCode, &stat));
    ((IntValue*)statRec->get(1))->val = stat.objects_num;
    ((IntValue*)statRec->get(2))->val = stat.versions_num;
    ((IntValue*)statRec->get(3))->val = stat.core_pages;
    ((IntValue*)statRec->get(4))->val = stat.blob_pages;
    ((IntValue*)statRec->get(5))->val = stat.core_space;
}

void McoTable::deleteAllRecords(Transaction* trans)
{
    #if defined(MCO_CFG_EVENTS_SUPPORTED) && defined(USE_DELETEALL)
        if (event_id_delete_all == MCO_DB_EVENT_ID_NONE)
        {
            McoDatabase::checkStatus(mco_w_obj_delete_all(((McoTransaction*)trans)->transaction, classCode));
        }
        else
        {
            McoDatabase::checkStatus(mco_w_obj_delete_all_ev(((McoTransaction*)trans)->transaction, classCode,
                                     event_id_delete));
        }
    #else
        McoCursor cursor(this, trans, classCode, indexCode);
        while (cursor.hasNext())
        {
            mco_objhandle_t obj;
            cursor.moveNext(&obj);
            MCO_RET ret = mco_w_obj_delete(&obj);
            if (ret == MCO_E_LONG_TRANSACTON)
            {
                ((McoTransaction*)trans)->restart();
            }
            else
            {
                McoDatabase::checkStatus(ret);
            }
        }
    #endif
}

Record* McoTable::createRecord(Transaction* trans)
{
    mco_objhandle_t obj;
    #ifdef MCO_CFG_EVENTS_SUPPORTED
        if (event_id_new == MCO_DB_EVENT_ID_NONE)
        {
            McoDatabase::checkStatus(mco_w_new_obj_noid(((McoTransaction*)trans)->transaction, initSize, classCode,
                                     &obj));
        }
        else
        {
            McoDatabase::checkStatus(mco_w_new_obj_noid_ev(((McoTransaction*)trans)->transaction, initSize, classCode,
                                     &obj, event_id_new));
        }
    #else
        McoDatabase::checkStatus(mco_w_new_obj_noid(((McoTransaction*)trans)->transaction, initSize, classCode, &obj));
    #endif
    return new McoRecord(_fields, trans, &obj);
}

Iterator < Constraint > * McoTable::constraints()
{
    return new ArrayIterator < Constraint > ((Constraint*)0, 0);
}

//
// Cursor
//


void McoCursor::open()
{
    MCO_RET ret;
    while (true)
    {
        ret = indexCode < 0 ? mco_w_list_cursor(trans->transaction, classCode, &cursor): mco_w_index_cursor(trans->transaction, (uint2)indexCode, &cursor);
        if (ret == MCO_E_VOLUNTARY_NOT_EXIST)
        {
            if (!trans->upgrade()) {
                MCO_THROW UpgradeNotPossible();
            }
            McoDatabase::checkStatus(mco_w_voluntary_create(trans->transaction, (uint2)indexCode));
            continue;
        } else if (ret == MCO_S_OK) {
            ret = mco_cursor_first(trans->transaction, &cursor);
        }
        break;
    }
    if (ret == MCO_S_CURSOR_EMPTY || ret == MCO_S_CURSOR_END || ret == MCO_S_NOTFOUND)
    {
        hasCurrent = false;
    }
    else
    {
        McoDatabase::checkStatus(ret);
        hasCurrent = true;
    }
}

McoCursor::McoCursor(McoTable* table, Transaction* trans, uint2 classCode, int2 indexCode)
{
    this->table = table;
    this->trans = (McoTransaction*)trans;
    this->classCode = classCode;
    this->indexCode = indexCode;
    open();
}

bool McoCursor::hasNext()
{
    return hasCurrent;
}


Record* McoCursor::next()
{
    if (!hasCurrent)
    {
        MCO_THROW NoMoreElements();
    }
    mco_objhandle_t obj;
    moveNext(&obj);
    return new McoRecord(table->_fields, trans, &obj);
}

void McoCursor::release()
{
    McoDatabase::checkStatus(mco_cursor_close(trans->transaction, &cursor));
}

void McoCursor::moveNext(mco_objhandle_h hnd)
{
    MCO_RET ret;
    while (true)
    {
        ret = mco_w_obj_from_cursor(trans->transaction, &cursor, classCode, hnd);
        if (ret == MCO_E_CURSOR_INVALID)
        {
            // fprintf(stderr, "!!!Reinitialize cursor\n");
            open();
        }
        else
        {
            McoDatabase::checkStatus(ret);
            break;
        }
    }
    ret = mco_cursor_next(trans->transaction, &cursor);
    if (ret == MCO_S_CURSOR_END)
    {
        hasCurrent = false;
    }
    else
    {
        McoDatabase::checkStatus(ret);
        hasCurrent = true;
    }
}

//
// Field
//

#ifdef MCO_DB_FT_BOOL
class McoBoolField: public McoField { 
  public:
    virtual McoSql::Value* get(McoSql::Struct* rec) { 
        uint1 value;
        McoDatabase::checkStatus(mco_w_bit_get2(&((McoRecord*)rec)->obj, layout.u_offset, layout.c_offset, fieldNo, &value));
        return BoolValue::create(value != 0);
    }         
};  
#endif  
class McoUInt1Field: public McoField { 
  public:
    virtual McoSql::Value* get(McoSql::Struct* rec) { 
        uint1 value;
        McoDatabase::checkStatus(mco_w_b1_get2(&((McoRecord*)rec)->obj, layout.u_offset, layout.c_offset, fieldNo, &value));
        return new (((McoRecord*)rec)->allocator) IntValue(value);
    }         
};    
class McoUInt2Field: public McoField { 
  public:
    virtual McoSql::Value* get(McoSql::Struct* rec) { 
        uint2 value;
        McoDatabase::checkStatus(mco_w_b2_get2(&((McoRecord*)rec)->obj, layout.u_offset, layout.c_offset, fieldNo, &value));
        return new (((McoRecord*)rec)->allocator) IntValue(value);
    }         
};    
class McoUInt4Field: public McoField { 
  public:
    virtual McoSql::Value* get(McoSql::Struct* rec) { 
        uint4 value;
        McoDatabase::checkStatus(mco_w_b4_get2(&((McoRecord*)rec)->obj, layout.u_offset, layout.c_offset, fieldNo, &value));
        return new (((McoRecord*)rec)->allocator) IntValue(value);
    }         
};    
class McoInt1Field: public McoField { 
  public:
    virtual McoSql::Value* get(McoSql::Struct* rec) { 
        int1 value;
        McoDatabase::checkStatus(mco_w_b1_get2(&((McoRecord*)rec)->obj, layout.u_offset, layout.c_offset, fieldNo, &value));
        return new (((McoRecord*)rec)->allocator) IntValue(value);
    }         
};    
class McoInt2Field: public McoField { 
  public:
    virtual McoSql::Value* get(McoSql::Struct* rec) { 
        int2 value;
        McoDatabase::checkStatus(mco_w_b2_get2(&((McoRecord*)rec)->obj, layout.u_offset, layout.c_offset, fieldNo, &value));
        return new (((McoRecord*)rec)->allocator) IntValue(value);
    }         
};    
class McoInt4Field: public McoField { 
  public:
    virtual McoSql::Value* get(McoSql::Struct* rec) { 
        int4 value;
        McoDatabase::checkStatus(mco_w_b4_get2(&((McoRecord*)rec)->obj, layout.u_offset, layout.c_offset, fieldNo, &value));
        return new (((McoRecord*)rec)->allocator) IntValue(value);
    }         
};    
class McoInt8Field: public McoField { 
  public:
    virtual McoSql::Value* get(McoSql::Struct* rec) { 
        int64_t value;
        McoDatabase::checkStatus(mco_w_b8_get2(&((McoRecord*)rec)->obj, layout.u_offset, layout.c_offset, fieldNo, &value));
        return new (((McoRecord*)rec)->allocator) IntValue(value);
    }         
};    
#ifndef NO_FLOATING_POINT
class McoReal4Field: public McoField { 
  public:
    virtual McoSql::Value* get(McoSql::Struct* rec) { 
        float value;
        McoDatabase::checkStatus(mco_w_b4_get2(&((McoRecord*)rec)->obj, layout.u_offset, layout.c_offset, fieldNo, &value));
        return new (((McoRecord*)rec)->allocator) RealValue(value);
    }         
};    
class McoReal8Field: public McoField { 
  public:
    virtual McoSql::Value* get(McoSql::Struct* rec) { 
        double value;
        McoDatabase::checkStatus(mco_w_b8_get2(&((McoRecord*)rec)->obj, layout.u_offset, layout.c_offset, fieldNo, &value));
        return new (((McoRecord*)rec)->allocator) RealValue(value);
    }         
};    
#endif
class McoVarCharField: public McoField { 
  public:
    virtual McoSql::Value* get(McoSql::Struct* rec) { 
        uint2 len;
        mco_objhandle_t& obj = ((McoRecord*)rec)->obj;
        McoDatabase::checkStatus(mco_w_string_len2(&obj, layout.u_offset, layout.c_offset, fieldNo, &len));
        String* str = String::create(len, ((McoRecord*)rec)->allocator);
        McoDatabase::checkStatus(mco_w_string_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, str->body(), len + 1, &len));
        return str;
    }
};    
class McoCharField: public McoField { 
  public:
    virtual McoSql::Value* get(McoSql::Struct* rec) { 
        String* str = FixedString::create(fieldSize, ((McoRecord*)rec)->allocator);
        McoDatabase::checkStatus(mco_w_chars_get2(&((McoRecord*)rec)->obj, layout.u_offset, layout.c_offset, fieldNo, str->body(), fieldSize + 1, fieldSize));
        return str;
    }
};    
#ifdef UNICODE_SUPPORT
class McoVarNCharField: public McoField { 
  public:
    virtual McoSql::Value* get(McoSql::Struct* rec) { 
        uint2 len;
        mco_objhandle_t& obj = ((McoRecord*)rec)->obj;
        McoDatabase::checkStatus(mco_w_nstring_len2(&obj, layout.u_offset, layout.c_offset, fieldNo, &len));
        UnicodeString* str = UnicodeString::create(len, ((McoRecord*)rec)->allocator);
        McoDatabase::checkStatus(mco_w_nstring_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, (nchar_t*)str ->body(), len + 1, &len));
        convertNcharToWchar(str);
        return str;
    }
};    
class McoNCharField: public McoField { 
  public:
    virtual McoSql::Value* get(McoSql::Struct* rec) { 
        UnicodeFixedString* str = UnicodeFixedString::create(fieldSize, ((McoRecord*)rec)->allocator);
        McoDatabase::checkStatus(mco_w_nchars_get2(&((McoRecord*)rec)->obj, layout.u_offset, layout.c_offset, fieldNo, (nchar_t*)str ->body(), fieldSize + 1, fieldSize));
        convertNcharToWchar(str);
        return str;
    }
};    
#endif

inline McoField* newMcoField(mco_dict_field_t* field, McoField* scope) 
{
    if (!(field->flags & (MCO_DICT_FLDF_ARRAY|MCO_DICT_FLDF_VECTOR|MCO_DICT_FLDF_NUMERIC|MCO_DICT_FLDF_NULLABLE)) && scope == NULL) { 
        switch (field->field_el_type) { 
        #ifdef MCO_DB_FT_BOOL
        case MCO_DB_FT_BOOL:
            return new McoBoolField();
        #endif
        case MCO_DB_FT_UINT1:
            return new McoUInt1Field();
        case MCO_DB_FT_UINT2:
            return new McoUInt2Field();
        case MCO_DB_FT_UINT4:
            return new McoUInt4Field();
        case MCO_DB_FT_INT8:
        case MCO_DB_FT_UINT8:
            return new McoInt8Field();
        case MCO_DB_FT_INT1:
            return new McoInt1Field();
        case MCO_DB_FT_INT2:
            return new McoInt2Field();
        case MCO_DB_FT_INT4:
            return new McoInt4Field();
        #ifdef UNICODE_SUPPORT
        case MCO_DB_FT_UNICODE_CHARS:
            return new McoNCharField();
        case MCO_DB_FT_UNICODE_STRING:
            return new McoVarNCharField();
        #endif
        case MCO_DB_FT_CHARS:
            return new McoCharField();
        case MCO_DB_FT_STRING:
            return new McoVarCharField();
        #ifndef NO_FLOATING_POINT
        case MCO_DB_FT_FLOAT:
            return new McoReal4Field();
        case MCO_DB_FT_DOUBLE:
            return new McoReal8Field();
        #endif
        default:;
            // no break
        }
    }
    return new McoField();
}

Vector < McoField > * McoField::structComponents(McoTable* table, mco_database_h db, mco_dict_struct_t* sp, McoField* scope)
{
    int n_fields = sp->n_fields;
    int i, n_nullable_fields = 0;
    for (i = 0; i < n_fields; i++)
    {
        mco_dict_field_t* field = MCO_DICT_CLASS_STRUCT_FIELD(db, sp, i);
        if (field->flags & MCO_DICT_FLDF_NULLABLE) {
            n_nullable_fields += 1;
            table->isPlain = false;
        }
    }
    
    Vector < McoField > * fields = Vector < McoField > ::create(n_fields - n_nullable_fields);
    for (i = 0; i < n_fields; i++)
    {
        mco_dict_field_t* field = MCO_DICT_CLASS_STRUCT_FIELD(db, sp, i);
        if (!(field->flags & MCO_DICT_FLDF_NULL_INDICATOR)) {
            mco_dict_field_t* indicator = NULL;
            if (field->flags & MCO_DICT_FLDF_NULLABLE) {
                indicator = MCO_DICT_CLASS_STRUCT_FIELD(db, sp, field->indicator);
            }
            fields->items[field->order_no] = (newMcoField(field, scope))->convert(table, db, field, indicator, scope);
        }
    }
    return fields;
}

McoField* McoField::convert(McoTable* table, mco_database_h db, mco_dict_field_t* field, mco_dict_field_t* indicator, McoField* scope)
{
    _table = table;
    _scope = scope;
    _name = String::create(MCO_DICT_NAME(db, field->name));
    mcoType = field->field_el_type;
    layout = field->layout;
    flags = field->flags;
    structNum = field->struct_num;
    fieldSize = field->field_size;
    _precision = field->precision;
    seq_order = field->seq_order;
    seq_elem_size = field->seq_elem_size;
    #ifdef MCO_CFG_EVENTS_SUPPORTED
        event_id = field->event_id;
    #endif
    referencedTable = field->refto_class <= 0 ? NULL : (McoTable*)table->db->_tables->getAt(field->refto_class - 1);
    arraySize = 0;
    _element = NULL;
    _components = NULL;
    indexAware = 0;
    structSizeU = 0;
    structSizeC = 0;
    structAlignU = 0;
    structAlignC = 0;
    fieldNo = field->no;
    sqlElemType = tpNull;
    if (indicator != NULL) {
        indicator_layout = indicator->layout;
        indicatorNo = indicator->no;
    }
    switch (mcoType)
    {
        #ifdef MCO_DB_FT_BOOL
        case MCO_DB_FT_BOOL:
             sqlType = tpBool;
             break;
        #endif
        case MCO_DB_FT_UINT1:
            sqlType = tpUInt1;
            break;
        case MCO_DB_FT_UINT2:
            sqlType = tpUInt2;
            break;
        case MCO_DB_FT_UINT4:
            sqlType = tpUInt4;
            break;
        case MCO_DB_FT_INT1:
            sqlType = tpInt1;
            break;
        case MCO_DB_FT_INT2:
            sqlType = tpInt2;
            break;
        case MCO_DB_FT_INT4:
            sqlType = tpInt4;
            break;
        case MCO_DB_FT_CHARS:
        case MCO_DB_FT_STRING:
            sqlType = tpString;
            break;
        case MCO_DB_FT_UNICODE_CHARS:
        case MCO_DB_FT_UNICODE_STRING:
        case MCO_DB_FT_WIDE_CHARS:
        case MCO_DB_FT_WCHAR_STRING:
            sqlType = tpUnicode;
            break;
        case MCO_DB_FT_REF:
            sqlType = tpRaw;
            break;
        case MCO_DB_FT_FLOAT:
            sqlType = tpReal4;
            break;
        case MCO_DB_FT_DOUBLE:
            sqlType = tpReal8;
            break;
        case MCO_DB_FT_UINT8:
            sqlType = tpUInt8;
            break;
        case MCO_DB_FT_INT8:
            sqlType = (referencedTable != NULL) ? tpReference : tpInt8;
            break;
        case MCO_DB_FT_AUTOID:
             /* 8 byte */
            table->autoidOffsetU = layout.u_offset;
            table->autoidOffsetC = layout.c_offset;
            table->autoidFieldNo = field->no;
            // no break
        case MCO_DB_FT_AUTOOID:
             /* 8 byte */
            sqlType = tpReference;
            break;
        case MCO_DB_FT_OBJVERS:
             /* 2 byte */
            sqlType = tpUInt2;
            break;
        case MCO_DB_FT_DATE:
        case MCO_DB_FT_TIME:
        case MCO_DB_FT_DATETIME:
            sqlType = tpDateTime;
            break;
        case MCO_DB_FT_STRUCT:
             /* not used in indexed field */
            {
                mco_dict_struct_t* s = MCO_DICT_STRUCT(db, field->struct_num);
                sqlType = tpStruct;
                _components = structComponents(table, db, s, this);
                structSizeC = (s->c_size + s->c_align - 1) &~(s->c_align - 1);
                structSizeU = (s->u_size + s->u_align - 1) &~(s->u_align - 1);
                structAlignC = s->c_align;
                structAlignU = s->u_align;
                table->isPlain = false;
                break;
            }
        case MCO_DB_FT_BLOB:
             /* not used in indexed field */
            sqlType = tpBlob;
            table->isPlain = false;
            break;
        case MCO_DB_FT_SEQUENCE_UINT1:
        case MCO_DB_FT_SEQUENCE_UINT2:
        case MCO_DB_FT_SEQUENCE_UINT4:
        case MCO_DB_FT_SEQUENCE_UINT8:
        case MCO_DB_FT_SEQUENCE_INT1:
        case MCO_DB_FT_SEQUENCE_INT2:
        case MCO_DB_FT_SEQUENCE_INT4:
        case MCO_DB_FT_SEQUENCE_INT8:
        case MCO_DB_FT_SEQUENCE_FLOAT:
        case MCO_DB_FT_SEQUENCE_DOUBLE:
        case MCO_DB_FT_SEQUENCE_CHAR:
            sqlType = tpSequence;
            sqlElemType = mco2sql[mcoType];
            table->isPlain = false;
            break;
        default:
            MCO_THROW InvalidOperation(String::format("Operations with type %s are not supported", typeMnemonic[mcoType])->cstr());    
    }
    if (flags & MCO_DICT_FLDF_NUMERIC) {
        sqlType = tpNumeric;
        table->isPlain = false;
    }
    if ((flags &(MCO_DICT_FLDF_ARRAY | MCO_DICT_FLDF_VECTOR)) != 0)
    {
        McoField* array = new McoField();
        *array = * this;
        _scope = array;
        _name = String::create("[]");
        array->_element = this;
        array->arraySize = field->array_size;
        array->sqlType = tpArray;
        array->sqlElemType = sqlType;
        table->isPlain = false;
        return array;
    }
    return this;
}


bool McoField::isNullable()
{
    return (flags & MCO_DICT_FLDF_NULLABLE) != 0;
}

bool McoField::isAutoGenerated()
{
    switch (mcoType)
    {
        case MCO_DB_FT_AUTOID:
        case MCO_DB_FT_AUTOOID:
        case MCO_DB_FT_OBJVERS:
            return true;
        default:
            return false;
    }
}


String* McoField::name()
{
    return _name;
}

Type McoField::type()
{
    return sqlType;
}

Type McoField::elementType()
{
    return sqlElemType;
}

int McoField::elementSize()
{
    return seq_elem_size;
}

SortOrder McoField::order()
{
    return (SortOrder)seq_order;
}

Table* McoField::table()
{
    return _table;
}

Field* McoField::scope()
{
    return _scope;
}

bool McoField::loadStructComponent(mco_objhandle_h hnd)
{
    mco_objhandle_t obj;
    if (sqlType != tpStruct)
    {
        MCO_THROW InvalidOperation("McoField::get(array component)");
    }
    if (_scope != NULL)
    {
        if (!_scope->loadStructComponent(hnd))
        {
            return false;
        }
    }
    MCO_RET ret;
    if (flags & MCO_DICT_FLDF_OPTIONAL)
    {
        ret = mco_w_optional_get2(hnd, &obj, layout.u_offset, layout.c_offset, fieldNo );
        if (ret == MCO_E_EMPTYOPTIONAL)
        {
            return false;
        }
    }
    else
    {
        ret = mco_w_struct_get2(hnd, &obj, layout.u_offset, layout.c_offset, fieldNo);
    }
    McoDatabase::checkStatus(ret);
    *hnd = obj;
    return true;
}

void McoField::storeStructComponent(mco_objhandle_h hnd, Struct* rec)
{
    mco_objhandle_t obj;
    if (sqlType != tpStruct)
    {
        MCO_THROW InvalidOperation("McoField::set(array component)");
    }
    if (_scope != NULL)
    {
        _scope->storeStructComponent(hnd, rec);
    }
    if ((flags & MCO_DICT_FLDF_OPTIONAL) != 0)
    {
        McoDatabase::checkStatus(mco_w_optional_put2(hnd, &obj, structNum, layout.u_offset, layout.c_offset, fieldNo,
                                 structSizeU, structSizeC, structAlignU, structAlignC));
    }
    else
    {
        McoDatabase::checkStatus(mco_w_struct_put2(hnd, &obj, layout.u_offset, layout.c_offset, fieldNo, indexAware));
    }
    *hnd = obj;
    mco_tmgr_init_handle(hnd);
}

McoSql::Value* McoField::get(Struct* rec)
{
    AbstractAllocator* allocator = ((McoRecord*)rec)->allocator;
    mco_objhandle_t obj = ((McoRecord*)rec)->obj;
    mco_tmgr_init_handle(&obj);
    if (_scope != NULL && rec->scope() == NULL)
    {
        if (!_scope->loadStructComponent(&obj))
        {
            return &Null;
        }
    }
    if ((flags & MCO_DICT_FLDF_NULLABLE) != 0)
    {
        uint1 indicator;
        McoDatabase::checkStatus(mco_w_bit_get2(&obj, indicator_layout.u_offset, indicator_layout.c_offset, indicatorNo, &indicator));
        if (!indicator) {
            return &Null;
        }
    }
    if ((flags & MCO_DICT_FLDF_ARRAY) != 0)
    {
        return new (allocator) McoArray(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
    }
    else if ((flags & MCO_DICT_FLDF_VECTOR) != 0)
    {
        return new (allocator) McoVector(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
    }
    switch (mcoType)
    {
        #ifdef MCO_DB_FT_BOOL
            case MCO_DB_FT_BOOL:
                {
                    uint1 value;
                    McoDatabase::checkStatus(mco_w_bit_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, &value));
                    return BoolValue::create(value != 0);
                }
            #endif
        case MCO_DB_FT_UINT1:
            {
                uint1 value;
                McoDatabase::checkStatus(mco_w_b1_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, &value));
                return new (allocator) IntValue(value);
            }
        case MCO_DB_FT_OBJVERS:
             /* 2 byte */
        case MCO_DB_FT_UINT2:
            {
                uint2 value;
                McoDatabase::checkStatus(mco_w_b2_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, &value));
                return new (allocator) IntValue(value);
            }
        case MCO_DB_FT_UINT4:
            {
                uint4 value;
                McoDatabase::checkStatus(mco_w_b4_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, &value));
                return new (allocator) IntValue(value);
            }
        case MCO_DB_FT_INT1:
            {
                int1 value;
                McoDatabase::checkStatus(mco_w_b1_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, &value));
                if (flags & MCO_DICT_FLDF_NUMERIC) {
                    return new (allocator) NumericValue((int64_t)value, _precision);
                }
                return new (allocator) IntValue(value);
            }
        case MCO_DB_FT_INT2:
            {
                int2 value;
                McoDatabase::checkStatus(mco_w_b2_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, &value));
                if (flags & MCO_DICT_FLDF_NUMERIC) {
                    return new (allocator) NumericValue((int64_t)value, _precision);
                }
                return new (allocator) IntValue(value);
            }
        case MCO_DB_FT_INT4:
            {
                int4 value;
                McoDatabase::checkStatus(mco_w_b4_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, &value));
                if (flags & MCO_DICT_FLDF_NUMERIC) {
                    return new (allocator) NumericValue((int64_t)value, _precision);
                }
                return new (allocator) IntValue(value);
            }
            #ifdef UNICODE_SUPPORT
            case MCO_DB_FT_UNICODE_CHARS:
                {
                    UnicodeFixedString* str = UnicodeFixedString::create(fieldSize, allocator);
                    McoDatabase::checkStatus(mco_w_nchars_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, (nchar_t*)str ->body(), fieldSize + 1, fieldSize));
                    convertNcharToWchar(str);
                    return str;
                }
            case MCO_DB_FT_UNICODE_STRING:
                {
                    uint2 len;
                    McoDatabase::checkStatus(mco_w_nstring_len2(&obj, layout.u_offset, layout.c_offset, fieldNo, &len));
                    UnicodeString* str = UnicodeString::create(len, allocator);
                    McoDatabase::checkStatus(mco_w_nstring_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, (nchar_t*)str ->body(), len + 1, &len));
                    convertNcharToWchar(str);
                    return str;
                }
                #ifdef MCO_CFG_WCHAR_SUPPORT
                case MCO_DB_FT_WIDE_CHARS:
                    {
                        UnicodeFixedString* str = UnicodeFixedString::create(fieldSize, allocator);
                        McoDatabase::checkStatus(mco_w_wchars_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, str->body(), fieldSize + 1, fieldSize));
                        return str;
                    }
                case MCO_DB_FT_WCHAR_STRING:
                    {
                        uint2 len;
                        McoDatabase::checkStatus(mco_w_wstring_len2(&obj, layout.u_offset, layout.c_offset, fieldNo, &len));
                        UnicodeString* str = UnicodeString::create(len, allocator);
                        McoDatabase::checkStatus(mco_w_wstring_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, str->body(),
                                                 len + 1, &len));
                        return str;
                    }
                #endif
            #endif
        case MCO_DB_FT_CHARS:
            {
                String* str = FixedString::create(fieldSize, allocator);
                McoDatabase::checkStatus(mco_w_chars_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, str->body(), fieldSize + 1, fieldSize));
                return str;
            }
        case MCO_DB_FT_STRING:
            {
                uint2 len;
                McoDatabase::checkStatus(mco_w_string_len2(&obj, layout.u_offset, layout.c_offset, fieldNo, &len));
                String* str = String::create(len, allocator);
                McoDatabase::checkStatus(mco_w_string_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, str->body(), len + 1, &len));
                return str;
            }
        case MCO_DB_FT_REF:
            {
                int4 value;
                McoDatabase::checkStatus(mco_w_b4_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, &value));
                return new (allocator) IntValue(value);
            }
            #ifndef NO_FLOATING_POINT
            case MCO_DB_FT_FLOAT:
                {
                    float value;
                    McoDatabase::checkStatus(mco_w_b4_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, &value));
                    return new (allocator) RealValue(value);
                }
            case MCO_DB_FT_DOUBLE:
                {
                    double value;
                    McoDatabase::checkStatus(mco_w_b8_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, &value));
                    return new (allocator) RealValue(value);
                }
            #endif
        case MCO_DB_FT_UINT8:
            {
                int64_t value;
                McoDatabase::checkStatus(mco_w_b8_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, &value));
                return new (allocator) IntValue(value);
            }
        case MCO_DB_FT_AUTOID:
             /* 8 byte */
        case MCO_DB_FT_AUTOOID:
             /* 8 byte */
            {
                int64_t value;
                McoDatabase::checkStatus(mco_w_b8_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, &value));
                return new (allocator) McoReference(((McoRecord*)rec)->trans, _table, value);
            }
        case MCO_DB_FT_INT8:
            {
                int64_t value;
                McoDatabase::checkStatus(mco_w_b8_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, &value));
                if (flags & MCO_DICT_FLDF_NUMERIC) {
                    return new (allocator) NumericValue(value, _precision);
                }
                return (referencedTable != NULL) ? value == 0 ? (McoSql::Value*) &Null: (McoSql::Value*)new (allocator) McoReference(((McoRecord*)
                        rec)->trans, referencedTable, value): (McoSql::Value*)new (allocator) IntValue(value);
            }
        case MCO_DB_FT_DATE:
        case MCO_DB_FT_TIME:
            {
                uint4 value;
                McoDatabase::checkStatus(mco_w_b4_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, &value));
                return new (allocator) DateTime((time_t)value);
            }
        case MCO_DB_FT_DATETIME:
            {
                int64_t value;
                McoDatabase::checkStatus(mco_w_b8_get2(&obj, layout.u_offset, layout.c_offset, fieldNo, &value));
                return new (allocator) DateTime((time_t)value);
            }
        case MCO_DB_FT_STRUCT:
             /* not used in indexed field */
            {
                mco_objhandle_t s;
                if ((flags &MCO_DICT_FLDF_OPTIONAL) != 0)
                {
                    MCO_RET ret = mco_w_optional_get2(&obj, &s, layout.u_offset, layout.c_offset, fieldNo);
                    if (ret == MCO_E_EMPTYOPTIONAL)
                    {
                        return &Null;
                    }
                }
                else
                {
                    McoDatabase::checkStatus(mco_w_struct_get2(&obj, &s, layout.u_offset, layout.c_offset, fieldNo));
                }
                return new (allocator) McoStruct(_components, ((McoRecord*)rec)->trans, &s, rec);
            }
        case MCO_DB_FT_BLOB:
             /* not used in indexed field */
            return new (allocator) McoBlob(this, &obj);
#ifdef MCO_CFG_WRAPPER_SEQUENCE_SUPPORT
      case MCO_DB_FT_SEQUENCE_UINT1:
        return new (tpUInt1, allocator) McoSequence<uint1>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_UINT2:
        return new (tpUInt2, allocator) McoSequence<uint2>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_UINT4:
        return new (tpUInt4, allocator) McoSequence<uint4>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_UINT8:
        return new (tpUInt8, allocator) McoSequence<uint8>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_INT1:
        return new (tpInt1, allocator) McoSequence<int1>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_INT2:
        return new (tpInt2, allocator) McoSequence<int2>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_INT4:
        return new (tpInt4, allocator) McoSequence<int4>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_INT8:
        return new (tpInt8, allocator) McoSequence<mco_int8>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_FLOAT:
        return new (tpReal4, allocator) McoSequence<float>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_DOUBLE:
        return new (tpReal8, allocator) McoSequence<double>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_CHAR:
        return new (tpString, allocator) McoSequence<char>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
#endif
      default:
            MCO_THROW InvalidOperation(String::format("Operations with type %s are not supported", typeMnemonic[mcoType])->cstr());    
    }
    return NULL;
}

McoSql::Value* McoField::update(Struct* rec)
{
    AbstractAllocator* allocator = ((McoRecord*)rec)->allocator;
    mco_objhandle_t obj = ((McoRecord*)rec)->obj;
    mco_tmgr_init_handle(&obj);
    if (_scope != NULL && rec->scope() == NULL)
    {
        _scope->storeStructComponent(&obj, rec);
    }
    if ((flags & MCO_DICT_FLDF_ARRAY) != 0)
    {
        return new (allocator) McoArray(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
    }
    else if ((flags & MCO_DICT_FLDF_VECTOR) != 0)
    {
        return new (allocator) McoVector(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
    }
    switch (mcoType) {
      case MCO_DB_FT_STRUCT:
      {
        mco_objhandle_t s;
        if ((flags & MCO_DICT_FLDF_OPTIONAL) != 0)
        {
            McoDatabase::checkStatus(mco_w_optional_put2(&obj, &s, structNum, layout.u_offset, layout.c_offset, fieldNo,
                                                         structSizeU, structSizeC, structAlignU, structAlignC));
        }
        else
        {
            McoDatabase::checkStatus(mco_w_struct_put2(&obj, &s, layout.u_offset, layout.c_offset, fieldNo, indexAware));
        }
        return new (allocator) McoStruct(_components, ((McoRecord*)rec)->trans, &s, rec);
      }
      case MCO_DB_FT_BLOB:
         /* not used in indexed field */
        return new (allocator) McoBlob(this, &obj);
#ifdef MCO_CFG_WRAPPER_SEQUENCE_SUPPORT
      case MCO_DB_FT_SEQUENCE_UINT1:
        return new (tpUInt1, allocator) McoSequence<uint1>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_UINT2:
        return new (tpUInt2, allocator) McoSequence<uint2>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_UINT4:
        return new (tpUInt4, allocator) McoSequence<uint4>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_UINT8:
        return new (tpUInt8, allocator) McoSequence<uint8>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_INT1:
        return new (tpInt1, allocator) McoSequence<int1>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_INT2:
        return new (tpInt2, allocator) McoSequence<int2>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_INT4:
        return new (tpInt4, allocator) McoSequence<int4>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_INT8:
        return new (tpInt8, allocator) McoSequence<mco_int8>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_FLOAT:
        return new (tpReal4, allocator) McoSequence<float>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_DOUBLE:
        return new (tpReal8, allocator) McoSequence<double>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
      case MCO_DB_FT_SEQUENCE_CHAR:
        return new (tpString, allocator) McoSequence<char>(this, ((McoRecord*)rec)->trans, &((McoRecord*)rec)->obj);
#endif
      default:
        MCO_THROW InvalidOperation("McoField::update");
    }
}

void McoField::set(Struct* rec, McoSql::Value* v)
{
    if (v == NULL)
    {
        v = &Null;
    }
    if ((flags & (MCO_DICT_FLDF_ARRAY | MCO_DICT_FLDF_VECTOR)) != 0)
    {
        if (v->type() == tpString) {
            int errPos;
            v = parseStringAsArray(sqlElemType, ((String*)v)->cstr(), errPos);
            if (v == NULL) {
                if (errPos < 0) {
                    MCO_THROW RuntimeError("Unsupported array type");
                } else {
                    MCO_THROW RuntimeError(String::format("Failed to parse array at position %d", errPos)->cstr());
                }
            }
        }
        if (v->type() == tpArray)
        {
            Array* dst = (Array*)update(rec);
            Array* src = (Array*)v;
            if (src->getElemType() != dst->getElemType() && (dst->getElemType() != tpDateTime || src->getElemType() != tpInt8)) {
                src = castArray(dst->getElemType(), src);
            }
            char buf[MCO_CFG_ARRAY_COPY_BUF_SIZE];
            int buf_size = MCO_CFG_ARRAY_COPY_BUF_SIZE / src->getElemSize();
            int size = src->size();
            //if (dst->size() != size)
            {
                dst->setSize(size);
            }
            if (dst->getElemType() == tpNumeric || dst->getElemType() == tpString || dst->getElemType() == tpUnicode) {
                for (int i = 0; i < size; i++) {
                    dst->setAt(i, src->getAt(i));
                }
            } else {
                for (int offs = 0; offs < size; offs += buf_size)
                {
                    int len = size - offs < buf_size ? size - offs : buf_size;
                    src->getBody(buf, offs, len);
                    dst->setBody(buf, offs, len);
                }
            }
            return ;
        }
        else if (v != &Null)
        {
            MCO_THROW InvalidOperation("McoField::set(array)");
        } 
        else if ((flags & MCO_DICT_FLDF_NULLABLE) == 0)
        {
            MCO_THROW NullValueException();
        }
    }
    mco_objhandle_t obj = ((McoRecord*)rec)->obj;
    mco_tmgr_init_handle(&obj);
    if (_scope != NULL && rec->scope() == NULL)
    {
        _scope->storeStructComponent(&obj, rec);
    }
    if ((flags & MCO_DICT_FLDF_NULLABLE) != 0)
    {
        if (v != &Null) {
            McoDatabase::checkStatus(mco_w_bit_put2(&obj, indicator_layout.u_offset, indicator_layout.c_offset, indicatorNo, indexAware, 1));
        } else {
            McoDatabase::checkStatus(mco_w_bit_put2(&obj, indicator_layout.u_offset, indicator_layout.c_offset, indicatorNo, indexAware, 0));
            return;
        }
    } else if (v->isNull()) { 
        MCO_THROW NullValueException();
    }

    MCO_RET ret = MCO_S_OK;
    #ifdef MCO_CFG_EVENTS_SUPPORTED
        if (event_id == MCO_DB_EVENT_ID_NONE)
        {
        #endif
        switch (mcoType)
        {
            #ifdef MCO_DB_FT_BOOL
                case MCO_DB_FT_BOOL:
                    {
                        uint1 value = (uint1)(v->isTrue() ? 1 : 0);
                        ret = mco_w_bit_put2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, value);
                        break;
                    }
                #endif
            case MCO_DB_FT_UINT1:
                {
                    uint1 value = (uint1)v->intValue();
                    ret = mco_w_b1_put2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, value);
                    break;
                }
            case MCO_DB_FT_OBJVERS:
                 /* 2 byte */
            case MCO_DB_FT_UINT2:
                {
                    uint2 value = (uint2)v->intValue();
                    ret = mco_w_b2_put2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, value);
                    break;
                }
            case MCO_DB_FT_UINT4:
                {
                    uint4 value = (uint4)v->intValue();
                    ret = mco_w_b4_put2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, value);
                    break;
                }
            case MCO_DB_FT_DATE:
            case MCO_DB_FT_TIME:
                {
                    uint4 value = (uint4)v->timeValue();
                    ret = mco_w_b4_put2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, value);
                    break;
                }
            case MCO_DB_FT_DATETIME:
                {
                    int64_t value = v->timeValue();
                    ret = mco_w_b8_put2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, &value);
                    break;
                }
            case MCO_DB_FT_INT1:
                {
                    int1 value = (int1)v->intValue(_precision);
                    ret = mco_w_b1_put2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, (uint1)value);
                    break;
                }
            case MCO_DB_FT_INT2:
                {
                    int2 value = (int2)v->intValue(_precision);
                    ret = mco_w_b2_put2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, (uint2)value);
                    break;
                }
            case MCO_DB_FT_INT4:
                {
                    int4 value = (int4)v->intValue(_precision);
                    ret = mco_w_b4_put2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, (uint4)value);
                    break;
                }
                #ifdef UNICODE_SUPPORT
                case MCO_DB_FT_UNICODE_CHARS:
                    {
                        UnicodeString* str = v->unicodeStringValue();
                        ret = mco_w_nchars_put2(&obj, layout.u_offset, layout.c_offset, fieldNo, convertWcharToNchar(str), str ->size(), fieldSize, indexAware);
                        break;
                    }
                case MCO_DB_FT_UNICODE_STRING:
                    {
                        UnicodeString* str = v->unicodeStringValue();
                        ret = mco_w_nstring_put2(&obj, layout.u_offset, layout.c_offset, fieldNo, convertWcharToNchar(str), str ->size(), indexAware);
                        break;
                    }
                    #ifdef MCO_CFG_WCHAR_SUPPORT
                    case MCO_DB_FT_WIDE_CHARS:
                        {
                            UnicodeString* str = v->unicodeStringValue();
                            ret = mco_w_wchars_put2(&obj, layout.u_offset, layout.c_offset, fieldNo, str->body(), str->size(), fieldSize, indexAware);
                            break;
                        }
                    case MCO_DB_FT_WCHAR_STRING:
                        {
                            UnicodeString* str = v->unicodeStringValue();
                            ret = mco_w_wstring_put2(&obj, layout.u_offset, layout.c_offset, fieldNo, str->body(), str->size(), indexAware);
                            break;
                        }
                    #endif
                #endif
            case MCO_DB_FT_CHARS:
                {
                    String* str = v->stringValue();
                    ret = mco_w_chars_put2(&obj, layout.u_offset, layout.c_offset, fieldNo, str->body(), str->size(), fieldSize, indexAware);
                    break;
                }
            case MCO_DB_FT_STRING:
                {
                    String* str = v->stringValue();
                    ret = mco_w_string_put2(&obj, layout.u_offset, layout.c_offset, fieldNo, str->body(), str->size(), indexAware);
                    break;
                }
            case MCO_DB_FT_REF:
                break;
                #ifndef NO_FLOATING_POINT
                case MCO_DB_FT_FLOAT:
                    {
                        union
                        {
                            float f;
                            uint4 i;
                        }
                        value;
                        value.f = (float)v->realValue();
                        ret = mco_w_b4_put2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, value.i);
                        break;
                    }
                case MCO_DB_FT_DOUBLE:
                    {
                        double value = v->realValue();
                        ret = mco_w_b8_put2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, &value);
                        break;
                    }
                #endif
            case MCO_DB_FT_AUTOID:
                 /* 8 byte */
            case MCO_DB_FT_AUTOOID:
                 /* 8 byte */
                // assignment to autogenerated field has no effect
                break;
            case MCO_DB_FT_UINT8:
            case MCO_DB_FT_INT8:
                {
                    int64_t value = v->intValue(_precision);
                    ret = mco_w_b8_put2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, &value);
                    break;
                }
            case MCO_DB_FT_STRUCT:
                 /* not used in indexed field */
                MCO_THROW InvalidOperation("McoField::set(struct)");
            case MCO_DB_FT_BLOB:
                 /* not used in indexed field */
                if (v->type() == tpArray)
                {
                    Blob* dst = (Blob*)update(rec);
                    Array* src = (Array*)v;
                    if (src->getElemSize() != 1) {
                        MCO_THROW RuntimeError("Incompatible array and blob types");
                    }
                    char buf[MCO_CFG_ARRAY_COPY_BUF_SIZE];
                    int size = src->size();
                    dst->truncate();
                    for (int offs = 0; offs < size; offs += MCO_CFG_ARRAY_COPY_BUF_SIZE)
                    {
                        int len = size - offs < MCO_CFG_ARRAY_COPY_BUF_SIZE ? size - offs: MCO_CFG_ARRAY_COPY_BUF_SIZE;
                        src->getBody(buf, offs, len);
                        dst->append(buf, len);
                    }
                    return ;
                }
                else if (v->type() == tpString)
                {
                    Blob* dst = (Blob*)update(rec);
                    String* str = (String*)v;
                    dst->truncate();
                    dst->append(str->body(), str->size());
                    return ;
                }
                else if (v->type() == tpBlob)
                {
                    Blob* dst = (Blob*)update(rec);
                    Blob* src = (Blob*)v;
                    char buf[MCO_CFG_ARRAY_COPY_BUF_SIZE];
                    dst->truncate();
                    while (true)
                    {
                        int len = src->get(buf, MCO_CFG_ARRAY_COPY_BUF_SIZE);
                        if (len == 0)
                        {
                            return;
                        }
                        dst->append(buf, len);
                    }
                }
                MCO_THROW InvalidOperation("McoField::set(blob)");
#ifdef MCO_CFG_WRAPPER_SEQUENCE_SUPPORT
            case MCO_DB_FT_SEQUENCE_UINT1:
            {
                McoSequence<uint1>* dst = (McoSequence<uint1>*)update(rec);
                if (v->type() == tpArray)
                {
                    Array* src = (Array*)v;
                    if (src->getElemType() != dst->elemType) {
                        src = castArray(dst->elemType, src);
                    }
                    int size = src->size();
                    if (src->isPlain()) {
                        dst->append((uint1*)src->pointer(), size);
                    } else {  
                        uint1 buf[MCO_CFG_ARRAY_COPY_BUF_SIZE];
                        for (int offs = 0; offs < size; offs += MCO_CFG_ARRAY_COPY_BUF_SIZE)
                        {
                            int len = size - offs < MCO_CFG_ARRAY_COPY_BUF_SIZE ? size - offs: MCO_CFG_ARRAY_COPY_BUF_SIZE;
                            src->getBody(buf, offs, len);
                            dst->append(buf, len);
                        }
                    }
                 } else if (v->type() == tpString) {
                    dst->store(McoGenericSequence::parse(tpUInt1, ((String*)v)->cstr())->getIterator());
                } else if (v->type() == tpSequence) {
                    dst->store(((McoGenericSequence*)v)->getIterator());
                } else {
                    uint1 val = (uint1)v->intValue();
                    dst->append(&val, 1);
                }
                break;
            }
            case MCO_DB_FT_SEQUENCE_UINT2:
            {
                McoSequence<uint2>* dst = (McoSequence<uint2>*)update(rec);
                if (v->type() == tpArray)
                {
                    Array* src = (Array*)v;
                    if (src->getElemType() != dst->elemType) {
                        src = castArray(dst->elemType, src);
                    }
                    int size = src->size();
                    if (src->isPlain()) {
                        dst->append((uint2*)src->pointer(), size);
                    } else {  
                        uint2 buf[MCO_CFG_ARRAY_COPY_BUF_SIZE];
                        for (int offs = 0; offs < size; offs += MCO_CFG_ARRAY_COPY_BUF_SIZE)
                        {
                            int len = size - offs < MCO_CFG_ARRAY_COPY_BUF_SIZE ? size - offs: MCO_CFG_ARRAY_COPY_BUF_SIZE;
                            src->getBody(buf, offs, len);
                            dst->append(buf, len);
                        }
                    }
                } else if (v->type() == tpString) {
                    dst->store(McoGenericSequence::parse(tpUInt2, ((String*)v)->cstr())->getIterator());
                } else if (v->type() == tpSequence) {
                    dst->store(((McoGenericSequence*)v)->getIterator());
                } else {
                    uint2 val = (uint2)v->intValue();
                    dst->append(&val, 1);
                }
                break;
            }
            case MCO_DB_FT_SEQUENCE_UINT4:
            {
                McoSequence<uint4>* dst = (McoSequence<uint4>*)update(rec);
                if (v->type() == tpArray)
                {
                    Array* src = (Array*)v;
                    if (src->getElemType() != dst->elemType) {
                        src = castArray(dst->elemType, src);
                    }
                    int size = src->size();
                    if (src->isPlain()) {
                        dst->append((uint4*)src->pointer(), size);
                    } else {  
                        uint4 buf[MCO_CFG_ARRAY_COPY_BUF_SIZE];
                        for (int offs = 0; offs < size; offs += MCO_CFG_ARRAY_COPY_BUF_SIZE)
                        {
                            int len = size - offs < MCO_CFG_ARRAY_COPY_BUF_SIZE ? size - offs: MCO_CFG_ARRAY_COPY_BUF_SIZE;
                            src->getBody(buf, offs, len);
                            dst->append(buf, len);
                        }
                    }
                } else if (v->type() == tpString) {
                    dst->store(McoGenericSequence::parse(tpUInt4, ((String*)v)->cstr())->getIterator());
                } else if (v->type() == tpSequence) {
                    dst->store(((McoGenericSequence*)v)->getIterator());
                } else {
                    uint4 val = (uint4)v->intValue();
                    dst->append(&val, 1);
                }
                break;
            }
            case MCO_DB_FT_SEQUENCE_UINT8:
            {
                McoSequence<uint8>* dst = (McoSequence<uint8>*)update(rec);
                if (v->type() == tpArray)
                {
                    Array* src = (Array*)v;
                    if (src->getElemType() != dst->elemType) {
                        src = castArray(dst->elemType, src);
                    }
                    int size = src->size();
                    if (src->isPlain()) {
                        dst->append((uint8*)src->pointer(), size);
                    } else {  
                        uint8 buf[MCO_CFG_ARRAY_COPY_BUF_SIZE];
                        for (int offs = 0; offs < size; offs += MCO_CFG_ARRAY_COPY_BUF_SIZE)
                        {
                            int len = size - offs < MCO_CFG_ARRAY_COPY_BUF_SIZE ? size - offs: MCO_CFG_ARRAY_COPY_BUF_SIZE;
                            src->getBody(buf, offs, len);
                            dst->append(buf, len);
                        }
                    }
                } else if (v->type() == tpString) {
                    dst->store(McoGenericSequence::parse(tpUInt8, ((String*)v)->cstr())->getIterator());
                } else if (v->type() == tpSequence) {
                    dst->store(((McoGenericSequence*)v)->getIterator());
                } else {
                    uint8 val = (uint8)v->intValue();
                    dst->append(&val, 1);
                }
                break;
            }
            case MCO_DB_FT_SEQUENCE_INT1:
            {
                McoSequence<int1>* dst = (McoSequence<int1>*)update(rec);
                if (v->type() == tpArray)
                {
                    Array* src = (Array*)v;
                    if (src->getElemType() != dst->elemType) {
                        src = castArray(dst->elemType, src);
                    }
                    int size = src->size();
                    if (src->isPlain()) {
                        dst->append((int1*)src->pointer(), size);
                    } else {  
                        int1 buf[MCO_CFG_ARRAY_COPY_BUF_SIZE];
                        for (int offs = 0; offs < size; offs += MCO_CFG_ARRAY_COPY_BUF_SIZE)
                        {
                            int len = size - offs < MCO_CFG_ARRAY_COPY_BUF_SIZE ? size - offs: MCO_CFG_ARRAY_COPY_BUF_SIZE;
                            src->getBody(buf, offs, len);
                            dst->append(buf, len);
                        }
                    }
                 } else if (v->type() == tpString) {
                    dst->store(McoGenericSequence::parse(tpInt1, ((String*)v)->cstr())->getIterator());
                } else if (v->type() == tpSequence) {
                    dst->store(((McoGenericSequence*)v)->getIterator());
                } else {
                    int1 val = (int1)v->intValue();
                    dst->append(&val, 1);
                }
                break;
            }
            case MCO_DB_FT_SEQUENCE_INT2:
            {
                McoSequence<int2>* dst = (McoSequence<int2>*)update(rec);
                if (v->type() == tpArray)
                {
                    Array* src = (Array*)v;
                    if (src->getElemType() != dst->elemType) {
                        src = castArray(dst->elemType, src);
                    }
                    int size = src->size();
                    if (src->isPlain()) {
                        dst->append((int2*)src->pointer(), size);
                    } else {  
                        int2 buf[MCO_CFG_ARRAY_COPY_BUF_SIZE];
                        for (int offs = 0; offs < size; offs += MCO_CFG_ARRAY_COPY_BUF_SIZE)
                        {
                            int len = size - offs < MCO_CFG_ARRAY_COPY_BUF_SIZE ? size - offs: MCO_CFG_ARRAY_COPY_BUF_SIZE;
                            src->getBody(buf, offs, len);
                            dst->append(buf, len);
                        }
                    }
                } else if (v->type() == tpString) {
                    dst->store(McoGenericSequence::parse(tpInt2, ((String*)v)->cstr())->getIterator());
                } else if (v->type() == tpSequence) {
                   dst->store(((McoGenericSequence*)v)->getIterator());
                } else {
                    int2 val = (int2)v->intValue();
                    dst->append(&val, 1);
                }
                break;
            }
            case MCO_DB_FT_SEQUENCE_INT4:
            {
                McoSequence<int4>* dst = (McoSequence<int4>*)update(rec);
                /* not used in indexed field */
                if (v->type() == tpArray)
                {
                    Array* src = (Array*)v;
                    if (src->getElemType() != dst->elemType) {
                        src = castArray(dst->elemType, src);
                    }
                    int size = src->size();
                    if (src->isPlain()) {
                        dst->append((int4*)src->pointer(), size);
                    } else {  
                        int4 buf[MCO_CFG_ARRAY_COPY_BUF_SIZE];
                        for (int offs = 0; offs < size; offs += MCO_CFG_ARRAY_COPY_BUF_SIZE)
                        {
                            int len = size - offs < MCO_CFG_ARRAY_COPY_BUF_SIZE ? size - offs: MCO_CFG_ARRAY_COPY_BUF_SIZE;
                            src->getBody(buf, offs, len);
                            dst->append(buf, len);
                        }
                    }
                } else if (v->type() == tpString) {
                    dst->store(McoGenericSequence::parse(tpInt4, ((String*)v)->cstr())->getIterator());
                } else if (v->type() == tpSequence) {
                   dst->store(((McoGenericSequence*)v)->getIterator());
                } else {
                    int4 val = (int4)v->intValue();
                    dst->append(&val, 1);
                }
                break;
            }
            case MCO_DB_FT_SEQUENCE_INT8:
            {
                McoSequence<int64_t>* dst = (McoSequence<int64_t>*)update(rec);
                if (v->type() == tpArray)
                {
                    Array* src = (Array*)v;
                    if (src->getElemType() != dst->elemType) {
                        src = castArray(dst->elemType, src);
                    }
                    int size = src->size();
                    if (src->isPlain()) {
                        dst->append((int64_t*)src->pointer(), size);
                    } else {  
                        int64_t buf[MCO_CFG_ARRAY_COPY_BUF_SIZE];
                        for (int offs = 0; offs < size; offs += MCO_CFG_ARRAY_COPY_BUF_SIZE)
                        {
                            int len = size - offs < MCO_CFG_ARRAY_COPY_BUF_SIZE ? size - offs: MCO_CFG_ARRAY_COPY_BUF_SIZE;
                            src->getBody(buf, offs, len);
                            dst->append(buf, len);
                        }
                    }
                } else if (v->type() == tpString) {
                    dst->store(McoGenericSequence::parse(tpInt8, ((String*)v)->cstr())->getIterator());
                } else if (v->type() == tpSequence) {
                    dst->store(((McoGenericSequence*)v)->getIterator());
                } else {
                    int64_t val = v->intValue();
                    dst->append(&val, 1);
                }
                break;
            }
            case MCO_DB_FT_SEQUENCE_FLOAT:
            {
                McoSequence<float>* dst = (McoSequence<float>*)update(rec);
                if (v->type() == tpArray)
                {
                    Array* src = (Array*)v;
                    if (src->getElemType() != dst->elemType) {
                        src = castArray(dst->elemType, src);
                    }
                    int size = src->size();
                    if (src->isPlain()) {
                        dst->append((float*)src->pointer(), size);
                    } else {  
                        float buf[MCO_CFG_ARRAY_COPY_BUF_SIZE];
                        for (int offs = 0; offs < size; offs += MCO_CFG_ARRAY_COPY_BUF_SIZE)
                        {
                            int len = size - offs < MCO_CFG_ARRAY_COPY_BUF_SIZE ? size - offs: MCO_CFG_ARRAY_COPY_BUF_SIZE;
                            src->getBody(buf, offs, len);
                            dst->append(buf, len);
                        }
                    }
                } else if (v->type() == tpString) {
                    dst->store(McoGenericSequence::parse(tpReal4, ((String*)v)->cstr())->getIterator());
                } else if (v->type() == tpSequence) {
                    dst->store(((McoGenericSequence*)v)->getIterator());
                } else {
                    float val = (float)v->realValue();
                    dst->append(&val, 1);
                }
                break;
            }
            case MCO_DB_FT_SEQUENCE_DOUBLE:
            {
                McoSequence<double>* dst = (McoSequence<double>*)update(rec);
                if (v->type() == tpArray)
                {
                    Array* src = (Array*)v;
                    if (src->getElemType() != dst->elemType) {
                        src = castArray(dst->elemType, src);
                    }
                    int size = src->size();
                    if (src->isPlain()) {
                        dst->append((double*)src->pointer(), size);
                    } else {  
                        double buf[MCO_CFG_ARRAY_COPY_BUF_SIZE];
                        for (int offs = 0; offs < size; offs += MCO_CFG_ARRAY_COPY_BUF_SIZE)
                        {
                            int len = size - offs < MCO_CFG_ARRAY_COPY_BUF_SIZE ? size - offs: MCO_CFG_ARRAY_COPY_BUF_SIZE;
                            src->getBody(buf, offs, len);
                            dst->append(buf, len);
                        }
                    }
                } else if (v->type() == tpString) {
                    dst->store(McoGenericSequence::parse(tpReal8, ((String*)v)->cstr())->getIterator());
                } else if (v->type() == tpSequence) {
                    dst->store(((McoGenericSequence*)v)->getIterator());
                } else {
                    double val = v->realValue();
                    dst->append(&val, 1);
                }
                break;
            }
            case MCO_DB_FT_SEQUENCE_CHAR:
            {
                McoSequence<char>* dst = (McoSequence<char>*)update(rec);
                if (v->type() == tpArray)
                {
                    Array* src = (Array*)v;
                    int size = src->size();
                    if (src->isPlain()) {
                        if (src->getElemSize() != seq_elem_size) { 
                            MCO_THROW RuntimeError("Size of sequence element doesn't match");
                        }
                        dst->append((char*)src->pointer(), size);
                    } else {                      
                        for (int i = 0; i < size; i++) {
                            String* s = src->getAt(i)->stringValue();
                            dst->append(s->cstr(), 1);
                        }
                    }
                } else if (v->type() == tpSequence) {
                    dst->store(((McoGenericSequence*)v)->getIterator());
                } else {
                    dst->append(v->stringValue()->cstr(), 1);
                }
                break;
            }
#endif
            default:
                MCO_THROW InvalidOperation(String::format("Operations with type %s are not supported", typeMnemonic[mcoType])->cstr());    
        }
        #ifdef MCO_CFG_EVENTS_SUPPORTED
        }
        else
        {
            switch (mcoType)
            {
                #ifdef MCO_DB_FT_BOOL
                    case MCO_DB_FT_BOOL:
                        {
                            uint1 value = (uint1)(v->isTrue() ? 1 : 0);
                            ret = mco_w_bit_put_ev2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, value, event_id) ;
                            break;
                        }
                    #endif
                case MCO_DB_FT_UINT1:
                    {
                        uint1 value = (uint1)v->intValue();
                        ret = mco_w_b1_put_ev2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, value, event_id);
                        break;
                    }
                case MCO_DB_FT_OBJVERS:
                     /* 2 byte */
                case MCO_DB_FT_UINT2:
                    {
                        uint2 value = (uint2)v->intValue();
                        ret = mco_w_b2_put_ev2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, value, event_id);
                        break;
                    }
                case MCO_DB_FT_UINT4:
                    {
                        uint4 value = (uint4)v->intValue();
                        ret = mco_w_b4_put_ev2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, value, event_id);
                        break;
                    }
                case MCO_DB_FT_DATE:
                case MCO_DB_FT_TIME:
                    {
                        uint4 value = (uint4)v->timeValue();
                        ret = mco_w_b4_put_ev2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, value, event_id);
                        break;
                    }
                case MCO_DB_FT_DATETIME:
                    {
                        int64_t value = (int64_t)v->timeValue();
                        ret = mco_w_b8_put_ev2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, &value, event_id);
                        break;
                    }
                case MCO_DB_FT_INT1:
                    {
                        int1 value = (int1)v->intValue(_precision);
                        ret = mco_w_b1_put_ev2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, (uint1)value, event_id);
                        break;
                    }
                case MCO_DB_FT_INT2:
                    {
                        int2 value = (int2)v->intValue(_precision);
                        ret = mco_w_b2_put_ev2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, (uint2)value, event_id);
                        break;
                    }
                case MCO_DB_FT_INT4:
                    {
                        int4 value = (int4)v->intValue(_precision);
                        ret = mco_w_b4_put_ev2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, (uint4)value, event_id);
                        break;
                    }
                    #ifdef UNICODE_SUPPORT
                    case MCO_DB_FT_UNICODE_CHARS:
                        {
                            UnicodeString* str = v->unicodeStringValue();
                            ret = mco_w_nchars_put_ev2(&obj, layout.u_offset, layout.c_offset, fieldNo, convertWcharToNchar(str), str->size(), fieldSize, indexAware, event_id);
                            break;
                        }
                    case MCO_DB_FT_UNICODE_STRING:
                        {
                            UnicodeString* str = v->unicodeStringValue();
                            ret = mco_w_nstring_put_ev2(&obj, layout.u_offset, layout.c_offset, fieldNo, convertWcharToNchar(str) , str->size(), indexAware, event_id);
                            break;
                        }
                        #ifdef MCO_CFG_WCHAR_SUPPORT
                        case MCO_DB_FT_WIDE_CHARS:
                            {
                                UnicodeString* str = v->unicodeStringValue();
                                ret = mco_w_wchars_put_ev2(&obj, layout.u_offset, layout.c_offset, fieldNo, str->body(), str ->size(), fieldSize, indexAware, event_id);
                                break;
                            }
                        case MCO_DB_FT_WCHAR_STRING:
                            {
                                UnicodeString* str = v->unicodeStringValue();
                                ret = mco_w_wstring_put_ev2(&obj, layout.u_offset, layout.c_offset, fieldNo, str->body(), str ->size(), indexAware, event_id);
                                break;
                            }
                        #endif
                    #endif
                case MCO_DB_FT_CHARS:
                    {
                        String* str = v->stringValue();
                        ret = mco_w_chars_put_ev2(&obj, layout.u_offset, layout.c_offset, fieldNo, str->body(), str->size(),
                                                  fieldSize, indexAware, event_id);
                        break;
                    }
                case MCO_DB_FT_STRING:
                    {
                        String* str = v->stringValue();
                        ret = mco_w_string_put_ev2(&obj, layout.u_offset, layout.c_offset, fieldNo, str->body(), str->size(), indexAware, event_id);
                        break;
                    }
                case MCO_DB_FT_REF:
                    break;
                    #ifndef NO_FLOATING_POINT
                    case MCO_DB_FT_FLOAT:
                        {
                            union
                            {
                                float f;
                                uint4 i;
                            }
                            value;
                            value.f = (float)v->realValue();
                            ret = mco_w_b4_put_ev2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, value.i, event_id);
                            break;
                        }
                    case MCO_DB_FT_DOUBLE:
                        {
                            double value = v->realValue();
                            ret = mco_w_b8_put_ev2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, &value, event_id) ;
                            break;
                        }
                    #endif
                case MCO_DB_FT_AUTOID:
                     /* 8 byte */
                case MCO_DB_FT_AUTOOID:
                     /* 8 byte */
                    // assignment to autogenerated field has no effect
                    break;
                case MCO_DB_FT_UINT8:
                case MCO_DB_FT_INT8:
                    {
                        int64_t value = v->intValue(_precision);
                        ret = mco_w_b8_put_ev2(&obj, layout.u_offset, layout.c_offset, fieldNo, indexAware, &value, event_id);
                        break;
                    }
                case MCO_DB_FT_STRUCT:
                     /* not used in indexed field */
                    MCO_THROW InvalidOperation("McoField::set(struct)");
                case MCO_DB_FT_BLOB:
                     /* not used in indexed field */
                    MCO_THROW InvalidOperation("McoField::set(blob)");
                default:
                    MCO_THROW InvalidOperation(String::format("Operations with type %s are not supported", typeMnemonic[mcoType])->cstr());    
            }
        }
    #endif
    McoDatabase::checkStatus(ret);
}

String* McoField::referencedTableName()
{
    if (sqlType != tpReference)
    {
        MCO_THROW InvalidOperation("McoField::referencedTableName");
    }
	return referencedTable != NULL ? referencedTable->_name : NULL;
}

Iterator < Field > * McoField::components()
{
    if (sqlType != tpStruct)
    {
        MCO_THROW InvalidOperation("McoField::components");
    }
    return (Iterator < Field > *)_components->iterator();
}

Field* McoField::element()
{
    if (sqlType != tpArray)
    {
        MCO_THROW InvalidOperation("McoField::element");
    }
    return _element;
}

int McoField::precision()
{
    return _precision;
}

int McoField::width()
{
    return -1;
}

int McoField::fixedSize()
{
    if (sqlType == tpString || sqlType == tpUnicode)
    {
        return fieldSize;
    }
    else if (sqlType != tpArray)
    {
        MCO_THROW InvalidOperation("McoField::fixedSize");
    }
    return arraySize;
}

//
// Record
//

McoRecord::McoRecord(Vector < McoField > * fields, Transaction* trans, mco_objhandle_h hnd)
{
    this->fields = fields;
    this->trans = (McoTransaction*)trans;
    this->obj = *hnd;
    allocator = MemoryManager::getAllocator();
    mco_tmgr_init_handle(&obj);
}

mco_size_t McoRecord::extract(char* dst)
{
    MCO_WRK w;
    McoDatabase::checkStatus(mco_reading(&obj, &w));
    char* src = (char*)mco_obj_pos(w.pobj, obj.con, obj.pm, obj.mo, MCO_RO);
    mco_size_t size = 0;
    for (int i = 0, n = fields->length; i < n; i++) { 
        McoField* f = fields->items[i];
        int offs = f->layout.u_offset;
        switch (f->mcoType) { 
        case MCO_DB_FT_BOOL:
        case MCO_DB_FT_UINT1:
        case MCO_DB_FT_INT1:
            dst[size++] = src[offs];
            break;
        case MCO_DB_FT_OBJVERS:
        case MCO_DB_FT_UINT2:
        case MCO_DB_FT_INT2:
            size = MCO_ALIGN(size, 2);
            *(uint2*)&dst[size] = *(uint2*)&src[offs];
            size += 2;
            break;
        case MCO_DB_FT_UINT4:
        case MCO_DB_FT_INT4:
        case MCO_DB_FT_FLOAT:
        case MCO_DB_FT_DATE:
        case MCO_DB_FT_TIME:
            size = MCO_ALIGN(size, 4);
            *(uint4*)&dst[size] = *(uint4*)&src[offs];
            size += 4;
            break;
        #ifdef UNICODE_SUPPORT
        case MCO_DB_FT_UNICODE_CHARS:
            size = MCO_ALIGN(size, sizeof(nchar_t));
            mco_memcpy((nchar_t*)&dst[size], (nchar_t*)&src[offs], f->fieldSize*sizeof(nchar_t));
            size += f->fieldSize*sizeof(nchar_t);
            break;            
        #ifdef MCO_CFG_WCHAR_SUPPORT
        case MCO_DB_FT_WIDE_CHARS:
            size = MCO_ALIGN(size, sizeof(wchar_t));
            mco_memcpy((wchar_t*)&dst[size], (wchar_t*)&src[offs], f->fieldSize*sizeof(wchar_t));
            size += f->fieldSize*sizeof(wchar_t);
            break;            
        #endif
        #endif
        case MCO_DB_FT_CHARS:
            mco_memcpy(&dst[size], &src[offs], f->fieldSize);
            size += f->fieldSize;
            break;            
        case MCO_DB_FT_REF:
        case MCO_DB_FT_DOUBLE:
        case MCO_DB_FT_UINT8:
        case MCO_DB_FT_AUTOID:
        case MCO_DB_FT_AUTOOID:
        case MCO_DB_FT_INT8:
            size = MCO_ALIGN(size, 8);
            *(int64_t*)&dst[size] = *(int64_t*)&src[offs];
            size += 8;
            break;
        default:
            MCO_THROW InvalidOperation(String::format("Operations with type %s are not supported", typeMnemonic[f->mcoType])->cstr());    
        }
    }
    MCO_UNPIN(obj.con, obj.pm, src);
    return size;
}

mco_size_t McoRecord::extractCompact(char* dst)
{
    MCO_WRK w;
    McoDatabase::checkStatus(mco_reading(&obj, &w));
    char* src = (char*)mco_obj_pos(w.pobj, obj.con, obj.pm, obj.mo, MCO_RO);
    mco_size_t size = 0;
    for (int i = 0, n = fields->length; i < n; i++) { 
        McoField* f = fields->items[i];
        int offs = f->layout.c_offset;
        switch (f->mcoType) { 
        case MCO_DB_FT_BOOL:
        case MCO_DB_FT_UINT1:
        case MCO_DB_FT_INT1:
            dst[size++] = src[offs];
            break;
        case MCO_DB_FT_OBJVERS:
        case MCO_DB_FT_UINT2:
        case MCO_DB_FT_INT2:
            size = MCO_ALIGN(size, 2);
            *(uint2*)&dst[size] = *(uint2*)&src[offs];
            size += 2;
            break;
        case MCO_DB_FT_UINT4:
        case MCO_DB_FT_INT4:
        case MCO_DB_FT_FLOAT:
        case MCO_DB_FT_DATE:
        case MCO_DB_FT_TIME:
            size = MCO_ALIGN(size, 4);
            *(uint4*)&dst[size] = *(uint4*)&src[offs];
            size += 4;
            break;
        #ifdef UNICODE_SUPPORT
        case MCO_DB_FT_UNICODE_CHARS:
            size = MCO_ALIGN(size, sizeof(nchar_t));
            mco_memcpy((nchar_t*)&dst[size], (nchar_t*)&src[offs], f->fieldSize*sizeof(nchar_t));
            size += f->fieldSize*sizeof(nchar_t);
            break;            
        #ifdef MCO_CFG_WCHAR_SUPPORT
        case MCO_DB_FT_WIDE_CHARS:
            size = MCO_ALIGN(size, sizeof(wchar_t));
            mco_memcpy((wchar_t*)&dst[size], (wchar_t*)&src[offs], f->fieldSize*sizeof(wchar_t));
            size += f->fieldSize*sizeof(wchar_t);
            break;            
        #endif
        #endif
        case MCO_DB_FT_CHARS:
            mco_memcpy(&dst[size], &src[offs], f->fieldSize);
            size += f->fieldSize;
            break;            
        case MCO_DB_FT_REF:
        case MCO_DB_FT_DOUBLE:
        case MCO_DB_FT_UINT8:
        case MCO_DB_FT_AUTOID:
        case MCO_DB_FT_AUTOOID:
        case MCO_DB_FT_INT8:
            size = MCO_ALIGN(size, 8);
            *(int64_t*)&dst[size] = *(int64_t*)&src[offs];
            size += 4;
            break;
        default:
            MCO_THROW InvalidOperation(String::format("Operations with type %s are not supported", typeMnemonic[f->mcoType])->cstr());    
        }
    }
    MCO_UNPIN(obj.con, obj.pm, src);
    return size;
}

int McoRecord::nComponents()
{
    return fields->length;
}

McoSql::Value* McoRecord::get(int index)
{
    return fields->getAt(index)->get(this);
}

McoSql::Value* McoRecord::update(int index)
{
    return fields->getAt(index)->update(this);
}

void McoRecord::set(int index, McoSql::Value* value)
{
    fields->getAt(index)->set(this, value);
}

Struct* McoRecord::source()
{
    return this;
}

void McoRecord::deleteRecord()
{
    McoDatabase::checkStatus(mco_w_obj_delete(&obj));
}


void McoRecord::updateRecord()
{
    McoDatabase::checkStatus(mco_w_obj_checkpoint(&obj));
}

McoSql::Value* McoRecord::clone(AbstractAllocator* allocator)
{
    McoRecord* rec = new (allocator) McoRecord(fields, trans, &obj);
    rec->allocator = allocator;
    return rec;
}

//
// Struct
//
McoSql::Value* McoStruct::scope()
{
    return _scope;
}

McoSql::Value* McoStruct::clone(AbstractAllocator* allocator)
{
    McoStruct* s = new (allocator) McoStruct(fields, trans, &obj, _scope);
    s->allocator = allocator;
    return s;
}



//
// Reference
//
McoReference::McoReference(Transaction* trans, McoTable* referencedTable, int64_t id)
{
    this->referencedTable = referencedTable;
    this->trans = (McoTransaction*)trans;
    this->id = id;
}

McoSql::Value* McoReference::clone(AbstractAllocator* allocator)
{
    return new (allocator) McoReference(trans, referencedTable, id);
}


Record* McoReference::dereference()
{
    if (referencedTable == NULL)
    {
        MCO_THROW InvalidOperation("McoReference::dereference");
    }
    else
    {
        if (trans->lastDeref.id != id)  {
            mco_external_field_t key;
            key.field_type = MCO_DB_FT_AUTOID;
            *(int64_t*) &key.v.i8 = trans->lastDeref.id = id;
            McoDatabase::checkStatus(mco_w_hash_find_scalar(trans->transaction, (uint2)referencedTable->autoId, &key, &trans->lastDeref.hnd));
        }
        return new McoRecord(referencedTable->_fields, trans, &trans->lastDeref.hnd);
    }
}

int McoReference::compare(McoSql::Value* v)
{
    return v->isNull() ? 1 : id < ((McoReference*)v)->id ?  - 1: id == ((McoReference*)v)->id ? 0 : 1;
}

size_t McoReference::toString(char* buf, size_t bufSize)
{
    IntValue iv(id);
    return iv.toString(buf, bufSize);
}

int64_t McoReference::intValue()
{
    return id;
}

#ifndef NO_FLOATING_POINT
double McoReference::realValue()
{
    return (double)id;
}
#endif


String* McoReference::stringValue()
{
    IntValue iv(id);
    return iv.stringValue();
}

//
// Array
//

McoArray::McoArray(McoField* field, Transaction* trans, mco_objhandle_h hnd)
{
    this->field = field;
    this->trans = trans;
    this->hnd = hnd;
    allocator = MemoryManager::getAllocator();
}

McoSql::Value* McoArray::clone(AbstractAllocator* allocator)
{
    McoArray* arr = new (allocator) McoArray(field, trans, hnd);
    arr->allocator = allocator;
    return arr;
}

McoSql::Value* McoArray::getAt(int index)
{
    McoField* fd = field;
    if ((unsigned)index >= (unsigned)fd->arraySize)
    {
        MCO_THROW IndexOutOfBounds(index, fd->arraySize);
    }
    if ((fd->flags & MCO_DICT_FLDF_NULLABLE) != 0)
    {
        uint1 indicator;
        McoDatabase::checkStatus(mco_w_bit_get2(hnd, fd->indicator_layout.u_offset, fd->indicator_layout.c_offset, fd->indicatorNo, &indicator));
        if (!indicator) {
            MCO_THROW NullValueException();
        }
    }
    uint2 i = (uint2)index;
    uint2 len = (uint2)fd->arraySize;
    switch (fd->mcoType)
    {
        #ifdef MCO_DB_FT_BOOL
            case MCO_DB_FT_BOOL:
                {
                    uint1 value;
                    McoDatabase::checkStatus(mco_w_va_bit_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, &value, len));
                    return BoolValue::create(value != 0);
                }
            #endif
        case MCO_DB_FT_UINT1:
            {
                uint1 value;
                McoDatabase::checkStatus(mco_w_va_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), &value, len));
                return new (allocator) IntValue(value);
            }
        case MCO_DB_FT_OBJVERS:
             /* 2 byte */
        case MCO_DB_FT_UINT2:
            {
                uint2 value;
                McoDatabase::checkStatus(mco_w_va_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), &value, len));
                return new (allocator) IntValue(value);
            }
        case MCO_DB_FT_UINT4:
            {
                uint4 value;
                McoDatabase::checkStatus(mco_w_va_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), &value, len));
                return new (allocator) IntValue(value);
            }
        case MCO_DB_FT_INT1:
            {
                int1 value;
                McoDatabase::checkStatus(mco_w_va_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), &value, len));
                return  (fd->flags & MCO_DICT_FLDF_NUMERIC) ? (McoSql::Value*)new (allocator) NumericValue((int64_t)value, fd->_precision) : (McoSql::Value*)new (allocator) IntValue(value);
            }
        case MCO_DB_FT_INT2:
            {
                int2 value;
                McoDatabase::checkStatus(mco_w_va_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), &value, len));
                return  (fd->flags & MCO_DICT_FLDF_NUMERIC) ? (McoSql::Value*)new (allocator) NumericValue((int64_t)value, fd->_precision) : (McoSql::Value*)new (allocator) IntValue(value);
            }
        case MCO_DB_FT_INT4:
            {
                int4 value;
                McoDatabase::checkStatus(mco_w_va_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), &value, len));
                return (fd->flags & MCO_DICT_FLDF_NUMERIC) ? (McoSql::Value*)new (allocator) NumericValue((int64_t)value, fd->_precision) : (McoSql::Value*)new (allocator) IntValue(value);
            }
            #ifdef UNICODE_SUPPORT
            case MCO_DB_FT_UNICODE_CHARS:
                {
                    UnicodeString* str = UnicodeFixedString::create(fd->fieldSize, allocator);
                    McoDatabase::checkStatus(mco_w_va_nchars_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, fd->fieldSize, (nchar_t*)str->body(), fd->fieldSize + 1, fd->arraySize));
                    convertNcharToWchar(str);
                    return str;
                }
            case MCO_DB_FT_UNICODE_STRING:
                {
                    uint2 len;
                    McoDatabase::checkStatus(mco_w_va_nstring_e_get_len2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i,  &len, fd->arraySize));
                    UnicodeString* str = UnicodeString::create(len, allocator);
                    McoDatabase::checkStatus(mco_w_va_nstring_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, (nchar_t*)str->body(), len + 1, &len, fd->arraySize));
                    convertNcharToWchar(str);
                    return str;
                }
                #ifdef MCO_CFG_WCHAR_SUPPORT
                case MCO_DB_FT_WIDE_CHARS:
                    {
                        UnicodeString* str = UnicodeFixedString::create(fd->fieldSize, allocator);
                        McoDatabase::checkStatus(mco_w_va_wchars_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo,
                                                 i, fd->fieldSize, str->body(), fd->fieldSize + 1, fd->arraySize));
                        return str;
                    }
                case MCO_DB_FT_WCHAR_STRING:
                    {
                        uint2 len;
                        McoDatabase::checkStatus(mco_w_va_wstring_e_get_len2(hnd, fd->layout.u_offset, fd->fieldNo, fd->layout.c_offset, i,  &len, fd->arraySize));
                        UnicodeString* str = UnicodeString::create(len, allocator);
                        McoDatabase::checkStatus(mco_w_va_wstring_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, str->body(), len + 1, &len, fd->arraySize));
                        return str;
                    }
                #endif
            #endif
        case MCO_DB_FT_CHARS:
            {
                String* str = FixedString::create(fd->fieldSize, allocator);
                McoDatabase::checkStatus(mco_w_va_chars_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, fd ->fieldSize, str->body(), fd->fieldSize + 1, fd->arraySize));
                return str;
            }
        case MCO_DB_FT_STRING:
            {
                uint2 len;
                McoDatabase::checkStatus(mco_w_va_string_e_get_len2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, &len, fd->arraySize));
                String* str = String::create(len, allocator);
                McoDatabase::checkStatus(mco_w_va_string_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, str->body(), len + 1, &len, fd->arraySize));
                return str;
            }
        case MCO_DB_FT_REF:
            {
                return  &Null;
            }
            #ifndef NO_FLOATING_POINT
            case MCO_DB_FT_FLOAT:
                {
                    float value;
                    McoDatabase::checkStatus(mco_w_va_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), &value, len));
                    return new (allocator) RealValue(value);
                }
            case MCO_DB_FT_DOUBLE:
                {
                    double value;
                    McoDatabase::checkStatus(mco_w_va_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), &value, len));
                    return new (allocator) RealValue(value);
                }
            #endif
        case MCO_DB_FT_UINT8:
            {
                int64_t value;
                McoDatabase::checkStatus(mco_w_va_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), &value, len));
                return new (allocator) IntValue(value);
            }
        case MCO_DB_FT_AUTOID:
             /* 8 byte */
        case MCO_DB_FT_AUTOOID:
             /* 8 byte */
            {
                int64_t value;
                McoDatabase::checkStatus(mco_w_va_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), &value, len));
                return new (allocator) McoReference(trans, fd->_table, value);
            }
        case MCO_DB_FT_INT8:
            {
                int64_t value;
                McoDatabase::checkStatus(mco_w_va_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), &value, len));
                return (fd->referencedTable != NULL) ? value == 0 ? (McoSql::Value*) &Null: (McoSql::Value*)new (allocator) McoReference(trans, fd->referencedTable, value)
                    : (fd->flags & MCO_DICT_FLDF_NUMERIC) ? (McoSql::Value*)new (allocator) NumericValue(value, fd->_precision) : (McoSql::Value*)new (allocator) IntValue(value);
            }
        case MCO_DB_FT_DATE:
        case MCO_DB_FT_TIME:
            {
                uint4 value;
                McoDatabase::checkStatus(mco_w_va_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), &value, len));
                return new (allocator) DateTime((time_t)value);
            }
        case MCO_DB_FT_DATETIME:
            {
                int64_t value;
                McoDatabase::checkStatus(mco_w_va_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), &value, len));
                return new (allocator) DateTime((time_t)value);
            }
        case MCO_DB_FT_STRUCT:
            {
                mco_objhandle_t s;
                McoDatabase::checkStatus(mco_w_va_struct_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, &s, i, fd->structSizeU, fd->structSizeC, len));
                return new (allocator) McoStruct(fd->_components, trans, &s, this);
            }
        default:
            MCO_THROW InvalidOperation(String::format("Operations with type %s are not supported", typeMnemonic[fd->mcoType])->cstr());    
    }
    return NULL;
}

McoSql::Value* McoArray::updateAt(int index)
{
    McoField* fd = field;
    if (fd->mcoType == MCO_DB_FT_STRUCT)
    {
        mco_objhandle_t s;
        McoDatabase::checkStatus(mco_w_va_struct_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, &s, (uint2)index,
                                 fd->structSizeU, fd->structSizeC, fd->indexAware, fd->arraySize));
        return new (allocator) McoStruct(fd->_components, trans, &s, this);
    }
    else
    {
        MCO_THROW InvalidOperation("McoArray::updateAt");
    }
}

void McoArray::setAt(int index, McoSql::Value* v)
{
    McoField* fd = field;
    if ((unsigned)index >= (unsigned)fd->arraySize)
    {
        MCO_THROW IndexOutOfBounds(index, fd->arraySize);
    }
    if (v == NULL)
    {
        v = &Null;
    }
    if ((fd->flags & MCO_DICT_FLDF_NULLABLE) != 0)
    {
        McoDatabase::checkStatus(mco_w_bit_put2(hnd, fd->indicator_layout.u_offset, fd->indicator_layout.c_offset, fd->indicatorNo, fd->indexAware, 1));
    }
    uint2 i = (uint2)index;
    uint2 len = (uint2)fd->arraySize;
    MCO_RET ret = MCO_S_OK;
    switch (fd->mcoType)
    {
        #ifdef MCO_DB_FT_BOOL
            case MCO_DB_FT_BOOL:
                {
                    uint1 value = (uint1)(v->isTrue() ? 1 : 0);
                    ret = mco_w_va_bit_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, fd->indexAware, &value, len);
                    break;
                }
            #endif
        case MCO_DB_FT_UINT1:
            {
                uint1 value = (uint1)v->intValue();
                ret = mco_w_va_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd ->indexAware, &value, len);
                break;
            }
        case MCO_DB_FT_OBJVERS:
             /* 2 byte */
        case MCO_DB_FT_UINT2:
            {
                uint2 value = (uint2)v->intValue();
                ret = mco_w_va_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd
                                             ->indexAware, &value, len);
                break;
            }
        case MCO_DB_FT_UINT4:
            {
                uint4 value = (uint4)v->intValue();
                ret = mco_w_va_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd
                                             ->indexAware, &value, len);
                break;
            }
        case MCO_DB_FT_DATE:
        case MCO_DB_FT_TIME:
            {
                uint4 value = (uint4)v->timeValue();
                ret = mco_w_va_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd
                                             ->indexAware, &value, len);
                break;
            }
        case MCO_DB_FT_DATETIME:
            {
                int64_t value = (int64_t)v->timeValue();
                ret = mco_w_va_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd
                                             ->indexAware, &value, len);
                break;
            }
        case MCO_DB_FT_INT1:
            {
                int1 value = (int1)v->intValue(fd->_precision);
                ret = mco_w_va_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd
                                             ->indexAware, &value, len);
                break;
            }
        case MCO_DB_FT_INT2:
            {
                int2 value = (int2)v->intValue(fd->_precision);
                ret = mco_w_va_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd
                                             ->indexAware, &value, len);
                break;
            }
        case MCO_DB_FT_INT4:
            {
                int4 value = (int4)v->intValue(fd->_precision);
                ret = mco_w_va_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd
                                             ->indexAware, &value, len);
                break;
            }
            #ifdef UNICODE_SUPPORT
            case MCO_DB_FT_UNICODE_CHARS:
                {
                    UnicodeString* str = v->unicodeStringValue();
                    ret = mco_w_va_nchars_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, fd->fieldSize,
                                                 convertWcharToNchar(str), str->size(), fd->indexAware, fd->arraySize);
                    break;
                }
            case MCO_DB_FT_UNICODE_STRING:
                {
                    UnicodeString* str = v->unicodeStringValue();
                    ret = mco_w_va_nstring_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, convertWcharToNchar
                                                  (str), str->size(), fd->indexAware, fd->arraySize);
                    break;
                }
                #ifdef MCO_CFG_WCHAR_SUPPORT
                case MCO_DB_FT_WIDE_CHARS:
                    {
                        UnicodeString* str = v->unicodeStringValue();
                        ret = mco_w_va_wchars_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, fd->fieldSize,
                                                     str->body(), str->size(), fd->indexAware, fd->arraySize);
                        break;
                    }
                case MCO_DB_FT_WCHAR_STRING:
                    {
                        UnicodeString* str = v->unicodeStringValue();
                        ret = mco_w_va_wstring_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, str->body(),
                                                      str->size(), fd->indexAware, fd->arraySize);
                        break;
                    }
                #endif
            #endif
        case MCO_DB_FT_CHARS:
            {
                String* str = v->stringValue();
                ret = mco_w_va_chars_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, fd->fieldSize, str->body()
                                            , str->size(), fd->indexAware, fd->arraySize);
                break;
            }
        case MCO_DB_FT_STRING:
            {
                String* str = v->stringValue();
                ret = mco_w_va_string_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, str->body(), str->size(),
                                             fd->indexAware, fd->arraySize);
                break;
            }
        case MCO_DB_FT_REF:
            break;
            #ifndef NO_FLOATING_POINT
            case MCO_DB_FT_FLOAT:
                {
                    float value = (float)v->realValue();
                    ret = mco_w_va_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd ->indexAware, &value, len);
                    break;
                }
            case MCO_DB_FT_DOUBLE:
                {
                    double value = v->realValue();
                    ret = mco_w_va_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd ->indexAware, &value, len);
                    break;
                }
            #endif
        case MCO_DB_FT_INT8:
        case MCO_DB_FT_UINT8:
        case MCO_DB_FT_AUTOID:
        case MCO_DB_FT_AUTOOID:
             /* 8 byte */
            {
                int64_t value = v->intValue(fd->_precision);
                ret = mco_w_va_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd ->indexAware, &value, len);
                break;
            }
        case MCO_DB_FT_STRUCT:
             /* not used in indexed field */
            MCO_THROW InvalidOperation("McoArray::setAt(struct)");
        default:
            MCO_THROW InvalidOperation(String::format("Operations with type %s are not supported", typeMnemonic[fd->mcoType])->cstr());    
    }
    McoDatabase::checkStatus(ret);
}

int McoArray::size()
{
    return field->arraySize;
}

void McoArray::setSize(int newSize)
{ 
    if (newSize != field->arraySize) { 
        MCO_THROW InvalidOperation("McoArray::setSize");
    }
}

void McoArray::getBody(void* dst, int offs, int len)
{
    McoField* fd = field;
    switch (fd->mcoType)
    {
        case MCO_DB_FT_CHARS:
        case MCO_DB_FT_STRING:
        case MCO_DB_FT_UNICODE_CHARS:
        case MCO_DB_FT_UNICODE_STRING:
        case MCO_DB_FT_WIDE_CHARS:
        case MCO_DB_FT_WCHAR_STRING:
        case MCO_DB_FT_STRUCT:
             /* not used in indexed field */
            MCO_THROW InvalidOperation("McoArray::getBody");
        default:
          if ((fd->flags & MCO_DICT_FLDF_NULLABLE) != 0)
          {
              uint1 indicator;
              McoDatabase::checkStatus(mco_w_bit_get2(hnd, fd->indicator_layout.u_offset, fd->indicator_layout.c_offset, fd->indicatorNo, &indicator));
              if (!indicator) {
                  MCO_THROW NullValueException();
              }
          }
          if (fd->mcoType == MCO_DB_FT_BOOL) {
              int byteLen = (len + 7) >> 3;
              char buf[1024];
              char* src = buf;
              if (sizeof(buf)*8 < (size_t)len) {
                  src = (char*)MemoryManager::allocator->allocate(byteLen);
              }
              McoDatabase::checkStatus(mco_w_va_bit_e_getrange2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, offs,
                                                                len, src, fd->arraySize));
              for (int i = 0; i < len; i++) {
                  *((bool*)dst + i) = (src[i >> 3] & (1 << (i & 7))) != 0;
              }
          } else {
              McoDatabase::checkStatus(mco_w_va_simple_e_getrange2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, offs,
                                                               fd->fieldSize, len, dst, fd->arraySize, 0));
          }
    }
}

void McoArray::setBody(void* src, int offs, int len)
{
    McoField* fd = field;
    switch (fd->mcoType)
    {
        case MCO_DB_FT_CHARS:
        case MCO_DB_FT_STRING:
        case MCO_DB_FT_UNICODE_CHARS:
        case MCO_DB_FT_UNICODE_STRING:
        case MCO_DB_FT_WIDE_CHARS:
        case MCO_DB_FT_WCHAR_STRING:
        case MCO_DB_FT_STRUCT:
             /* not used in indexed field */
            MCO_THROW InvalidOperation("McoArray::setBody");
        default:
            if ((fd->flags & MCO_DICT_FLDF_NULLABLE) != 0)
            {
                if (src != NULL) {
                    McoDatabase::checkStatus(mco_w_bit_put2(hnd, fd->indicator_layout.u_offset, fd->indicator_layout.c_offset, fd->indicatorNo, fd->indexAware, 1));
                } else {
                    McoDatabase::checkStatus(mco_w_bit_put2(hnd, fd->indicator_layout.u_offset, fd->indicator_layout.c_offset, fd->indicatorNo, fd->indexAware, 0));
                    return;
                }
            }
            if (fd->mcoType == MCO_DB_FT_BOOL) {
                int byteLen = (len + 7) >> 3;
                char buf[1024];
                char* dst = buf;
                if (sizeof(buf)*8 < (size_t)len) {
                    dst = (char*)MemoryManager::allocator->allocate(byteLen);
                }
                mco_memnull(dst, byteLen);
                for (int i = 0; i < len; i++) {
                    if (*((bool*)src + i)) {
                        dst[i >> 3] |= 1 << (i & 7);
                    }
                }
                McoDatabase::checkStatus(mco_w_va_bit_e_putrange2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, offs,
                                                                  len, fd->indexAware, dst, fd->arraySize));
            } else {
                McoDatabase::checkStatus(mco_w_va_simple_e_putrange2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, offs,
                                                                     fd->fieldSize, len, fd->indexAware, src, fd->arraySize, 0));
            }
    }
}

Type McoArray::getElemType() const
{
    return (field->flags & MCO_DICT_FLDF_NUMERIC) ? tpNumeric : mco2sql[field->mcoType];
}


int McoArray::getElemSize() const
{
    return field->fieldSize;
}


//
// Vector
//

McoVector::McoVector(McoField* field, Transaction* trans, mco_objhandle_h hnd)
{
    this->field = field;
    this->trans = trans;
    this->hnd = hnd;
    allocator = MemoryManager::getAllocator();
}


McoSql::Value* McoVector::clone(AbstractAllocator* allocator)
{
    McoVector* vec = new (allocator) McoVector(field, trans, hnd);
    vec->allocator = allocator;
    return vec;
}

McoSql::Value* McoVector::getAt(int index)
{
    McoField* fd = field;
    uint2 i = (uint2)index;
    if ((fd->flags & MCO_DICT_FLDF_NULLABLE) != 0)
    {
        uint1 indicator;
        McoDatabase::checkStatus(mco_w_bit_get2(hnd, fd->indicator_layout.u_offset, fd->indicator_layout.c_offset, fd->indicatorNo, &indicator));
        if (!indicator) {
            MCO_THROW NullValueException();
        }
    }
    switch (fd->mcoType)
    {
        #ifdef MCO_DB_FT_BOOL
            case MCO_DB_FT_BOOL:
                {
                    uint1 value;
                    McoDatabase::checkStatus(mco_w_v_bit_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, &value));
                    return BoolValue::create(value != 0);
                }
            #endif
        case MCO_DB_FT_UINT1:
            {
                uint1 value;
                McoDatabase::checkStatus(mco_w_v_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof (value), &value));
                return new (allocator) IntValue(value);
            }
        case MCO_DB_FT_OBJVERS:
             /* 2 byte */
        case MCO_DB_FT_UINT2:
            {
                uint2 value;
                McoDatabase::checkStatus(mco_w_v_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof (value), &value));
                return new (allocator) IntValue(value);
            }
        case MCO_DB_FT_UINT4:
            {
                uint4 value;
                McoDatabase::checkStatus(mco_w_v_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof (value), &value));
                return new (allocator) IntValue(value);
            }
        case MCO_DB_FT_INT1:
            {
                int1 value;
                McoDatabase::checkStatus(mco_w_v_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof (value), &value));
                return (fd->flags & MCO_DICT_FLDF_NUMERIC) ? (McoSql::Value*)new (allocator) NumericValue((int64_t)value, fd->_precision) : (McoSql::Value*)new (allocator) IntValue(value);
            }
        case MCO_DB_FT_INT2:
            {
                int2 value;
                McoDatabase::checkStatus(mco_w_v_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof (value), &value));
                return (fd->flags & MCO_DICT_FLDF_NUMERIC) ? (McoSql::Value*)new (allocator) NumericValue((int64_t)value, fd->_precision) : (McoSql::Value*)new (allocator) IntValue(value);
            }
        case MCO_DB_FT_INT4:
            {
                int4 value;
                McoDatabase::checkStatus(mco_w_v_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof (value), &value));
                return (fd->flags & MCO_DICT_FLDF_NUMERIC) ? (McoSql::Value*)new (allocator) NumericValue((int64_t)value, fd->_precision) : (McoSql::Value*)new (allocator) IntValue(value);
            }
        case MCO_DB_FT_CHARS:
            {
                String* str = FixedString::create(fd->fieldSize, allocator);
                McoDatabase::checkStatus(mco_w_v_chars_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, fd ->fieldSize, str->body(), fd->fieldSize + 1));
                return str;
            }
        case MCO_DB_FT_STRING:
            {
                uint2 len;
                McoDatabase::checkStatus(mco_w_v_string_e_get_len2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, &len));
                String* str = String::create(len, allocator);
                McoDatabase::checkStatus(mco_w_v_string_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, str ->body(), len + 1, &len));
                return str;
            }
            #ifdef UNICODE_SUPPORT
            case MCO_DB_FT_UNICODE_CHARS:
                {
                    UnicodeString* str = UnicodeFixedString::create(fd->fieldSize, allocator);
                    McoDatabase::checkStatus(mco_w_v_nchars_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, fd ->fieldSize, (nchar_t*)str->body(), fd->fieldSize + 1));
                    convertNcharToWchar(str);
                    return str;
                }
            case MCO_DB_FT_UNICODE_STRING:
                {
                    uint2 len;
                    McoDatabase::checkStatus(mco_w_v_nstring_e_get_len2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, &len));
                    UnicodeString* str = UnicodeString::create(len, allocator);
                    McoDatabase::checkStatus(mco_w_v_nstring_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, (nchar_t*)str->body(), len + 1, &len));
                    convertNcharToWchar(str);
                    return str;
                }
                #ifdef MCO_CFG_WCHAR_SUPPORT
                case MCO_DB_FT_WIDE_CHARS:
                    {
                        UnicodeString* str = UnicodeFixedString::create(fd->fieldSize, allocator);
                        McoDatabase::checkStatus(mco_w_v_wchars_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, fd->fieldSize, str->body(), fd->fieldSize + 1));
                        return str;
                    }
                case MCO_DB_FT_WCHAR_STRING:
                    {
                        uint2 len;
                        McoDatabase::checkStatus(mco_w_v_wstring_e_get_len2(hnd, fd->layout.u_offset, fd->fieldNo, fd ->layout.c_offset, i, &len));
                        UnicodeString* str = UnicodeString::create(len, allocator);
                        McoDatabase::checkStatus(mco_w_v_wstring_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, str->body(), len + 1, &len));
                        return str;
                    }
                #endif
            #endif
        case MCO_DB_FT_REF:
            {
                return  &Null;
            }
            #ifndef NO_FLOATING_POINT
            case MCO_DB_FT_FLOAT:
                {
                    float value;
                    McoDatabase::checkStatus(mco_w_v_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), &value));
                    return new (allocator) RealValue(value);
                }
            case MCO_DB_FT_DOUBLE:
                {
                    double value;
                    McoDatabase::checkStatus(mco_w_v_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), &value));
                    return new (allocator) RealValue(value);
                }
            #endif
        case MCO_DB_FT_UINT8:
            {
                int64_t value;
                McoDatabase::checkStatus(mco_w_v_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof (value), &value));
                return new (allocator) IntValue(value);
            }
        case MCO_DB_FT_AUTOID:
             /* 8 byte */
        case MCO_DB_FT_AUTOOID:
             /* 8 byte */
            {
                int64_t value;
                McoDatabase::checkStatus(mco_w_v_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof (value), &value));
                return new (allocator) McoReference(trans, fd->_table, value);
            }
        case MCO_DB_FT_INT8:
            {
                int64_t value;
                McoDatabase::checkStatus(mco_w_v_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof (value), &value));
                return (fd->referencedTable != NULL) ? value == 0 ? (McoSql::Value*) &Null: (McoSql::Value*)new (allocator) McoReference(trans, fd->referencedTable, value)
                    : (fd->flags & MCO_DICT_FLDF_NUMERIC) ? (McoSql::Value*)new (allocator) NumericValue(value, fd->_precision) : (McoSql::Value*)new (allocator) IntValue(value);
            }
        case MCO_DB_FT_DATE:
        case MCO_DB_FT_TIME:
            {
                uint4 value;
                McoDatabase::checkStatus(mco_w_v_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof (value), &value));
                return new (allocator) DateTime((time_t)value);
            }
        case MCO_DB_FT_DATETIME:
            {
                int64_t value;
                McoDatabase::checkStatus(mco_w_v_simple_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof (value), &value));
                return new (allocator) DateTime((time_t)value);
            }
        case MCO_DB_FT_STRUCT:
            {
                mco_objhandle_t s;
                MCO_RET ret = mco_w_v_struct_e_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, &s, i);
                if (ret == MCO_E_EMPTYVECTOREL)
                {
                    return &Null;
                }
                McoDatabase::checkStatus(ret);
                return new (allocator) McoStruct(fd->_components, trans, &s, this);
            }
        default:
            MCO_THROW InvalidOperation(String::format("Operations with type %s are not supported", typeMnemonic[fd->mcoType])->cstr());    
    }
    return NULL;
}

McoSql::Value* McoVector::updateAt(int index)
{
    McoField* fd = field;
    if (fd->mcoType == MCO_DB_FT_STRUCT)
    {
        mco_objhandle_t s;
        McoDatabase::checkStatus(mco_w_v_struct_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, &s, (uint2)index,
                                 fd->structSizeU, fd->structSizeC, fd->structAlignU, fd->structAlignC, fd->indexAware));
        return new (allocator) McoStruct(fd->_components, trans, &s, this);
    }
    else
    {
        MCO_THROW InvalidOperation("McoVector::updateAt");
    }
}


void McoVector::setAt(int index, McoSql::Value* v)
{
    McoField* fd = field;
    uint2 i = (uint2)index;
    MCO_RET ret = MCO_S_OK;
    if (v == NULL)
    {
        v = &Null;
    }
    if ((fd->flags & MCO_DICT_FLDF_NULLABLE) != 0)
    {
        uint1 indicator;
        McoDatabase::checkStatus(mco_w_bit_get2(hnd, fd->indicator_layout.u_offset, fd->indicator_layout.c_offset, fd->indicatorNo, &indicator));
        if (!indicator) {
            MCO_THROW NullValueException();
        }
    }

    switch (fd->mcoType)
    {
        #ifdef MCO_DB_FT_BOOL
            case MCO_DB_FT_BOOL:
                {
                    uint1 value = (uint1)(v->isTrue() ? 1 : 0);
                    ret = mco_w_v_bit_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, fd->indexAware, &value);
                    break;
                }
            #endif
        case MCO_DB_FT_UINT1:
            {
                uint1 value = (uint1)v->intValue();
                ret = mco_w_v_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd
                                            ->indexAware, &value);
                break;
            }
        case MCO_DB_FT_OBJVERS:
             /* 2 byte */
        case MCO_DB_FT_UINT2:
            {
                uint2 value = (uint2)v->intValue();
                ret = mco_w_v_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd
                                            ->indexAware, &value);
                break;
            }
        case MCO_DB_FT_UINT4:
            {
                uint4 value = (uint4)v->intValue();
                ret = mco_w_v_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd
                                            ->indexAware, &value);
                break;
            }
        case MCO_DB_FT_DATE:
        case MCO_DB_FT_TIME:
            {
                uint4 value = (uint4)v->timeValue();
                ret = mco_w_v_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd
                                            ->indexAware, &value);
                break;
            }
        case MCO_DB_FT_DATETIME:
            {
                int64_t value = (int64_t)v->timeValue();
                ret = mco_w_v_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd
                                            ->indexAware, &value);
                break;
            }
         case MCO_DB_FT_INT1:
            {
                int1 value = (int1)v->intValue(fd->_precision);
                ret = mco_w_v_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd
                                            ->indexAware, &value);
                break;
            }
        case MCO_DB_FT_INT2:
            {
                int2 value = (int2)v->intValue(fd->_precision);
                ret = mco_w_v_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd
                                            ->indexAware, &value);
                break;
            }
        case MCO_DB_FT_INT4:
            {
                int4 value = (int4)v->intValue(fd->_precision);
                ret = mco_w_v_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd
                                            ->indexAware, &value);
                break;
            }
            #ifdef UNICODE_SUPPORT
            case MCO_DB_FT_UNICODE_CHARS:
                {
                    UnicodeString* str = v->unicodeStringValue();
                    ret = mco_w_v_nchars_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, fd->fieldSize,
                                                convertWcharToNchar(str), str->size(), fd->indexAware);
                    break;
                }
            case MCO_DB_FT_UNICODE_STRING:
                {
                    UnicodeString* str = v->unicodeStringValue();
                    ret = mco_w_v_nstring_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, convertWcharToNchar
                                                 (str), str->size(), fd->indexAware);
                    break;
                }
                #ifdef MCO_CFG_WCHAR_SUPPORT
                case MCO_DB_FT_WIDE_CHARS:
                    {
                        UnicodeString* str = v->unicodeStringValue();
                        ret = mco_w_v_wchars_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, fd->fieldSize,
                                                    str->body(), str->size(), fd->indexAware);
                        break;
                    }
                case MCO_DB_FT_WCHAR_STRING:
                    {
                        UnicodeString* str = v->unicodeStringValue();
                        ret = mco_w_v_wstring_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, str->body(), str
                                                     ->size(), fd->indexAware);
                        break;
                    }
                #endif
            #endif
        case MCO_DB_FT_CHARS:
            {
                String* str = v->stringValue();
                ret = mco_w_v_chars_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, fd->fieldSize, str->body(),
                                           str->size(), fd->indexAware);
                break;
            }
        case MCO_DB_FT_STRING:
            {
                String* str = v->stringValue();
                ret = mco_w_v_string_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, str->body(), str->size(),
                                            fd->indexAware);
                break;
            }
        case MCO_DB_FT_REF:
            break;
            #ifndef NO_FLOATING_POINT
            case MCO_DB_FT_FLOAT:
                {
                    float value = (float)v->realValue();
                    ret = mco_w_v_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd
                                                ->indexAware, &value);
                    break;
                }
            case MCO_DB_FT_DOUBLE:
                {
                    double value = v->realValue();
                    ret = mco_w_v_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd
                                                ->indexAware, &value);
                    break;
                }
            #endif
        case MCO_DB_FT_INT8:
        case MCO_DB_FT_UINT8:
        case MCO_DB_FT_AUTOID:
        case MCO_DB_FT_AUTOOID:
             /* 8 byte */
            {
                int64_t value = v->intValue(fd->_precision);
                ret = mco_w_v_simple_e_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, i, sizeof(value), fd ->indexAware, &value);
                break;
            }
        case MCO_DB_FT_STRUCT:
             /* not used in indexed field */
            MCO_THROW InvalidOperation("McoVector::setAt(struct)");
        default:
            MCO_THROW InvalidOperation(String::format("Operations with type %s are not supported", typeMnemonic[fd->mcoType])->cstr());    
    }
    McoDatabase::checkStatus(ret);
}

int McoVector::size()
{
    uint2 len;
    McoField* fd = field;
    if ((fd->flags & MCO_DICT_FLDF_NULLABLE) != 0)
    {
        uint1 indicator;
        McoDatabase::checkStatus(mco_w_bit_get2(hnd, fd->indicator_layout.u_offset, fd->indicator_layout.c_offset, fd->indicatorNo, &indicator));
        if (!indicator) {
            MCO_THROW NullValueException();
        }
    }
    McoDatabase::checkStatus(mco_w_vector_size_get2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, &len));
    return len;
}

void McoVector::setSize(int newSize)
{
    McoField* fd = field;
    MCO_RET ret;
    if ((fd->flags & MCO_DICT_FLDF_NULLABLE) != 0)
    {
        McoDatabase::checkStatus(mco_w_bit_put2(hnd, fd->indicator_layout.u_offset, fd->indicator_layout.c_offset, fd->indicatorNo, fd->indexAware, 1));
    }
    switch (fd->mcoType)
    {
        case MCO_DB_FT_CHARS:
            ret = mco_w_v_chars_size_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, (uint2)newSize, fd->fieldSize,
                                          fd->indexAware);
            break;
        case MCO_DB_FT_STRING:
            ret = mco_w_v_string_size_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, (uint2)newSize, fd
                                           ->indexAware);
            break;
            #ifdef UNICODE_SUPPORT
            case MCO_DB_FT_UNICODE_CHARS:
                ret = mco_w_v_nchars_size_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, (uint2)newSize, fd ->fieldSize, fd->indexAware);
                break;
            case MCO_DB_FT_UNICODE_STRING:
                ret = mco_w_v_nstring_size_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, (uint2)newSize, fd ->indexAware);
                break;
                #ifdef MCO_CFG_WCHAR_SUPPORT
                case MCO_DB_FT_WIDE_CHARS:
                    ret = mco_w_v_wchars_size_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, (uint2)newSize, fd ->fieldSize, fd->indexAware);
                    break;
                case MCO_DB_FT_WCHAR_STRING:
                    ret = mco_w_v_wstring_size_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, (uint2)newSize, fd ->indexAware);
                    break;
                #endif
            #endif
        case MCO_DB_FT_STRUCT:
            ret = mco_w_v_struct_size_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, (uint2)newSize, fd ->indexAware);
            break;
        default:
            ret = mco_w_v_simple_size_put2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, (uint2)newSize, fd->fieldSize,
                                           fd->indexAware);
    }
    McoDatabase::checkStatus(ret);
}

void McoVector::getBody(void* dst, int offs, int len)
{
    McoField* fd = field;

    switch (fd->mcoType)
    {
        case MCO_DB_FT_CHARS:
        case MCO_DB_FT_STRING:
        case MCO_DB_FT_UNICODE_CHARS:
        case MCO_DB_FT_UNICODE_STRING:
        case MCO_DB_FT_WIDE_CHARS:
        case MCO_DB_FT_WCHAR_STRING:
        case MCO_DB_FT_STRUCT:
             /* not used in indexed field */
            MCO_THROW InvalidOperation("McoVector::getBody");
        default:
          if ((fd->flags & MCO_DICT_FLDF_NULLABLE) != 0)
          {
              uint1 indicator;
              McoDatabase::checkStatus(mco_w_bit_get2(hnd, fd->indicator_layout.u_offset, fd->indicator_layout.c_offset, fd->indicatorNo, &indicator));
              if (!indicator) {
                  MCO_THROW NullValueException();
              }
          }
          if (fd->mcoType == MCO_DB_FT_BOOL) {
              int byteLen = (len + 7) >> 3;
              char buf[1024];
              char* src = buf;
              if (sizeof(buf)*8 < (size_t)len) {
                  src = (char*)MemoryManager::allocator->allocate(byteLen);
              }
              McoDatabase::checkStatus(mco_w_v_bit_e_getrange2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, offs,
                                                               len, src));
              for (int i = 0; i < len; i++) {
                  *((bool*)dst + i) = (src[i >> 3] & (1 << (i & 7))) != 0;
              }
          } else {
              McoDatabase::checkStatus(mco_w_v_simple_e_getrange2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, offs, fd->fieldSize, len, dst, 0));
          }
    }
}

void McoVector::setBody(void* src, int offs, int len)
{
    McoField* fd = field;
    switch (fd->mcoType)
    {
        case MCO_DB_FT_CHARS:
        case MCO_DB_FT_STRING:
        case MCO_DB_FT_UNICODE_CHARS:
        case MCO_DB_FT_UNICODE_STRING:
        case MCO_DB_FT_WIDE_CHARS:
        case MCO_DB_FT_WCHAR_STRING:
        case MCO_DB_FT_STRUCT:
             /* not used in indexed field */
            MCO_THROW InvalidOperation("McoVector::getBody");
        default:
          if ((fd->flags & MCO_DICT_FLDF_NULLABLE) != 0)
          {
              uint1 indicator;
              McoDatabase::checkStatus(mco_w_bit_get2(hnd, fd->indicator_layout.u_offset, fd->indicator_layout.c_offset, fd->indicatorNo, &indicator));
              if (!indicator) {
                  MCO_THROW NullValueException();
              }
          }
          if (fd->mcoType == MCO_DB_FT_BOOL) {
              int byteLen = (len + 7) >> 3;
              char buf[1024];
              char* dst = buf;
              if (sizeof(buf)*8 < (size_t)len) {
                  dst = (char*)MemoryManager::allocator->allocate(byteLen);
              }
              mco_memnull(dst, byteLen);
              for (int i = 0; i < len; i++) {
                  if (*((bool*)src + i)) {
                      dst[i >> 3] |= 1 << (i & 7);
                  }
              }
              McoDatabase::checkStatus(mco_w_v_bit_e_putrange2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, offs,
                                                                len, fd->indexAware, dst));
          } else {
              McoDatabase::checkStatus(mco_w_v_simple_e_putrange2(hnd, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, offs, fd ->fieldSize, len, fd->indexAware, src, 0));
          }
    }
}


int McoVector::getElemSize()const
{
    return field->fieldSize;
}

Type McoVector::getElemType() const
{
    return (field->flags & MCO_DICT_FLDF_NUMERIC) ? tpNumeric : mco2sql[field->mcoType];
}



//
// BLOB
//
McoBlob::McoBlob(McoField* field, mco_objhandle_h hnd)
{
    this->field = field;
    obj = * hnd;
    pos = 0;
    mco_tmgr_init_handle(&obj);
}

McoSql::Value* McoBlob::clone(AbstractAllocator* allocator)
{
    return new (allocator) McoBlob(field, &obj);
}

int McoBlob::available()
{
    mco_size32_t result;
    MCO_RET rc = mco_w_blob_len2(&obj, field->layout.u_offset, field->layout.c_offset, field->fieldNo, &result);
    if (rc == MCO_E_EMPTYBLOB)
    {
        return 0;
    }
    McoDatabase::checkStatus(rc);
    return (int)result - pos;
}

int McoBlob::get(void* buffer, int size)
{
    uint4 len;
    McoField* fd = field;
    if ((fd->flags & MCO_DICT_FLDF_NULLABLE) != 0)
    {
        uint1 indicator;
        McoDatabase::checkStatus(mco_w_bit_get2(&obj, fd->indicator_layout.u_offset, fd->indicator_layout.c_offset, fd->indicatorNo, &indicator));
        if (!indicator) {
            MCO_THROW NullValueException();
        }
    }
    MCO_RET rc = mco_w_blob_get2(&obj, fd->layout.u_offset, fd->layout.c_offset, fd->fieldNo, pos, (char*)buffer, size, &len);
    if (rc == MCO_E_EMPTYBLOB)
    {
        return 0;
    }
    McoDatabase::checkStatus(rc);
    pos += len;
    return (int)len;
}

void McoBlob::append(void const* buffer, int size)
{
    MCO_RET rc = mco_w_blob_append2(&obj, field->layout.u_offset, field->layout.c_offset, field->fieldNo, (char*)buffer, size);
    if (rc == MCO_E_EMPTYBLOB)
    {
        rc = mco_w_blob_put2(&obj, field->layout.u_offset, field->layout.c_offset, field->fieldNo, (char*)buffer, size);
    }
    McoDatabase::checkStatus(rc);
}

void McoBlob::truncate()
{
    McoDatabase::checkStatus(mco_w_blob_put2(&obj, field->layout.u_offset, field->layout.c_offset, field->fieldNo, NULL, 0));
}

void McoBlob::reset()
{
    pos = 0;
}

//
// Key
//

McoKey::McoKey(McoField* field, SortOrder order)
{
    _field = field;
    _order = order;
}

SortOrder McoKey::order()
{
    return _order;
}

Field* McoKey::field()
{
    return _field;
}

//
// Index
//

McoIndex::McoIndex(McoTable* table, int indexCode, mco_database_h db)
{
    this->indexCode = indexCode;
    _table = table;
    _name = String::create(MCO_DICT_INDEX_NAME(db, indexCode));
    mco_dict_index_t* index = MCO_DICT_INDEX_MAP(db, indexCode);
    _keys = Vector < McoKey > ::create(index->numof_fields);
    _unique = (index->flags & MCO_DB_INDF_UNIQUE) != 0;
    _flags = index->flags;
    switch (index->impl_no)
    {
        case MCO_INDEX_BTREE_INMEM:
        case MCO_INDEX_BTREE_DISK:
          type = BTREE;
          break;

        case MCO_INDEX_HASH_INMEM:
            type = HASH;
            break;

        case MCO_INDEX_KDTREE_INMEM:
        case MCO_INDEX_KDTREE_DISK:
            type = BTREE;
            break;

        case MCO_INDEX_RTREE_INMEM:
        case MCO_INDEX_RTREE_DISK:
            type = RTREE;
            break;

        case MCO_INDEX_TRIGRAM_INMEM:
        case MCO_INDEX_TRIGRAM_DISK:
            type = TRIGRAM;
            break;

        #ifdef MCO_CFG_PATRICIA_SUPPORT
        case MCO_INDEX_PATRICIA_INMEM:
        case MCO_INDEX_PATRICIA_DISK:
            type = PTREE;
            break;
        #endif
    }

    for (int i = 0; i < _keys->length; i++)
    {
        mco_dict_index_field_t* field = MCO_DICT_INDEX_FIELD(db, index, i);
        McoField* fd = table->findFieldByOffset((_flags & (MCO_DB_INDF_VTYPE_BASED|MCO_DB_INDF_ATYPE_BASED)) ? index->vect_field_offset : field->field_offset);
        assert(fd != NULL);
        fd->indexAware = 1; // field->fld_idx;
        _keys->items[i] = new McoKey(fd, type != BTREE ? UNSPECIFIED_ORDER: (field->fld_flags &MCO_DB_INDFLD_DESCENDING)
                                     == 0 ? ASCENT_ORDER: DESCENT_ORDER);
    }
}

void McoIndex::updateStatistic(Transaction* trans, Record* statRec)
{
    mco_index_stat_t stat;
    McoDatabase::checkStatus(mco_index_stat_get(((McoTransaction*)trans)->transaction, indexCode, &stat));
    ((IntValue*)statRec->get(2))->val = stat.keys_num;
    ((IntValue*)statRec->get(3))->val = stat.pages_num;
    ((IntValue*)statRec->get(4))->val = stat.avg_cmp;
    ((IntValue*)statRec->get(5))->val = stat.max_cmp;
}

String* McoIndex::name()
{
    return _name;
}

Iterator < Key > * McoIndex::keys()
{
    return (Iterator < Key > *)_keys->iterator();
}

int McoIndex::nKeys()
{
    return _keys->length;
}

void McoIndex::setKeys(bool low, mco_external_field_t ek[], int nRanges, Range ranges[])
{
    for (int i = 0; i < nRanges; i++)
    {
        McoField* fd = _keys->items[i]->_field;
        McoSql::Value* v = low ? ranges[i].lowBound: ranges[i].highBound;
        if (fd->sqlType == tpArray) {
            ek[i].field_type  = 0;
            ek[i].v.ptr_size = sizeof(void*);
            ek[i].ptr = (char*)((Array*)v)->pointer();
            return;
        }
        switch (ek[i].field_type = fd->mcoType)
        {
            #ifdef MCO_DB_FT_BOOL
                case MCO_DB_FT_BOOL:
                    ek[i].v.u1 = (uint1)(v->isTrue() ? 1 : 0);
                    break;
                #endif
            case MCO_DB_FT_UINT1:
                ek[i].v.u1 = (uint1)v->intValue();
                break;
            case MCO_DB_FT_OBJVERS:
                 /* 2 byte */
            case MCO_DB_FT_UINT2:
                ek[i].v.u2 = (uint2)v->intValue();
                break;
            case MCO_DB_FT_DATE:
            case MCO_DB_FT_TIME:
                ek[i].v.u4 = (uint4)v->timeValue();
                break;
            case MCO_DB_FT_DATETIME:
              *(int64_t*)&ek[i].v.u8 = (int64_t)v->timeValue();
                break;
            case MCO_DB_FT_UINT4:
                ek[i].v.u4 = (uint4)v->intValue();
                break;
            case MCO_DB_FT_INT1:
                ek[i].v.i1 = (int1)v->intValue(fd->_precision);
                break;
            case MCO_DB_FT_INT2:
                ek[i].v.i2 = (int2)v->intValue(fd->_precision);
                break;
            case MCO_DB_FT_INT4:
                ek[i].v.i4 = (int4)v->intValue(fd->_precision);
                break;
            case MCO_DB_FT_UINT8:
            case MCO_DB_FT_INT8:
            case MCO_DB_FT_AUTOID:
            case MCO_DB_FT_AUTOOID:
                 /* 8 byte */
                *(int64_t*) &ek[i].v.u8 = v->intValue(fd->_precision);
                break;
            case MCO_DB_FT_CHARS:
            case MCO_DB_FT_STRING:
                {
                    String* str = v->stringValue();
                    ek[i].v.ptr_size = str->size();
                    ek[i].ptr = str->body();
                    break;
                }

                #ifndef NO_FLOATING_POINT
                case MCO_DB_FT_FLOAT:
                    ek[i].v.flt = (float)v->realValue();
                    break;
                case MCO_DB_FT_DOUBLE:
                    ek[i].v.dbl = v->realValue();
                    break;
                #endif

                #ifdef UNICODE_SUPPORT
                case MCO_DB_FT_UNICODE_CHARS:
                case MCO_DB_FT_UNICODE_STRING:
                    {
                        UnicodeString* str = v->unicodeStringValue();
                        ek[i].v.ptr_size = str->size();
                        ek[i].ptr = (char*)convertWcharToNchar(str);
                        break;
                    }
                    #ifdef MCO_CFG_WCHAR_SUPPORT
                    case MCO_DB_FT_WIDE_CHARS:
                    case MCO_DB_FT_WCHAR_STRING:
                        {
                            UnicodeString* str = v->unicodeStringValue();
                            ek[i].v.ptr_size = str->size();
                            ek[i].ptr = (char*)str->body();
                            break;
                        }
                    #endif
                #endif
            default:
                MCO_THROW InvalidOperation(String::format("Operations with type %s are not supported", typeMnemonic[fd->mcoType])->cstr());    
        }
    }
}

void McoIndex::setMinValue(mco_external_field_t ek[], int nRanges)
{
    for (int i = nRanges; i < _keys->length; i++)
    {
        McoField* fd = _keys->items[i]->_field;
        switch (ek[i].field_type = fd->mcoType)
        {
            #ifdef MCO_DB_FT_BOOL
                case MCO_DB_FT_BOOL:
                #endif
            case MCO_DB_FT_UINT1:
            case MCO_DB_FT_OBJVERS:
                 /* 2 byte */
            case MCO_DB_FT_UINT2:
            case MCO_DB_FT_DATE:
            case MCO_DB_FT_TIME:
            case MCO_DB_FT_UINT4:
                ek[i].v.u4 = 0;
                break;
            case MCO_DB_FT_INT1:
                ek[i].v.i1 = (int1)0x80;
                break;
            case MCO_DB_FT_INT2:
                ek[i].v.i2 = (int2)0x8000;
                break;
            case MCO_DB_FT_INT4:
                ek[i].v.i4 = 0x80000000;
                break;
            case MCO_DB_FT_UINT8:
            case MCO_DB_FT_AUTOID:
            case MCO_DB_FT_DATETIME:
            case MCO_DB_FT_AUTOOID:
                 /* 8 byte */
                *(int64_t*) &ek[i].v.u8 = 0;
                break;
            case MCO_DB_FT_INT8:
                *(int64_t*) &ek[i].v.u8 = (int64_t)1 << 63;
                break;
            case MCO_DB_FT_CHARS:
            case MCO_DB_FT_STRING:
            case MCO_DB_FT_UNICODE_CHARS:
            case MCO_DB_FT_UNICODE_STRING:
            case MCO_DB_FT_WIDE_CHARS:
            case MCO_DB_FT_WCHAR_STRING:
                ek[i].v.ptr_size = 0;
                ek[i].ptr = "";
                break;
                #ifndef NO_FLOATING_POINT
                case MCO_DB_FT_FLOAT:
                    ek[i].v.flt = FLT_MIN;
                    break;
                case MCO_DB_FT_DOUBLE:
                    ek[i].v.dbl = DBL_MIN;
                    break;
                #endif
        }
    }
}

void McoIndex::setMaxValue(mco_external_field_t ek[], int nRanges)
{
    const int MaxStringLength = 8;
    static char const MaxSignedCharString[] =
    {
        0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f
    };
    static char const MaxUnsignedCharString[] =
    {
        (char)0xff, (char)0xff, (char)0xff, (char)0xff, (char)0xff, (char)0xff, (char)0xff, (char)0xff
    };
    static nchar_t const MaxUnsignedNCharString[] =
    {
        0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff
    };
    #if defined( MCO_CFG_CHAR_CMP_SIGNED )
        static nchar_t const MaxSignedNCharString[] =
        {
            0x7fff, 0x7fff, 0x7fff, 0x7fff, 0x7fff, 0x7fff, 0x7fff, 0x7fff
        };
        char const* const maxCharString = MaxSignedCharString;
        nchar_t const* const maxNCharString = MaxSignedNCharString;
    #elif defined( MCO_CFG_CHAR_CMP_UNSIGNED )
        char const* const maxCharString = MaxUnsignedCharString;
        nchar_t const* const maxNCharString = MaxUnsignedNCharString;
    #else
        char const* const maxCharString = (char)0xff < 0 ? MaxSignedCharString : MaxUnsignedCharString;
        nchar_t const* const maxNCharString = MaxUnsignedNCharString;
    #endif

    for (int i = nRanges; i < _keys->length; i++)
    {
        McoField* fd = _keys->items[i]->_field;
        switch (fd->mcoType)
        {
            #ifdef MCO_DB_FT_BOOL
                case MCO_DB_FT_BOOL:
                #endif
            case MCO_DB_FT_UINT1:
            case MCO_DB_FT_OBJVERS:
                 /* 2 byte */
            case MCO_DB_FT_UINT2:
            case MCO_DB_FT_DATE:
            case MCO_DB_FT_TIME:
            case MCO_DB_FT_UINT4:
                ek[i].v.u4 = ~0;
                break;
            case MCO_DB_FT_INT1:
                ek[i].v.i1 = 0x7F;
                break;
            case MCO_DB_FT_INT2:
                ek[i].v.i2 = 0x7fff;
                break;
            case MCO_DB_FT_INT4:
                ek[i].v.i4 = 0x7fffffff;
                break;
            case MCO_DB_FT_UINT8:
            case MCO_DB_FT_AUTOID:
            case MCO_DB_FT_DATETIME:
            case MCO_DB_FT_AUTOOID:
                 /* 8 byte */
                *(int64_t*) &ek[i].v.u8 = ~(int64_t)0;
                break;
            case MCO_DB_FT_INT8:
                *(int64_t*) &ek[i].v.u8 = ((uint64_t)1 << 63) - 1;
                break;
            case MCO_DB_FT_CHARS:
            case MCO_DB_FT_STRING:
                ek[i].v.ptr_size = MaxStringLength;
                ek[i].ptr = maxCharString;
                break;
            case MCO_DB_FT_UNICODE_CHARS:
            case MCO_DB_FT_UNICODE_STRING:
            case MCO_DB_FT_WIDE_CHARS:
            case MCO_DB_FT_WCHAR_STRING:
                ek[i].v.ptr_size = MaxStringLength;
                ek[i].ptr = (char*)maxNCharString;
                break;
                #ifndef NO_FLOATING_POINT
                case MCO_DB_FT_FLOAT:
                    ek[i].v.flt = FLT_MAX;
                    break;
                case MCO_DB_FT_DOUBLE:
                    ek[i].v.dbl = DBL_MAX;
                    break;
                #endif
        }
    }
}


DataSource* McoIndex::select(Transaction* trans, SearchOperation cop, int nRanges, Range ranges[])
{
    McoIndexSelection* selection = McoIndexSelection::create(nRanges, this);
    MCO_RET ret = MCO_S_OK;
    McoTransaction* t = (McoTransaction*)trans;
    if (type == BTREE || type == PTREE || type == RTREE || type == TRIGRAM || (type == HASH && !_unique))
    {
        while (true)
        {
            ret = mco_w_index_cursor(t->transaction, (uint2)indexCode,  &selection->cursor);
            if (ret == MCO_E_VOLUNTARY_NOT_EXIST)
            {
                if (!((McoTransaction*)trans)->upgrade()) {
                    MCO_THROW UpgradeNotPossible();
                }
                McoDatabase::checkStatus(mco_w_voluntary_create(t->transaction, (uint2)indexCode));
                continue;
            }
            break;
        }
    }
    if (ret == MCO_S_OK && nRanges != 0)
    {
        MCO_OPCODE op = MCO_EQ;
        #ifdef MCO_CFG_PATRICIA_SUPPORT
            if (type == PTREE)
            {
                switch (cop)
                {
                  case Index::opExactMatch:
                    op = MCO_EX;
                    break;
                  case Index::opPrefixMatch:
                    op = MCO_PREF;
                    selection->inverse = true;
                    break;
                  case Index::opEQ:
                    op = MCO_EQ;
                    break;
                  default:
                    MCO_THROW InvalidOperation("McoIndex::select: invalid operation");

                }
                setKeys(true, selection->ek, nRanges, ranges);
                if (ranges[0].highBound != NULL)
                {
                    selection->ek->bit_len = (uint1)ranges[0].highBound->intValue();
                }
                ret = mco_w_tree_find(t->transaction, &selection->cursor, op, selection->ek);
                if (ret == MCO_S_OK)
                {
                    selection->op = op;
                    selection->compare = true;
                }
                else if (ret != MCO_S_NOTFOUND && ret != MCO_S_CURSOR_EMPTY)
                {
                    McoDatabase::checkStatus(ret);
                }
                else
                {
                    selection->isEmpty = true;
                }
                return selection;
            }
        #endif
        if (type == RTREE)
        {
            switch (cop)
            {
            case Index::opEQ:
                op = MCO_EQ;
                break;
            case Index::opNear:
                op = MCO_NEIGHBOURHOOD;
                break;
            case Index::opOverlaps:
                op = MCO_OVERLAP;
                break;
            case Index::opContains:
                op = MCO_CONTAIN;
                break;
            case Index::opBelongs:
                op = MCO_LE;
                break;
            default:
                MCO_THROW InvalidOperation("McoIndex::select: invalid operation");

            }
            setKeys(true, selection->ek, nRanges, ranges);
            ret = mco_w_tree_find(t->transaction, &selection->cursor, op, selection->ek);
            if (ret == MCO_S_OK)
            {
                selection->op = op;
                selection->compare = false;
            }
            else if (ret != MCO_S_NOTFOUND && ret != MCO_S_CURSOR_EMPTY)
            {
                McoDatabase::checkStatus(ret);
            }
            else
            {
                selection->isEmpty = true;
            }
            return selection;
        }
        if (type == TRIGRAM)
        {
            if (cop != Index::opContains && cop != Index::opLike) {
                MCO_THROW InvalidOperation("McoIndex::select: invalid operation");
            }
            setKeys(true, selection->ek, nRanges, ranges);
            ret = mco_w_tree_find(t->transaction, &selection->cursor, MCO_CONTAIN, selection->ek);
            if (ret == MCO_S_OK)
            {
                selection->op = op;
                selection->compare = false;
            }
            else if (ret != MCO_S_NOTFOUND && ret != MCO_S_CURSOR_EMPTY)
            {
                McoDatabase::checkStatus(ret);
            }
            else
            {
                selection->isEmpty = true;
            }
            return selection;
        }
        bool low = true;
        bool between = false;
        if (ranges[0].lowBound == NULL)
        {
            low = false;
            op = _keys->items[0]->_order == ASCENT_ORDER ? ranges[0].isHighInclusive ? MCO_LE : MCO_LT:
                ranges[0].isHighInclusive ? MCO_GE : MCO_GT;
        }
        else if (ranges[0].highBound == NULL)
        {
            op = _keys->items[0]->_order == ASCENT_ORDER ? ranges[0].isLowInclusive ? MCO_GE : MCO_GT:
                ranges[0].isLowInclusive ? MCO_LE : MCO_LT;
        }
        else
        {
            int diff = 0;
            bool allInclusive = true;
            for (int i = 0; diff == 0 && i < nRanges; i++)
            {
                diff = ranges[i].lowBound == ranges[i].highBound ? 0 : ranges[i].lowBound->compare(ranges[i].highBound);
                allInclusive &= ranges[i].isLowInclusive & ranges[i].isHighInclusive;
            }
            if (diff < 0)
            {
                if (_keys->items[0]->_order == ASCENT_ORDER)
                {
                    op = ranges[0].isLowInclusive ? MCO_GE : MCO_GT;
                }
                else
                {
                    op = ranges[0].isHighInclusive ? MCO_GE : MCO_GT;
                    low = false;
                }
                between = true;
            }
            else if (diff > 0 || (diff == 0 && !allInclusive))
            {
                selection->isEmpty = true;
                return selection;
            }
        }
        if (cop == Index::opContainsAll || cop == Index::opContainsAny)
        {
            Array* set = (Array*)ranges[0].lowBound;
            int nKeywords = set->size();
            mco_search_kwd_t* keywords = (mco_search_kwd_t*)MemoryManager::allocator->allocate(nKeywords*sizeof(mco_search_kwd_t));
            Type elemType = set->getElemType();
            for (int i = 0; i < nKeywords; i++) {
                switch (elemType) {
                  case tpString:
                  {
                      String* s = (String*)set->getAt(i);
                      keywords[i].kwd_ptr = s->body();
                      keywords[i].kwd_val.ptr_size = (uint2)s->size();
                      break;
                  }
#ifdef UNICODE_SUPPORT
                  case tpUnicode:
                  {
                      UnicodeString* s = (UnicodeString*)set->getAt(i);
                      keywords[i].kwd_ptr = s->body();
                      keywords[i].kwd_val.ptr_size = (uint2)s->size();
                      break;
                  }
#endif
                  default:
                    set->getBody(&keywords[i].kwd_val, i, 1);
                }
            }
            ret = (cop == Index::opContainsAll) ? mco_w_tree_find_all(t->transaction, &selection->cursor, nKeywords, keywords) : mco_w_tree_find_any(t->transaction, &selection->cursor, nKeywords, keywords);
            if (ret == MCO_S_OK) {
                selection->op = MCO_EQ;
                selection->compare = false;
            } else if (ret != MCO_S_NOTFOUND) {
                selection->isEmpty = true;
            } else {
                McoDatabase::checkStatus(ret);
                selection->isEmpty = true;
            }
            return selection;
        }
        if (nRanges != _keys->length)
        {
            setMinValue(selection->ek, nRanges);
            op = MCO_GE;
        }
        setKeys(low, selection->ek, nRanges, ranges);
        if (type == BTREE)
        {
            selection->order = _keys->items[0]->_order;
            if (op == MCO_LE || op == MCO_LT)
            {
                ret = mco_w_tree_find(t->transaction, &selection->cursor, op, selection->ek);
                if (ret == MCO_S_OK) {
                    selection->order = (SortOrder)((int)selection->order ^ (ASCENT_ORDER|DESCENT_ORDER));
                    selection->inverse = true;
                    selection->op = op;
/*
                    mco_cursor_first(t->transaction, &selection->cursor);
                    selection->compare = true;
*/
                }
            }
            else
            {
                ret = mco_w_tree_find(t->transaction, &selection->cursor, op, selection->ek);
                if (ret == MCO_S_OK)
                {
                    int diff;
                    if (nRanges != _keys->length)
                    {
                        selection->compare = true;
                        setMaxValue(selection->ek, nRanges);
                        selection->op = MCO_LE;
                        McoDatabase::checkStatus(mco_w_cursor_compare(t->transaction, &selection->cursor, selection->ek,
                                                 &diff));
                        if (diff > 0)
                        {
                            selection->isEmpty = true;
                        }
                    }
                    else if (between)
                    {
                        selection->compare = true;
                        selection->op = low ? ranges[0].isHighInclusive ? MCO_LE : MCO_LT: ranges[0].isLowInclusive ?
                            MCO_LE : MCO_LT;
                        setKeys(!low, selection->ek, nRanges, ranges);
                        McoDatabase::checkStatus(mco_w_cursor_compare(t->transaction, &selection->cursor, selection->ek,
                                                 &diff));
                        if ((selection->op == MCO_LE && diff > 0) || (selection->op == MCO_LT && diff >= 0))
                        {
                            selection->isEmpty = true;
                        }
                    }
                    else if (op == MCO_EQ)
                    {
                        selection->compare = true;
                        selection->op = MCO_EQ;
                    }
                }
            }
        }
        else
        {
            if (_unique) {
                ret = mco_w_hash_find_scalar(((McoTransaction*)trans)->transaction, indexCode, selection->ek, &selection->obj);
            } else {
                ret = mco_w_hash_find(t->transaction, &selection->cursor, selection->ek);
                if (_flags & MCO_DB_INDF_PERSISTENT) {
                    selection->compare = true;
                    selection->op = MCO_EQ;
                }
            }
        }
    } else if (ret == MCO_S_OK) {
        if (type == BTREE) {
            selection->order = _keys->items[0]->_order;
            selection->invertable = true;
        }
        ret = mco_cursor_first(t->transaction, &selection->cursor);
    }
    if (ret != MCO_S_NOTFOUND && ret != MCO_S_CURSOR_EMPTY)
    {
        McoDatabase::checkStatus(ret);
    }
    else
    {
        selection->isEmpty = true;
    }
    return selection;
}


bool McoIndex::isApplicable(SearchOperation cop, int nRanges, Range ranges[])
{
    if (type == TRIGRAM)
    {
        return (cop == Index::opContains || cop == Index::opLike || cop == Index::opEQ) && nRanges == 1;
    }
    if (cop == Index::opContains || cop == Index::opBelongs || cop == Index::opNear)
    {
        return type == RTREE && nRanges == 1;
    }
    if (cop == Index::opContainsAll || cop == Index::opContainsAny)
    {
        return type == BTREE && nRanges == 1 && (_flags & MCO_DB_INDF_THICK);
    }
    if (cop == Index::opExactMatch || cop == Index::opPrefixMatch)
    {
        return type == PTREE && nRanges == 1;
    }
    if (_flags & (MCO_DB_INDF_VA_BASED | MCO_DB_INDF_TRIGRAM)) {
        return false;
    }
    MCO_OPCODE prevOp = MCO_EQ;
    for (int i = 0; i < nRanges; i++)
    {
        MCO_OPCODE op;
        if (ranges[i].lowBound == NULL)
        {
            op = ranges[i].isHighInclusive ? MCO_LE : MCO_LT;
        }
        else if (ranges[i].highBound == NULL)
        {
            op = ranges[i].isLowInclusive ? MCO_GE : MCO_GT;
        }
        else
        {
            if (!ranges[i].lowBound->equals(ranges[i].highBound) && type != BTREE)
            {
                return false;
            }
            op = MCO_EQ;
        }
        if (i != 0 && (op != MCO_EQ || prevOp != MCO_EQ))
        {
            return false;
        }
        prevOp = op;
    }
    if (nRanges != _keys->length && (type != BTREE || prevOp != MCO_EQ))
    {
        return false;
    }
    if ((cop == Index::opILike) ^ ((_keys->items[0]->_field->flags & MCO_DB_INDFLD_CASE_INSENSITIVE) != 0))
    {
        return false;
    }
    return prevOp == MCO_EQ || type == BTREE;
}

void McoIndex::drop(Transaction* trans)
{
    // MCO_THROW InvalidOperation("McoIndex::drop");
    McoDatabase::checkStatus(mco_w_voluntary_drop(((McoTransaction*)trans)->transaction, indexCode));
}


Table* McoIndex::table()
{
    return _table;
}


bool McoIndex::isUnique()
{
    return _unique;
}

bool McoIndex::isOrdered()
{
    return type == BTREE || type == PTREE;
}

bool McoIndex::isSpatial()
{
    return type == RTREE;
}

bool McoIndex::isTrigram()
{
    return type == TRIGRAM;
}

//
// Selection
//

int McoIndexSelection::nFields()
{
    return index->_table->nFields();
}

Iterator < Field > * McoIndexSelection::fields()
{
    return index->_table->fields();
}

Cursor* McoIndexSelection::records()
{
    MCO_THROW InvalidOperation("McoIndexSelection::records()");
}

Cursor* McoIndexSelection::records(Transaction* trans)
{
    if (index->type == HASH && index->_unique)
    {
        return new McoHashCursor(this, trans);
    }
    else
    {
        return new McoBtreeCursor(this, trans);
    }
}

size_t McoIndexSelection::nRecords(Transaction*)
{
    MCO_THROW InvalidOperation("McoIndexSelection::nRecords");
}

bool McoIndexSelection::isNumberOfRecordsKnown()
{
    return false;
}

int McoIndexSelection::compareRID(Record* r1, Record* r2)
{
    return index->_table->compareRID(r1, r2);
}

Reference* McoIndexSelection::getRID(Record* rec)
{
    return index->_table->getRID(rec);
}

bool McoIndexSelection::isRIDAvailable()
{
    return index->_table->isRIDAvailable();
}

void McoIndexSelection::extract(McoSql::Record* rec, void* dst, size_t size, bool nullIndicators[], McoSql::ExtractMode mode)
{
    return index->_table->extract(rec, dst, size, nullIndicators, mode);
}

SortOrder McoIndexSelection::sortOrder() 
{
    return order;
}

bool McoIndexSelection::invert(Transaction* trans)
{
    if (invertable) { 
        McoTransaction* t = (McoTransaction*)trans;
        MCO_RET ret = inverse
            ? mco_cursor_first(t->transaction, &cursor)
            : mco_cursor_last(t->transaction, &cursor);
        if (ret == MCO_S_OK) { 
            inverse = !inverse;
            return true;
        }
    }
    return false;
}
    

McoIndexSelection::McoIndexSelection(McoIndex* index)
{
    this->index = index;
    isEmpty = false;
    compare = false;
    inverse = false;
    invertable = false;
    order = UNSPECIFIED_ORDER;
    op = MCO_EQ;
    resetNeeded = false;
}

McoIndexSelection* McoIndexSelection::create(int nKeys, McoIndex* index)
{
    return new(sizeof(mco_external_field_t)* nKeys)McoIndexSelection(index);
}

//
// Btree cursor
//

McoBtreeCursor::McoBtreeCursor(McoIndexSelection* selection, Transaction* trans)
{
    this->selection = selection;
    this->trans = (McoTransaction*)trans;
    if (selection->isEmpty)
    {
        hasCurrent = false;
    }
    else
    {
        if (selection->resetNeeded)
        {
            McoDatabase::checkStatus(mco_w_cursor_locate(((McoTransaction*)trans)->transaction, (uint2)selection->index->indexCode,  &selection->obj,  &selection->cursor));
        }
        hasCurrent = true;
    }
}

void McoBtreeCursor::release()
{
    McoDatabase::checkStatus(mco_cursor_close(trans->transaction, &selection->cursor));
}

Record* McoBtreeCursor::next()
{
    if (!hasCurrent)
    {
        MCO_THROW NoMoreElements();
    }
    mco_objhandle_t obj;
    McoDatabase::checkStatus(mco_w_obj_from_cursor(trans->transaction, &selection->cursor, selection->index->_table ->classCode, &obj));
    if (!selection->resetNeeded)
    {
        selection->obj = obj;
        mco_tmgr_init_handle(&selection->obj);
        selection->resetNeeded = true;
    }
    while (true)
    {
        MCO_RET ret = selection->inverse 
            ? mco_cursor_prev(trans->transaction, &selection->cursor)
            : mco_cursor_next(trans->transaction, &selection->cursor);
        if (ret == MCO_S_CURSOR_END)
        {
            hasCurrent = false;
        }
        else
        {
            McoDatabase::checkStatus(ret);
            if (selection->compare)
            {
                int diff;
                McoDatabase::checkStatus(mco_w_cursor_compare(trans->transaction, &selection->cursor, selection->ek,
                                         &diff));
                switch (selection->op)
                {
                    case MCO_EQ:
                        #ifdef MCO_CFG_PATRICIA_SUPPORT
                        case MCO_PREF:
                        case MCO_EX:
                        #endif
                        hasCurrent = diff == 0;
                        break;
                    case MCO_LT:
                        hasCurrent = diff < 0;
                        break;
                    case MCO_LE:
                        hasCurrent = diff <= 0;
                        break;
                    case MCO_GT:
                        hasCurrent = diff > 0;
                        break;
                    case MCO_GE:
                        hasCurrent = diff >= 0;
                        break;
                    default:
                        assert(false);
                }
            }
        }
        break;
    }
    return new McoRecord(selection->index->_table->_fields, trans, &obj);
}

bool McoBtreeCursor::hasNext()
{
    #ifdef MCO_CFG_PATRICIA_SUPPORT
        if (!hasCurrent && selection->op == MCO_PREF)
        {
            while (true)
            {
                int diff;
                MCO_RET ret = mco_cursor_prev(trans->transaction, &selection->cursor);
                if (ret == MCO_S_CURSOR_END)
                {
                    return false;
                }
                McoDatabase::checkStatus(mco_w_cursor_compare(trans->transaction, &selection->cursor, selection->ek, &diff));
                if (diff ==  - 1 || (selection->ek[0].field_type == MCO_DB_FT_STRING && diff < 0 && diff >=  - 8))
                {
                    return false;
                }
                if (diff >= 0)
                {
                    hasCurrent = true;
                    return true;
                }
            }
        }
    #endif
    return hasCurrent;
}

//
// Hash cursor
//

McoHashCursor::McoHashCursor(McoIndexSelection* selection, Transaction* trans)
{
    this->selection = selection;
    this->trans = (McoTransaction*)trans;
    if (selection->isEmpty)
    {
        found = NULL;
    }
    else
    {
        found = new McoRecord(selection->index->_table->_fields, trans, &selection->obj);
    }
}

void McoHashCursor::release()
{
}

Record* McoHashCursor::next()
{
    if (found == NULL)
    {
        MCO_THROW NoMoreElements();
    }
    Record* r = found;
    found = NULL;
    return r;
}

bool McoHashCursor::hasNext()
{
    return found != NULL;
}

uint64_t FixedString::hashCode()
{
    uint64_t h = 0;
    size_t len = size();
    unsigned char* str = (unsigned char*)body();
    for (size_t i = 0; i < len && str[i] != MCO_SPACE_CHAR; i++) {
	h = h*31 + str[i];
    }
    return h;
}

String* FixedString::toLowerCase()
{
    char* src = cstr();
    int n = strlen(src);
    String* str = String::create(n);
    char* dst = str->body();
    for (int i = 0; i < n; i++)
    {
        dst[i] = tolower((unsigned char)src[i]);
    }
    dst[n] = '\0';
    return str;
}

McoSql::Value* FixedString::clone(AbstractAllocator* allocator)
{
    size_t len = size();
    FixedString* str = new (len, allocator) FixedString(len);
    memcpy(str->body(), body(), len+1);
    return str;
}

String* FixedString::toUpperCase()
{
    char* src = cstr();
    int n = strlen(src);
    String* str = String::create(n);
    char* dst = str->body();
    for (int i = 0; i < n; i++)
    {
        dst[i] = toupper((unsigned char)src[i]);
    }
    dst[n] = '\0';
    return str;
}


int FixedString::compare(McoSql::Value* x)
{
    if (x == NULL || x->isNull())
    {
        return 1;
    }
    String* s = (String*)x;
    int len1 = size();
    int len2 = s->size();
    int n = len1 < len2 ? len1 : len2;
    char* s1 = body();
    char* s2 = s->body();
    while (--n >= 0) {
        if (*s1 != *s2) {
            return *(unsigned char*)s1 - *(unsigned char*)s2;
        }
        if (*s1 == MCO_SPACE_CHAR) {
            return 0;
        }
        s1 += 1;
        s2 += 1;
    }
    return len1 > len2 ? *s1 == MCO_SPACE_CHAR ? 0 : 1 : len1 == len2 || *s2 == MCO_SPACE_CHAR ? 0 : -1;
}

void mco_sql_throw_exeption(char const* file, int line)
{
    mco_stop__(MCO_ERR_SQL_EXCEPTION, file, line);
}

#ifdef UNICODE_SUPPORT
    McoSql::Value* UnicodeFixedString::clone(AbstractAllocator* allocator)
    {
        size_t len = size();
        UnicodeFixedString* str = new (len*sizeof(wchar_t), allocator) UnicodeFixedString(len);
        memcpy(str->body(), body(), (len+1)*sizeof(wchar_t));
        return str;
    }

    UnicodeString* UnicodeFixedString::toLowerCase()
    {
        wchar_t* src = wcstr();
        int n = wcslen(src);
        UnicodeString* str = UnicodeString::create(n);
        wchar_t* dst = str->body();
        for (int i = 0; i < n; i++)
        {
            dst[i] = towlower(src[i]);
        }
        dst[n] = '\0';
        return str;
    }

    UnicodeString* UnicodeFixedString::toUpperCase()
    {
        wchar_t* src = wcstr();
        int n = wcslen(src);
        UnicodeString* str = UnicodeString::create(n);
        wchar_t* dst = str->body();
        for (int i = 0; i < n; i++)
        {
            dst[i] = towupper(src[i]);
        }
        dst[n] = '\0';
        return str;
    }

    uint64_t UnicodeFixedString::hashCode()
    {
        uint64_t h = 0;
        size_t len = size();
        wchar_t* str = body();
        for (size_t i = 0; i < len && str[i] != MCO_SPACE_CHAR; i++) {
            h = h*31 + str[i];
        }
        return h;
    }

    int UnicodeFixedString::compare(McoSql::Value* x)
    {
        if (x == NULL || x->isNull())
        {
            return 1;
        }
        UnicodeString* s = (UnicodeString*)x;
        int len1 = size();
        int len2 = s->size();
        int n = len1 < len2 ? len1 : len2;
        wchar_t* s1 = body();
        wchar_t* s2 = s->body();
        while (--n >= 0) {
            if (*s1 != *s2) {
                return *s1 - *s2;
            }
            if (*s1 == MCO_SPACE_CHAR) {
                return 0;
            }
            s1 += 1;
            s2 += 1;
        }
        return len1 > len2 ? *s1 == MCO_SPACE_CHAR ? 0 : 1 : len1 == len2 || *s2 == MCO_SPACE_CHAR ? 0 : -1;
    }
#endif



