/*******************************************************************
 *                                                                 *
 *  memmgr.cpp                                                      *
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
 * MODULE:    memmgr.cpp
 *
 * ABSTRACT:
 *
 * VERSION:   1.0
 *
 * HISTORY:
 *            1.0- 1 KK 16-Jan-2005 Created
 * --
 */

#include <memmgr.h>
#include "sqlcpp.h"
#include <exceptions.h>

#if MULTITHREADING_SUPPORT
    #include "sync.h"
#endif

#ifndef MCO_STACK_ALLOCATOR_BLOCK_SIZE
    #define MCO_STACK_ALLOCATOR_BLOCK_SIZE (64*1024)
#endif


#ifdef MCO_DEBUG_MEMORY_ALLOCATOR
size_t totalAllocated;
size_t maxAllocated;
#endif

#if MCO_CFG_SQL_MALLOC_PROFILE
#include <execinfo.h>
#endif

namespace McoSql
{


    #if MULTITHREADING_SUPPORT
        static MultithreadedAllocator mtAllocator;
        AbstractAllocator *MemoryManager::allocator = &mtAllocator;
    #else
        AbstractAllocator *MemoryManager::allocator;
    #endif

#if MCO_CFG_SQL_MALLOC_PROFILE
    #define MAX_STACK_DEPTH 6
    #define PROFILE_HASH_TABLE_SIZE 1013

    struct AllocStack {
        size_t depth;
        void* frame[MAX_STACK_DEPTH];

        bool operator == (AllocStack const& stack) const
        {
            if (depth != stack.depth) {
                return false;
            }
            for (int i = 0; i < MAX_STACK_DEPTH; i++) {
                if (frame[i] != stack.frame[i]) {
                    return false;
                }
            }
            return true;
        }

        size_t hashcode() const
        {
            size_t h = depth;
            for (int i = 0; i < MAX_STACK_DEPTH; i++) {
                h = (h << 8) ^ (size_t)frame[i];
            }
            return h;
        }

    };

    struct ProfileItem {
        AllocStack stack;
        ProfileItem* collision;
        size_t totalSize;
        size_t count;
    };

    static int order_iterms_by_size(const void* p, const void* q)
    {
        size_t s1 = (*(ProfileItem**)p)->totalSize;
        size_t s2 = (*(ProfileItem**)q)->totalSize;
        return s1 > s2 ? -1 : s1 == s2 ? 0 : 1;
    }

    class BacktraceMemoryProfile : public MemoryProfile
    {
        ProfileItem* table[PROFILE_HASH_TABLE_SIZE];
        size_t nItems;
        size_t totalSize;
        Mutex mutex;
      public:

        BacktraceMemoryProfile() {
            memset(table, 0, sizeof table);
            nItems = 0;
            totalSize = 0;
        }

        ~BacktraceMemoryProfile() {
            for (size_t i = 0; i < PROFILE_HASH_TABLE_SIZE; i++) {
                ProfileItem *curr, *next;
                for (curr = table[i]; curr != NULL; curr = next) {
                    next = curr->collision;
                    delete curr;
                }
            }
        }

        void add(AllocStack const& stack, size_t size) {
            size_t h = stack.hashcode() % PROFILE_HASH_TABLE_SIZE;
            ProfileItem* item;
            totalSize += size;
            for (item = table[h]; item != NULL; item = item->collision) {
                if (item->stack == stack) {
                    item->totalSize += size;
                    item->count += 1;
                    return;
                }
            }
            item = new ProfileItem();
            item->stack = stack;
            item->totalSize = size;
            item->count = 1;
            item->collision = table[h];
            table[h] = item;
            nItems += 1;
        }

        void addInterlocked(AllocStack const& stack, size_t size) {
            CriticalSection cs(mutex);
            add(stack, size);
        }

        virtual void dump(int fd, size_t top)
        {
            CriticalSection cs(mutex);
            char buf[256];
            ProfileItem** items = new ProfileItem*[nItems];
            size_t i, j;
            size_t len;
            for (i = 0, j = 0; i < PROFILE_HASH_TABLE_SIZE; i++) {
                for (ProfileItem* item = table[i]; item != NULL; item = item->collision) {
                    items[j++] = item;
                }
            }
            assert(j == nItems);
            qsort(items, j, sizeof(ProfileItem*), order_iterms_by_size);
            if (top == 0 || top > j) {
                top = j;
            }
            len = sprintf(buf, "Total size: %ld, number of traces: %ld\n", totalSize, nItems);
            write(fd, buf, len);
            for (i = 0; i < top; i++) {
                ProfileItem* item = items[i];
                len = sprintf(buf, "*** %ld bytes allocated for %ld objects at:\n", item->totalSize, item->count);
                write(fd, buf, len);
                backtrace_symbols_fd(item->stack.frame, item->stack.depth, fd);
            }
            delete[] items;
        }
    };

    struct AllocReq {
        size_t size;
        AllocStack stack;
    };

    static BacktraceMemoryProfile accProfile;
    MemoryProfile* AbstractAllocator::accumulativeProfile = &accProfile;
#endif

    void* mco_sql_allocate(size_t size) {
        return MemoryManager::allocator->allocate(size);
    }

    void MemoryManager::setAllocator(AbstractAllocator *newAllocator)
    {
        #if MULTITHREADING_SUPPORT
            allocator->attach();
            allocator->replace(newAllocator);
        #else
            allocator = newAllocator;
        #endif
    }

    void AbstractAllocator::attach()
    {
    }

    void AbstractAllocator::detach()
    {
    }

    AbstractAllocator::~AbstractAllocator() {}

    AbstractAllocator::AbstractAllocator()
    {
        nSegments = 0;
        lastSegmentId = 0;
        firstSegmentMark = 0;
    }

    AbstractAllocator *AbstractAllocator::getCurrent()
    {
        return this;
    }

    bool AbstractAllocator::isReentrant()
    {
        return false;
    }


    AbstractAllocator *AbstractAllocator::replace(AbstractAllocator *newAllocator)
    {
        MemoryManager::allocator = newAllocator;
        return this;
    }

    size_t AbstractAllocator::createSegment()
    {
        if (nSegments == 0)
        {
            firstSegmentMark = mark();
        }
        nSegments += 1;
        return ++lastSegmentId;
    }

    void AbstractAllocator::reset(size_t mark, size_t segment)
    {
        if (lastSegmentId == segment)
        {
            reset(mark);
        }
    }

    size_t AbstractAllocator::currentSegment()
    {
        return lastSegmentId;
    }

    void AbstractAllocator::releaseSegment(size_t segmentId)
    {
        assert(nSegments > lastSegmentId - segmentId);
        nSegments -= lastSegmentId - segmentId + 1;
        lastSegmentId = segmentId - 1;
    }

    void AbstractAllocator::deleteSegment(size_t segmentId, size_t mark)
    {
        assert(nSegments > 0);
        if (--nSegments == 0)
        {
            reset(firstSegmentMark);
            lastSegmentId = 0;
        }
        else if (lastSegmentId == segmentId)
        {
            reset(mark);
            lastSegmentId -= 1;
        }
    }

    size_t AbstractAllocator::usedMemory()
    {
        return mark();
    }

    void *StaticAllocator::allocate(size_t size)
    {
        size = (size + 7) &~7;
        if (used + size > allocated)
        {
            MCO_THROW NotEnoughMemory(allocated, used, size);
        }
        char *p = memory + used;
        used += size;
        return (void*)p;
    }

    void StaticAllocator::release()
    {
        memory = NULL;
        allocated = 0;
        used = 0;
    }

    size_t StaticAllocator::mark()
    {
        return used;
    }

    void StaticAllocator::reset(size_t mark)
    {
        used = mark;
    }

    DynamicAllocator::DynamicAllocator()
    {
        init(&::malloc, &::free, MCO_STACK_ALLOCATOR_BLOCK_SIZE, 8*MCO_STACK_ALLOCATOR_BLOCK_SIZE);
    }


    void DynamicAllocator::init(alloc_t alloc, free_t free, size_t quantum, size_t retain)
    {
        this->alloc = alloc;
        this->free = free;
        this->quantum = quantum;
        this->retain = retain;
        available = NULL;
        unused = 0;
        currOffs = 0;
        currBase = 0;
        currSize = 0;
        curr = NULL;
    }

    void *DynamicAllocator::allocate(size_t size)
    {
        size = (size + 7) & ~7;
#if MCO_CFG_SQL_MALLOC_PROFILE
        size += sizeof(AllocReq);
        if (currSize - currOffs < size) {
            if (currSize != currOffs) {
                AllocReq* tail = (AllocReq*)((char*)(curr + 1) + currOffs);
                tail->size = 0;
            }
#else
        if (currSize - currOffs < size) {
#endif
            if (available != NULL && available->size >= size)
            {
                Segment *segment = available;
                available = available->next;
                unused -= segment->size;
                segment->next = curr;
                curr = segment;
            }
            else
            {
                size_t segmentSize = size + sizeof(Segment) < quantum ? quantum : (size + sizeof(Segment) + quantum - 1) /
                    quantum * quantum;
#ifdef MCO_DEBUG_MEMORY_ALLOCATOR
                totalAllocated += segmentSize;
                if (totalAllocated > maxAllocated) maxAllocated = totalAllocated;
#endif
                Segment *segment = (Segment*)alloc(segmentSize);
                if (segment == NULL)
                {
                    MCO_THROW NotEnoughMemory(currBase + currSize, currBase + currOffs, size);
                }
                segment->next = curr;
                segment->size = segmentSize - sizeof(Segment);
                curr = segment;
            }
            currOffs = 0;
            currBase += currSize;
            currSize = curr->size;
        }
        char* ptr = (char*)(curr + 1) + currOffs;
        currOffs += size;
#if MCO_CFG_SQL_MALLOC_PROFILE
        void* bt[MAX_STACK_DEPTH+1];
        AllocReq* alloc = (AllocReq*)ptr;
        int depth = backtrace(bt, MAX_STACK_DEPTH+1) - 1;
        int i;
        if (depth > MAX_STACK_DEPTH) {
            depth = MAX_STACK_DEPTH;
        }
        for (i = 0; i < depth; i++) {
            alloc->stack.frame[i] = bt[i+1];
        }
        while (i < MAX_STACK_DEPTH) {
            alloc->stack.frame[i++] = NULL;
        }
        alloc->stack.depth = depth;
        alloc->size = size;
        {
            accProfile.addInterlocked(alloc->stack, size - sizeof(AllocReq));
        }
        ptr += sizeof(AllocReq);
#endif
        return ptr;
    }

    MemoryProfile* DynamicAllocator::profile(MemoryProfile* prof)
    {
#if MCO_CFG_SQL_MALLOC_PROFILE
        if (prof == NULL) {
            prof = new BacktraceMemoryProfile();
        }
        if (curr != NULL) {
            size_t saveSize = curr->size;
            curr->size = currOffs;
            for (Segment* segm = curr; segm != NULL; segm = segm->next) {
                AllocReq* req = (AllocReq*)(segm + 1);
                AllocReq* last = (AllocReq*)((char*)req + segm->size);
                while (req->size != 0 && req != last) {
                    ((BacktraceMemoryProfile*)prof)->add(req->stack, req->size);
                    req = (AllocReq*)((char*)req + req->size);
                }
            }
            curr->size = saveSize;
        }
#endif
        return prof;
    }

    size_t DynamicAllocator::mark()
    {
        return currBase + currOffs;
    }

    void DynamicAllocator::reset(size_t mark)
    {
        while (mark < currBase)
        {
            Segment *next = curr->next;
            if (unused + currSize <= retain)
            {
                curr->next = available;
                available = curr;
                unused += currSize;
            }
            else
            {
#ifdef MCO_DEBUG_MEMORY_ALLOCATOR
                totalAllocated -= curr->size;
#endif
                free(curr);
            }
            curr = next;
            currSize = curr->size;
            currBase -= currSize;
        }
        currOffs = mark - currBase;
    }

    AbstractDynamicAllocator *DynamicAllocator::clone()
    {
#ifdef MCO_DEBUG_MEMORY_ALLOCATOR
        totalAllocated += sizeof(DynamicAllocator);
#endif
        void* mem = alloc(sizeof(DynamicAllocator));
        if (mem == NULL)
        {
            MCO_THROW NotEnoughMemory(currBase + currSize, currBase + currOffs, sizeof(DynamicAllocator));
        }
        DynamicAllocator *copy = new(mem) DynamicAllocator(alloc, free, quantum, retain);
        return copy;
    }

    void DynamicAllocator::release()
    {
        Segment *segm,  *next;
        for (segm = available; segm != NULL; segm = next)
        {
            next = segm->next;
#ifdef MCO_DEBUG_MEMORY_ALLOCATOR
            totalAllocated -= segm->size;
#endif
            free(segm);
        }
        for (segm = curr; segm != NULL; segm = next)
        {
            next = segm->next;
#ifdef MCO_DEBUG_MEMORY_ALLOCATOR
            totalAllocated -= segm->size;
#endif
            free(segm);
        }
        curr = NULL;
        available = NULL;
        unused = 0;
        currBase = 0;
        currOffs = 0;
        currSize = 0;
    }

    void DynamicAllocator::destroy()
    {
        release();
#ifdef MCO_DEBUG_MEMORY_ALLOCATOR
        totalAllocated -= sizeof(*this);
#endif
        free(this);
    }

    DynamicAllocator::~DynamicAllocator()
    {
        release();
    }

    AllocatorContext::AllocatorContext(SqlEngine const& engine)
    {
        init(engine.getAllocator());
    }

    AllocatorContext::~AllocatorContext()
    {
        if (savedAllocator != NULL)
        {
            MemoryManager::allocator->replace(savedAllocator);
        }
    }


    void AllocatorContext::init(AbstractAllocator* allocator)
    {
        savedAllocator = NULL;
        if (allocator != NULL)
        {
            AbstractAllocator* currentAllocator = MemoryManager::allocator;
            if (currentAllocator != NULL)
            {
                savedAllocator = currentAllocator->replace(allocator);
            }
            else
            {
                MemoryManager::setAllocator(allocator);
            }
        }
    }

    #if MULTITHREADING_SUPPORT

        MultithreadedAllocator::MultithreadedAllocator()
        {
            ctx = new ThreadContext(NULL);
        }

        void MultithreadedAllocator::attach()
        {
            ctx->attach();
        }

        void MultithreadedAllocator::detach()
        {
            ctx->detach();
        }

        AbstractAllocator *MultithreadedAllocator::replace(AbstractAllocator *newAllocator)
        {
            AbstractAllocator* oldAllocator = (AbstractAllocator*)ctx->get();
            if (oldAllocator != newAllocator) {
                ctx->set(newAllocator);
            }
            return oldAllocator;
        }

        AbstractAllocator *MultithreadedAllocator::getCurrent()
        {
            return (AbstractAllocator*)ctx->get();
        }

        void *MultithreadedAllocator::allocate(size_t size)
        {
            return getCurrent()->allocate(size);
        }

        size_t MultithreadedAllocator::mark()
        {
            return getCurrent()->mark();
        }

        void MultithreadedAllocator::reset(size_t mark)
        {
            getCurrent()->reset(mark);
        }

        size_t MultithreadedAllocator::createSegment()
        {
            return getCurrent()->createSegment();
        }

        void MultithreadedAllocator::releaseSegment(size_t segmentId)
        {
            getCurrent()->releaseSegment(segmentId);
        }

        void MultithreadedAllocator::deleteSegment(size_t segmentId, size_t mark)
        {
            getCurrent()->deleteSegment(segmentId, mark);
        }

        void MultithreadedAllocator::reset(size_t mark, size_t segment)
        {
            getCurrent()->reset(mark, segment);
        }

        size_t MultithreadedAllocator::currentSegment()
        {
            return getCurrent()->currentSegment();
        }


        MultithreadedAllocator::~MultithreadedAllocator()
        {
            delete ctx;
        }

        size_t MultithreadedAllocator::usedMemory()
        {
            return getCurrent()->usedMemory();
        }

        void MultithreadedAllocator::release(){}

        bool MultithreadedAllocator::isReentrant()
        {
            return true;
        }

    #endif

}
