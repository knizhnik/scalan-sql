/*******************************************************************
 *                                                                 *
 *  memmgr.h                                                      *
 *                                                                 *
 *  This file is a part of the eXtremeDB source code               *
 *  Copyright (c) 2001-2005 McObject LLC                           *
 *  All Rights Reserved                                            *
 *                                                                 *
 *******************************************************************/
#ifndef __MEMMGR_H__
#define __MEMMGR_H__

#include "config.h"

namespace McoSql
{

    class AbstractAllocator;
    class MultithreadedAllocator;

    class MemoryProfile {
      public:
        virtual void dump(int fd, size_t top) = 0;
        virtual ~MemoryProfile() {}
    };

    /**
     * Allocator interface
     * Allocators in McoSQL are used in a stack manner:
     * McoSQL will not de-allocate a particular object. Instead, it
     * remembers the current position of allocator and when operation is completed,
     * de-allocates all temporary results by resetting allocator to
     * original position (mark).
     */
    class AbstractAllocator
    {
      public:
        /**
         * Allocate requested number of bytes.
         * @param size requested size of memory
         * @return pointer to allocated object
         * @throws NotEnoughMemory exception when allocation request cannot be satisfied
         */
        virtual void* allocate(size_t size) = 0;

        /**
         * Get current allocator position. Can be used to reset
         * allocator to the state corresponding to the moment of invocation of
         * mark() method. All objects
         * allocated after invocation of mark() method are efficiently de-allocated.
         * It is assumed no such object can be referenced by application.
         * It is responsibility of SQL engine to correctly choose moments
         * of mark and reset.
         * @return current allocator position
         */
        virtual size_t mark() = 0;

        /**
         * Reset allocator to previous state obtained using mark() method.
         * All objects
         * allocated after invocation of mark() method are efficiently de-allocated.
         * It is assumed no such object can be referenced by application.
         * It is responsibility of SQL engine to correctly choose moments
         * of mark and reset.
         * @param mark remembered allocator position
         */
        virtual void reset(size_t mark) = 0;

        /**
         * Reset allocator to previous state obtained using mark() method
         * if current segment is the same as specified.
         * All objects
         * allocated after invocation of mark() method are efficiently de-allocated.
         * It is assumed no such object can be referenced by application.
         * It is responsibility of SQL engine to correctly choose moments
         * of mark and reset.
         * @param mark remembered allocator position
         * @param segment previously allocated segment, mark is reset only when the current segment is equal
         * to the specified
         */
        virtual void reset(size_t mark, size_t segment);

        /**
         * Get current segment identifier
         * @return identifier of the last segment allocated by createSegment
         */
        virtual size_t currentSegment();

        /**
         * Starts allocation in new segment.
         * @return segment identifier
         */
        virtual size_t createSegment();

        /**
         * Release segment but preserve its content.
         * @param segmentId identifier of released segment
         */
        virtual void releaseSegment(size_t segmentId);

        /**
         * Delete segment and its content.
         * @param segmentId identifier of deleted segment
         * @param mark segment mark
         */
        virtual void deleteSegment(size_t segmentId, size_t mark);

        /**
         * Release all resources used by allocator.
         */
        virtual void release() = 0;

        /**
         * Get amount of used memory.
         */
        virtual size_t usedMemory();

        /**
         * Replace current allocator with new one
         * @param newAllocator new allocator
         * @return old allocator
         */
        virtual AbstractAllocator* replace(AbstractAllocator* newAllocator);

        /**
         * Whether allocator can be invoked from different threads
         */
        virtual bool isReentrant();

        /**
         * Get current allocator.
         * @return this in case of single-thread allocator or current thread allocator in case of
         * reentrant allcotor
         */
        virtual AbstractAllocator* getCurrent();

        /**
         * Attach new thread
         */
        virtual void attach();

        /**
         * Detach thread
         */
        virtual void detach();

        /**
         * Default constructor
         */
        AbstractAllocator();

        virtual ~AbstractAllocator();

        /**
         * Create or update memory profile
         */
        virtual MemoryProfile* profile(MemoryProfile* prof) {
            return prof;
        }

        static MemoryProfile* accumulativeProfile;

      private:
        size_t nSegments;
        size_t lastSegmentId;
        size_t firstSegmentMark;
    };


    /**
     * Allocator of statically allocated buffer. The pointer to the buffer and
     * its size are provided by application. This allocator can only allocate
     * space within this buffer.
     */
    class StaticAllocator: public AbstractAllocator
    {
      public:
        virtual void* allocate(size_t size);

        virtual size_t mark();

        virtual void reset(size_t mark);

        virtual void release();

        /**
         * Initialize static allocator
         * @param buffer memory reserved for allocator
         * @param size buffer size
         */
        void init(void* buffer, size_t size)
        {
            memory = (char*)buffer;
            allocated = size;
            used = 0;
        }

        /**
         * Constructor of static allocator
         * @param buffer memory reserved for allocator
         * @param size buffer size
         */
        StaticAllocator(void* buffer = NULL, size_t size = 0)
        {
            init(buffer, size);
        }

      private:
        char* memory;
        size_t allocated;
        size_t used;
    };

    /**
     * Base abstract class for all dynamic allocators -
     * allocators which allocate system space on demand
     */
    class AbstractDynamicAllocator: public AbstractAllocator
    {
      protected:
        void* operator new(size_t size, void* adr)
        {
            return adr;
        }
        void operator delete (void* ptr, void* adr){}
        void operator delete (void* ptr){}

      public:
        /**
         * Method used to obtain copy of this allocator
         * This method is used by multithreaded allocator to
         * allow concurrent allocation and concurrent execution of SQL.
         * @return copy of this allocator (state of cloned allocator is initial)
         */
        virtual AbstractDynamicAllocator* clone() = 0;

        /**
         * Destroy allocated clone of allocator.
         */
        virtual void destroy() = 0;
    };

    /**
     * Allocator with user-provided alloc and free functions
     * This allocator uses provided functions to allocate memory segments
     * which then are used to allocate objects within them.
     */
    class DynamicAllocator: public AbstractDynamicAllocator
    {
      public:
        typedef void *(*alloc_t)(size_t size);
        typedef void(*free_t)(void* ptr);

        virtual void* allocate(size_t size);

        virtual size_t mark();

        virtual void reset(size_t mark);

        virtual AbstractDynamicAllocator* clone();

        virtual void destroy();
        virtual void release();

        /**
         * Initialize allocator
         * @param alloc memory allocation function
         * @param free memory de-allocation function
         * @param quantum segment size allocated each time when more space is needed
         * If requested size of memory is larger than segment size, then size
         * of allocated segment will be multiplier of quantum.
         * @param retain space retained by allocator
         * If this parameter is larger than 0, allocator will retain segments which are not
         * used any more in order to reuse them in future. Total size of retained segment
         * will not exceed the value of this parameter. If this parameter is 0,
         * or if size of already retained segments is larger than value of this parameter,
         * then such segments will be immediately de-allocated using free function.
         */
        void init(alloc_t alloc, free_t free, size_t quantum, size_t retain);

        /**
         * Allocator constructor
         * @param alloc memory allocation function
         * @param free memory de-allocation function
         * @param quantum segment size allocated each time when more space is needed
         * If requested size of memory is larger than segment size, then size
         * of allocated segment will be multiplier of quantum.
         * @param retain If this parameter is true, then allocator will retain segments which are not
         * used anymore in order to reuse them in future. If this parameter is false,
         * then such segments will be immediately de-allocated using free function.
         */
        DynamicAllocator(alloc_t alloc, free_t free, size_t quantum, size_t retain)
        {
            init(alloc, free, quantum, retain);
        }

        /**
         * Create default dynamic allocator allocator.
         */
        DynamicAllocator();

        /**
         * Release all memory used by allocator.
         */
        virtual ~DynamicAllocator();

        DynamicAllocator(DynamicAllocator const& other) {
            init(other.alloc, other.free, other.quantum, other.retain);
        }

        MemoryProfile* profile(MemoryProfile* profile);

      private:
        struct Segment
        {
            Segment* next;
            size_t size;
        };

        alloc_t alloc;
        free_t free;
        Segment* curr;
        Segment* available;
        size_t currOffs;
        size_t currBase;
        size_t quantum;
        size_t currSize;
        size_t retain;
        size_t unused;
        size_t maxSize;
    };

    class ThreadContext;

    /**
     * Allocator for multithreaded enviroment allowing each thread to use
     * its own allocator
     */
    class MultithreadedAllocator: public AbstractAllocator
    {
      public:
        virtual void* allocate(size_t size);

        virtual void attach();

        virtual void detach();

        virtual size_t mark();

        virtual void reset(size_t mark);

        virtual void reset(size_t mark, size_t segment);

        virtual void release();

        virtual size_t usedMemory();

        virtual size_t createSegment();

        virtual void releaseSegment(size_t segmentId);

        virtual void deleteSegment(size_t segmentId, size_t mark);

        virtual size_t currentSegment();

        virtual bool isReentrant();

        virtual AbstractAllocator* replace(AbstractAllocator* newAllocator);

        virtual AbstractAllocator* getCurrent();

        /**
         * Multithreaded allocator constructor
         */
        MultithreadedAllocator();

        /**
         * Delete all per-thread allocators used by this allocator.
         */
        virtual ~MultithreadedAllocator();

      private:
        ThreadContext* ctx;
    };

    /**
     * Class responsible for controlling memory managemnet policy for McoSQL applications
     */
    class MemoryManager
    {
      public:
        /**
         * Allocator used to allocate all dynamic objects used by McoSQL and
         * implementation of underlying storage layer
         */
        static AbstractAllocator* allocator;

        /**
         * Set new allocator.
         * @param newAllocator allocator to be used
         */
        static void setAllocator(AbstractAllocator* newAllocator);

        /**
         * Get current allocator.
         */
        static AbstractAllocator* getAllocator()
        {
            return allocator != NULL ? allocator->getCurrent(): NULL;
        }
    };

    class SqlEngine;

    class AllocatorContext
    {
        AbstractAllocator* savedAllocator;

        void init(AbstractAllocator* allocator);

      public:
        AllocatorContext(SqlEngine const& engine);

        AllocatorContext(AbstractAllocator* allocator)
        {
            init(allocator);
        }

        ~AllocatorContext();
    };


    class NewMemorySegment
    {
      public:
        size_t mark;
        size_t segmentId;
        bool   detached;

        void detach() {
            detached = true;
        }

        NewMemorySegment() {
            mark = MemoryManager::allocator->mark();
            segmentId = MemoryManager::allocator->createSegment();
            detached = false;
        }

        void release(bool unwind = true) {
            if (unwind) {
                MemoryManager::allocator->reset(mark);
            }
            MemoryManager::allocator->releaseSegment(segmentId);
            detached = true;
        }

        ~NewMemorySegment() {
            if (!detached) {
                MemoryManager::allocator->deleteSegment(segmentId, mark);
            }
        }
    };
}

#endif
