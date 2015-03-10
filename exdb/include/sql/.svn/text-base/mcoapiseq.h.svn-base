/*******************************************************************
 *                                                                 *
 *  mcoapiseq.h                                                       *
 *                                                                 *
 *  This file is a part of the eXtremeDB source code               *
 *  Copyright (c) 2001-2013 McObject LLC                           *
 *  All Rights Reserved                                            *
 *                                                                 *
 *******************************************************************/
#ifndef __MCOAPISEQ_H__
    #define __MCOAPISEQ_H__

    #include "dbapi.h"
    #include "mcowrap.h"
    #include "mcoseq.h"
    #include "memmgr.h"

    class McoGenericSequence: public McoSql::IterableValue
    {
        /**
         * Maximal number of characters returned by stringValue method
         */
        static size_t printLimit;

      public:
        virtual McoSql::Type type();

        int compare(McoSql::Value* x);
        size_t toString(char* buf, size_t bufSize);
        void output(McoSql::FormattedOutput *streams, size_t n_streams);
        size_t serialize(char* buf, size_t size);
        McoSql::String* stringValue();

        /**
         * Iterate through sequence elements
         * @return IntValue, RealValue or String representing sequence element value or NULL if there are no more elements
         */
        McoSql::Value* next();

        /**
         * Update elemType depending on actual content of iterator.
         * elemType can be either initialized in constructor or set later after initialization of iterator
         */
        void updateType();

        virtual mco_seq_iterator_t& getIterator() {
            return iterator;
        }

        /**
         * Get number of sequence elements
         */
        virtual mco_seq_no_t count();

        /**
         * Get subsequence of this sequence
         */
        virtual void subseq(mco_seq_iterator_t& outIterator, mco_seq_no_t from = 0, mco_seq_no_t till = MCO_SEQ_INFINITY);

        /**
         * Extract from this sequence elements which positions are specified by map sequence (represented using mapIterator).
         */
        virtual void map(mco_seq_iterator_t& outIterator, mco_seq_iterator_t& mapIterator);

        /**
         * Find poistions of ordered sequence elements which are greater or equal (for ascending order) or smaller or equal (for
         * descending order) then correspodent elements of unordered joined sequence.
         */
        virtual void join(mco_seq_iterator_t& outIterator, mco_seq_iterator_t& joinIterator);

        /**
         * Extract subsequence from this sequence corresponding to interval specified by another sequence.
         * This method is intended to be used after search method to cut sequence elements by timestamp range
         * This method is applicable only to materialized sequences (object fields)
         */
        virtual void project(mco_seq_iterator_t& outIterator, mco_seq_iterator_t& inIterator);

        /**
         * This method is applicable only to materialized sequences (object fields)
         * Returns sequence iterator for current object in the curtsor
         */
        virtual void fromCursor(mco_seq_iterator_t& outIterator, McoSql::Cursor* cursor);

        void subseq(mco_seq_no_t from = 0, mco_seq_no_t till = MCO_SEQ_INFINITY) {
            subseq(iterator, from, till);
        }
        void map(mco_seq_iterator_t& mapIterator) {
            map(iterator, mapIterator);
        }
        void join(mco_seq_iterator_t& joinIterator) {
            join(iterator, joinIterator);
        }
        void project(mco_seq_iterator_t& inIterator) {
            project(iterator, inIterator);
        }
        void fromCursor(McoSql::Cursor* cursor) {
            fromCursor(iterator, cursor);
        }

        /**
         * Check if this sequence is materialized (stored in object field) or is intermediate result fo calculations
         */
        virtual bool isField() const {
            return false;
        }

        virtual mco_objhandle_h getObject() const {
            return NULL;
        }

        virtual McoField *getField() const {
            return NULL;
        }

        /**
         * Reset iterator
         */
        virtual void reset();

        /**
         * Parse string representation of sequence, i.e. "{1.0, 1.1, -1.0, 0.0}"
         * @param type sequence element type,
         * @param str string representation of sequence
         * @return sequence iterator
         */
        static McoGenericSequence* parse(McoSql::Type type, char const* str);


        McoGenericSequence* getParallelSequence();
        virtual McoSql::Value* clone(McoSql::AbstractAllocator* allocator);
        static McoGenericSequence* getWrapper(mco_seq_iterator_h iterator);

        /**
         * Replace current connection with unique one from pool to make it possible concurrently proceed disk sequence
         * (disk manager can not share connection between different threads)
         * @param con new connection
         * @param threadId identified (0..N) of processing thread
         */
        virtual void replaceConnection(mco_db_h& con, int threadId);

        // Alocate materialized sequence iterator, since this iterator is always cloned, we do not need space for tile
        void* operator new(size_t size, McoSql::AbstractAllocator* allocator);
        
        // Allocate sequence iterator: allocate as much space as needed for the partiular element type
        void* operator new(size_t size, McoSql::Type elemType, McoSql::AbstractAllocator* allocator);

        void* operator new(size_t size, McoSql::Type elemType)  {
            return operator new (size, elemType, McoSql::MemoryManager::allocator);
        }

        /// Create new sequence using allocator and type of this sequence
        McoGenericSequence* create(bool reentrant = false);

        McoGenericSequence(McoSql::Type t = McoSql::tpNull, bool reentrant = false);
        McoGenericSequence(McoSql::AbstractAllocator* allocator, McoSql::Type t = McoSql::tpNull, bool reentrant = false);

        McoSql::Type elemType;
        McoGenericSequence* groupBy;
        McoGenericSequence* projection;
        McoSql::AbstractAllocator* allocator;
        mco_seq_no_t* permutation;
        mco_seq_no_t n_elems;
        bool isSearchResult;
        bool isReentrant;
        bool isScatterable;
        bool isIgnored;

        // this fields actually belog to McoSequence class, we have to move them heer because of trick with mco_seq_iterator_t truncation
        McoField* fd;
        McoTransaction* trans;
        mco_objhandle_h hnd;

        mco_seq_iterator_t iterator;
    };

    template<class T>
    class McoSequence: public McoGenericSequence
    {
        friend class McoField;
      public:
        virtual void reset() {
        }

        virtual bool isField() const {
            return true;
        }

        virtual mco_objhandle_h getObject() const {
            return hnd;
        }

        virtual McoField *getField() const {
            return fd;
        }

        virtual mco_seq_iterator_t& getIterator() {
            subseq(iterator);
            return iterator;
        }

        virtual T first();
        virtual T last();
        virtual void append(T const* items, mco_size_t nItems);
        virtual void insert(mco_seq_no_t pos, T const* items, mco_size_t nItems);
        virtual void remove(mco_seq_no_t from = 0, mco_seq_no_t till = MCO_SEQ_INFINITY);

        /**
         * Perform search in ordered sequence.
         * This method selects subsequence matching searhc criteria. Result of search can be projected to other (non-ordered) sequences
         * using project method
         */
        virtual void search(mco_seq_iterator_t& outIterator, T low, mco_seq_boundary_kind_t low_boundary, T high, mco_seq_boundary_kind_t high_boundary);
        virtual void subseq(mco_seq_iterator_t& outIterator, mco_seq_no_t from = 0, mco_seq_no_t till = MCO_SEQ_INFINITY);
        virtual void map(mco_seq_iterator_t& outIterator, mco_seq_iterator_t& mapIterator);
        virtual void join(mco_seq_iterator_t& outIterator, mco_seq_iterator_t& joinIterator);
        virtual void project(mco_seq_iterator_t& outIterator, mco_seq_iterator_t& inIterator);
        virtual void fromCursor(mco_seq_iterator_t& outIterator, McoSql::Cursor* cursor);

        void search(T low, mco_seq_boundary_kind_t low_boundary, T high, mco_seq_boundary_kind_t high_boundary) {
            search(iterator, low, low_boundary, high, high_boundary);
        }

        virtual void store(mco_seq_iterator_t& inIterator);
        virtual mco_seq_no_t count();

        McoSequence(McoField* field, McoSql::Transaction* trans, mco_objhandle_h hnd);

        virtual void replaceConnection(mco_db_h &con, int threadId);

        virtual McoSql::Value* clone(McoSql::AbstractAllocator* allocator);
    };

#endif
