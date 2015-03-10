/*******************************************************************
 *                                                                 *
 *  exceptions.h                                                      *
 *                                                                 *
 *  This file is a part of the eXtremeDB source code               *
 *  Copyright (c) 2001-2009 McObject LLC                           *
 *  All Rights Reserved                                            *
 *                                                                 *
 *******************************************************************/
#ifndef __EXCEPTIONS_H__
    #define __EXCEPTIONS_H__

#ifndef USE_STD_EXCEPTION
#define USE_STD_EXCEPTION 1
#endif

#if USE_STD_EXCEPTION
/* hack around qnx gcc 4.3.3 */
#if defined(_QNX)
    #ifdef __cplusplus
      #define tcpls __cplusplus
    #endif
    #undef __cplusplus
    #define __cplusplus 0
    #include "exception"
    #undef __cplusplus
    #ifdef tcpls
      #define __cplusplus tcpls
      #undef tcpls
    #endif
#else
    #include "exception"
#endif
#endif

    namespace McoSql
    {

        class String;

        /**
         * Root class for all McoSql exceptions
         */
        class McoSqlException
#if USE_STD_EXCEPTION
            : public std::exception
#endif
        {
            public:
                enum ErrorCode
                {
                    NO_SQL_ERROR, NO_MORE_ELEMENTS, INVALID_TYPE_CAST, COMPILE_ERROR, NOT_SINGLE_VALUE,
                    INVALID_OPERATION, INDEX_OUT_OF_BOUNDS, NOT_ENOUGH_MEMORY, NOT_UNIQUE, NOT_PREPARED,
                    RUNTIME_ERROR, COMMUNICATION_ERROR, UPGRADE_NOT_POSSIBLE, TRANSACTION_CONFLICT,
                    NULL_REFERENCE_EXCEPTION, INVALID_STATE, INVALID_ARGUMENT, NULL_VALUE_EXCEPTION,
                    CSV_FORMAT_EXCEPTION, SYSTEM_ERROR
                };

                virtual String* getMessage()const;

                virtual const char* what()const throw();

                McoSqlException(ErrorCode code)
                {
                        this->code = code;
                }

                /**
                 * Error code
                 */
                ErrorCode code;
        };


        /**
         * Exception thrown in case of OS error
         */
        class SystemError : public McoSqlException
        {
          public:
            char const* file;
            int         line;
            int         errcode;

            /**
             * Get exception message.
             */
            virtual String* getMessage()const;

            SystemError(int errcode, char const* file, int line);
        };

        /**
         * Exception thrown during parsing of CSV file
         */
        class InvalidCsvFormat : public McoSqlException
        {
          public:
            String*     file;
            int         line;
            char const* msg;

            /**
             * Get exception message.
             */
            virtual String* getMessage()const;

            InvalidCsvFormat(String* file, int line, char const* msg);
        };

        /**
         * Exception thrown when memory reserved for the SQL engine is exhausted
         */
        class NotEnoughMemory: public McoSqlException
        {
            public:
                /**
                 * Amount of reserved space
                 */
                size_t allocated;

                /**
                 * Amount of used space
                 */
                size_t used;

                /**
                 * Requested number of bytes
                 */
                size_t requested;

                /**
                 * Get exception message.
                 */
                virtual String* getMessage()const;

                NotEnoughMemory(size_t allocated, size_t used, size_t requested): McoSqlException(NOT_ENOUGH_MEMORY)
                {
                        this->allocated = allocated;
                        this->used = used;
                        this->requested = requested;
                }
        };

        /**
         * Exception thrown when requested operation is invalid or not supported in this context
         */
        class InvalidOperation: public McoSqlException
        {
            public:
                InvalidOperation(char const* operation): McoSqlException(INVALID_OPERATION)
                {
                        this->operation = operation;
                }

                virtual String* getMessage()const;

                /**
                 * Name of the operation
                 */
                char const* operation;
        };

        /**
         * Exception thrown when requested operation is possiblw in the current state
         */
        class InvalidState: public McoSqlException
        {
            public:
                InvalidState(char const* msg): McoSqlException(INVALID_STATE)
                {
                    this->msg = msg;
                }

                virtual String* getMessage()const;

                /**
                 * Error message
                 */
                char const* msg;
        };

        /**
         * Exception thrown when argument value doesn't belong to the requested domain
         */
        class InvalidArgument: public McoSqlException
        {
            public:
                InvalidArgument(char const* msg): McoSqlException(INVALID_ARGUMENT)
                {
                    this->msg = msg;
                }

                virtual String* getMessage()const;

                /**
                 * Error message
                 */
                char const* msg;
        };

        /**
         * Exception thrown when requested type conversion cannot be performed
         */
        class InvalidTypeCast: public McoSqlException
        {
            public:
                InvalidTypeCast(char const* realType, char const* castType): McoSqlException(INVALID_TYPE_CAST)
                {
                        this->realType = realType;
                        this->castType = castType;
                }

                virtual String* getMessage()const;

                /**
                 * Type of converter value
                 */
                char const* realType;

                /**
                 * Type to which value is converted
                 */
                char const* castType;
        };

        /**
         * Runtime error during statement execution
         */
        class RuntimeError: public McoSqlException
        {
            public:
                RuntimeError(char const* message): McoSqlException(RUNTIME_ERROR)
                {
                        this->message = message;
                }

                virtual String* getMessage()const;

                /**
                 * Explanation message
                 */
                char const* message;
        };

        /**
         * Commnication failure
         */
        class CommunicationError: public McoSqlException
        {
            public:
            CommunicationError(char const* message, int errcode = 0): McoSqlException(COMMUNICATION_ERROR)
                {
                        this->message = message;
                        this->errorCode = errcode;
                }

                virtual String* getMessage()const;

                /**
                 * Explanation message
                 */
                char const* message;

                /**
                 * Error code
                 */
                int errorCode;
        };


        /**
         * Null reference is dereferenced
         */
        class NullReferenceException : public McoSqlException
        {
          public:
            NullReferenceException(int loopId =  - 1): McoSqlException(NULL_REFERENCE_EXCEPTION)
            {
                this->loopId = loopId;
            }
            virtual String* getMessage()const;

            /**
             * Loop identifier (used to implement EXISTS clause)
             */
            int loopId;
        };

         /**
         * Null reference is dereferenced
         */
        class NullValueException : public McoSqlException
        {
          public:
            NullValueException(int loopId =  - 1): McoSqlException(NULL_VALUE_EXCEPTION)
            {
            }
            virtual String* getMessage()const;
        };

        /**
         * Array index out of bounds
         */
        class IndexOutOfBounds: public McoSqlException
        {
            public:
                IndexOutOfBounds(int index, int length, int loopId =  - 1): McoSqlException(INDEX_OUT_OF_BOUNDS)
                {
                        this->index = index;
                        this->length = length;
                        this->loopId = loopId;
                }

                virtual String* getMessage()const;

                /**
                 * Value of index
                 */
                int index;

                /**
                 * Array length
                 */
                int length;

                /**
                 * Loop identifier (used to implement EXISTS clause)
                 */
                int loopId;
        };


        /**
         * Attempt to move an iterator to next element when element doesn't exist
         */
        class NoMoreElements: public McoSqlException
        {
            public:
                NoMoreElements(): McoSqlException(NO_MORE_ELEMENTS){}

                virtual String* getMessage()const;
        };

        /**
         * MVCC transaction conflict
         */
        class TransactionConflict: public McoSqlException
        {
            public:
                TransactionConflict(): McoSqlException(TRANSACTION_CONFLICT){}

                virtual String* getMessage()const;
        };

        /**
         * Attempt to insert duplicate value in unique index
         */
        class NotUnique: public McoSqlException
        {
            public:
                NotUnique(): McoSqlException(NOT_UNIQUE){}

                virtual String* getMessage()const;
        };

        /**
         * Attempt to execute not prepared statement
         */
        class NotPrepared: public McoSqlException
        {
            public:
                NotPrepared(): McoSqlException(NOT_PREPARED){}

                virtual String* getMessage()const;
        };


        /**
         * Subselect query used in expression returns more than one record
         */
        class NotSingleValue: public McoSqlException
        {
            public:
                NotSingleValue(char const* reason): McoSqlException(NOT_SINGLE_VALUE)
                {
                        this->reason = reason;
                }

                virtual String* getMessage()const;

                /**
                 * The problem details
                 */
                char const* reason;
        };


        /**
         * Syntax or semantic error detected by compiler
         */
        class CompileError: public McoSqlException
        {
            public:
                /**
                 * Error message
                 */
                char const* msg;

                /**
                 * SQL query
                 */
                char const* sql;

                /**
                 * Position in string specifying SQL statement (0-based)
                 */
                int pos;

                virtual String* getMessage()const;

               CompileError(char const* msg, char const* sql, int pos): McoSqlException(COMPILE_ERROR)
                {
                        this->msg = msg;
                        this->sql = sql;
                        this->pos = pos;
                }
        };

        /**
         * Attempt to upgrade tranaction is failed
         */
        class UpgradeNotPossible: public McoSqlException
        {
            public:
                UpgradeNotPossible(): McoSqlException(UPGRADE_NOT_POSSIBLE){}

                virtual String* getMessage()const;
        };

    }

#endif

