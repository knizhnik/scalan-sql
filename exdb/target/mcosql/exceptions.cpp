/*******************************************************************
 *                                                                 *
 *  exceptions.cpp                                                      *
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
 * MODULE:    exceptions.cpp
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
#include <apidef.h>
#include <memmgr.h>

namespace McoSql
{

    static char unwindBuffer[16*1024];
    static StaticAllocator unwindAllocator(unwindBuffer, sizeof unwindBuffer);

    const char* McoSqlException::what()const throw()
    {
        AllocatorContext ctx(&unwindAllocator); // unwind of exception maybe performed by separate thread, so we have to initialize memory allocator's context
        unwindAllocator.reset(0);
        return getMessage()->cstr();
    }

    String* McoSqlException::getMessage() const
    {
        return String::format("Exception with code %d", (int)code);
    }

    String* InvalidCsvFormat::getMessage() const
    {
        return String::format("%s:%d: error in CSV file: %s", file->cstr(), line, msg);
    }

    InvalidCsvFormat::InvalidCsvFormat(String* file, int line, char const* msg) : McoSqlException(CSV_FORMAT_EXCEPTION)
    {
        this->file = file;
        this->line = line;
        this->msg = msg;
    }

    String* SystemError::getMessage() const
    {
        return String::format("%s:%d: error code %d", file, line, errcode);
    }

    SystemError::SystemError(int errcode, char const* file, int line) : McoSqlException(SYSTEM_ERROR)
    {
        this->errcode = errcode;
        this->file = file;
        this->line = line;
    }

    String* NotEnoughMemory::getMessage()const
    {
        return String::format("Not enough memory: allocated=%ld, used=%ld, requested=%ld", (long)allocated, (long)used,
                              (long)requested);
    }

    String* InvalidOperation::getMessage()const
    {
        return String::format("Operation %s is invalid", operation);
    }

    String* InvalidState::getMessage()const
    {
        return String::format("Invalid state: %s", msg);
    }

    String* InvalidArgument::getMessage()const
    {
        return String::format("Invalid argument: %s", msg);
    }

    String* InvalidTypeCast::getMessage()const
    {
        return String::format("Failed to cast value of type %s to %s", realType, castType);
    }

    String* RuntimeError::getMessage()const
    {
        return String::format("Runtime error: %s", message);
    }

    String* CommunicationError::getMessage()const
    {
        return String::format("Communication error: %s, errno=%d", message, errorCode);
    }

    String* IndexOutOfBounds::getMessage()const
    {
        return String::format("Index %d is out of bounds (array length %d)", index, length);
    }

    String* NullReferenceException::getMessage()const
    {
        return  String::create("Null reference exception");
    }

     String* NullValueException::getMessage()const
    {
        return  String::create("Null value exception");
    }

    String* NoMoreElements::getMessage()const
    {
        return String::create("No more elements");
    }

    String* NotUnique::getMessage()const
    {
        return String::create("Key value is not unique");
    }

    String* NotPrepared::getMessage()const
    {
        return String::create("Statement was not prepared");
    }

    String* NotSingleValue::getMessage()const
    {
        return String::create(reason);
    }

    String* CompileError::getMessage()const
    {
        const int MaxContextSize = 80;
        const int MinContextSize = 20;
        const char Pointer = '^';

        char const* p = sql + pos;
        char const* before = p;
        char const* after = p;
        char ch;
        while (before > sql && p - before < MinContextSize && before[-1] != '\n') before--;
        while (before > sql && p - before < MaxContextSize && isalnum(before[-1] & 0xFF)) before--;  // find beginning of identifier
        while ((ch = *after) != '\0' && ch != '\r' && ch != '\n' && after - p < MinContextSize) after++;
        if (isalnum(ch & 0xFF)) {
            while (after - before < MaxContextSize && isalnum(after[1] & 0xFF)) after++;  // find end of identifier
        }
        return String::format("Compiler error at position %d: %s\n%.*s\n%*c\n", pos, msg, (int)(after - before + 1), before, (int)(p - before + 1), Pointer);
    }

    String* UpgradeNotPossible::getMessage()const
    {
        return String::create("Upgrade of transaction is not possible");
    }

    String* TransactionConflict::getMessage()const
    {
        return String::create("MVCC transaction conflict");
    }
}
