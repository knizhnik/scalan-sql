package.path = package.path .. ";./?.lua"
require("oop")
require("iter")

local ffi = require 'ffi'
local C = ffi.C

-- MySQLTable
local MySQLTableIter = Iter:new({})

function createMySQLTableIter(tableInfo, pCrsr, rowParser, initFieldPtrs)
  local max_column = tableInfo.maxColumn
  local res = MySQLTableIter:new({
    tableInfo = tableInfo,
    max_column = max_column,
    rowBuffer = ffi.new(tableInfo.bufferCTypeName),  -- e.g. "LineitemProjection"
    pCrsr = pCrsr,
    rowParser = rowParser,
    initFieldPtrs = initFieldPtrs,
    eof = ffi.new("int[1]"),
    pChunk = ffi.new("str_chunk")
  })
  res:initFieldPtrs(res.rowBuffer)
  return res
end

function MySQLTableIter:next()
  local rc = C.mysqlNextRecordt(self.pCrsr, self.eof)
  if rc ~= 0 then error("SQLITE_ERROR") end  
  if self.eof[0] ~= 0 then return nil end
  self:rowParser(row)
  return self.rowBuffer
end


function MySQLTableIter:parseInt(row, columnId, pBuf)
  C.mysqlGetInt(self.pCsr, columnId, pBuf)
end

function MySQLTableIter:parseDouble(row, columnId, pBuf)
  C.mysqlGetDouble(self.pCsr, columnId, pBuf)
end

function MySQLTableIter:parseString(row, columnId, pBuf)
  C.mysqlGetString(self.pCsr, columnId, pBuf)
end

function MySQLTableIter:parseByteChunk(row, columnId, pBuf, columnName)
  C.mysqlGetChunk(self.pCsr, columnId, pBuf)
  self.rowBuffer[columnName] = pBuf.ptr[0]
end

function MySQLTableIter:parseDate(row, columnId, pBuf)
  C.mysqlGetDate(self.pCsr, columnId, pBuf)
end

function getFieldPtr(fieldType, fieldName, rowBuffer)
  return ffi.cast(fieldType, ffi.cast("char *", rowBuffer) + assert(ffi.offsetof(rowBuffer, fieldName), "no such field, " .. fieldName))
end


