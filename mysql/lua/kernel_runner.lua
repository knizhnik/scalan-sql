package.path = package.path .. ";src/lua/?.lua;?.lua"

local ffi = require("ffi")

require("iter")
require("myqsl_iter")

ffi.cdef[[
  typedef unsigned int u32;
  typedef unsigned char u8;
  typedef long i64;

  typedef struct MySQLCursor MySQLCursor;
  typedef struct JOIN JOIN;
  typedef struct MySQLResult MySQLResult;

  int mysqlNextRecord(MySQLCursor *pCur, int *eof);

  MySQLCursor* mysqlGetCursor(JOIN *join, int k);

  typedef struct str_chunk {
    char *ptr;
    int n;
  } str_chunk;

  typedef struct dyn_string {
    char *ptr;
  } dyn_string;

  typedef struct flexistring {
    bool materialized;
    union {
        str_chunk ref; // used if materialized is 0
        dyn_string buf; // used then materialized is 1
    } mem;
  } flexistring;

  int flexistring_cmp_str(const flexistring *fstr1, const char *str2);
  int flexistring_cmp_flexistring(const flexistring *fstr1, const flexistring *fstr2);
  flexistring flexistring_materialize(const flexistring *fs);
  void flexistring_free(flexistring *fs);
  void flexistring_dbg_print(const flexistring *fs);

  typedef long date_t;

  void *memcpy(void *dest, const void *src, size_t n);
  int strcmp(const char *s1, const char *s2);

  // specialized version
  void mysqlGetInt(MySQLCursor* cursor, int columnNo, int *place);
  void mysqlGetDouble(MySQLCursor* cursor, int columnNo, double *place);
  void mysqlGetDate(MySQLCursor* cursor, int columnNo, date_t *place);
  void mysqlGetString(MySQLCursor* cursor, int columnNo, flexistring *fstr);

  MySQLResult* mysqlResultCreate(JOIN* join);
  void mysqlResultSend(MySQLResult* result);
  void mysqlResultEnd(MySQLResult* result);

  void mysqlResultWriteInt(MySQLResult* result, int val);
  void mysqlResultWriteDouble(MySQLResult* result, double val);
  void mysqlResultWriteDate(MySQLResult* result, date_t val);
  void mysqlResultWriteString(MySQLResult* result, char const* val);
]]

if os.getenv("JIT_DUMP") then
   jit_dump = require 'jit.dump'
   jit_dump.on()
end

prof = require 'jit.p'
jit = require 'jit'

local C=ffi.C

local t_flexistring = ffi.typeof("flexistring")
local t_flexistring_ref = ffi.typeof("flexistring &")

function flexistring_materialize(s)
  assert(type(s) == 'cdata' and ffi.typeof(s) == t_flexistring_ref)
  return C.flexistring_materialize(s)
end

function to_lua_string(s)
  assert(type(s) == 'cdata' and ffi.typeof(s) == t_flexistring_ref, tostring(ffi.typeof(s)))
  if s.materialized then
    return ffi.string(s.mem.buf.ptr)
  else
    return ffi.string(s.mem.ref.ptr, s.mem.ref.n)
  end
end

ffi.metatype(ffi.typeof("flexistring"), {
               __lt = function(s1,s2)
                 if type(s2) == 'string' then
                   return C.flexistring_cmp_str(s1,s2) < 0
                 elseif type(s1) == 'string' then
                   return C.flexistring_cmp_str(s2,s1) > 0
                 else
                   return C.flexistring_cmp_flexistring(s1,s2) < 0
                 end
               end,
               __le = function(s1,s2)
                 if type(s2) == 'string' then
                   return C.flexistring_cmp_str(s1,s2) <= 0
                 elseif type(s1) == 'string' then
                   return C.flexistring_cmp_str(s2,s1) >= 0
                 else
                   return C.flexistring_cmp_flexistring(s1,s2) <= 0
                 end
               end,
               __eq = function(s1,s2)
                 if type(s2) == 'string' then
                   return C.flexistring_cmp_str(s1,s2) == 0
                 elseif type(s1) == 'string' then
                   return C.flexistring_cmp_str(s2,s1) == 0
                 else
                   return C.flexistring_cmp_flexistring(s1,s2) == 0
                 end
               end,
               __gc = function(s)
                 C.flexistring_free(s)
               end
})

-- called by JOIN::exec on preparation stage before looping
function kernel_entry_point(join, kernel_id)
   jit.flush()
   if os.getenv("JIT_PROFILE") then
      prof.start(os.getenv("JIT_PROFILE"))
   end

   dbg("hello from kernel_entry_point")
   local ctx = assert(g_ctxs[kernel_id], "kernel context found")
   local K = assert(ctx.K, 'kernel found')

   -- opening table iterators
   local iters = {}
   for iter_id, cur in pairs(ctx.cursors) do
      local pCrsr = C.mysqlGetCursor(join, cur.iCsr)
      local table_info = assert(cur.table_info)
      local iter_data = assert(K.input_iterators[iter_id], "table is known to kernel")
      local tbl_iter = createMySQLTableIter(table_info, pCrsr,
                                            iter_data.unpack_row, iter_data.init_iter_fields)
      iters[iter_id] = tbl_iter
   end

   -- preparing result writer object
   local QueryResult = {
	  result = C.mysqlCreateResult(join)
      putInt = function(self, num)
         C.mysqlResultWriteInt(self.result, num)
      end,
      putDouble = function(self, num)
         C.mysqlResultWriteDouble(self.result, num)
      end,
      putString = function(self, str)
        if type(str) == 'string' then
          C.mysqlResultWriteString(self.result, str)
        elseif ffi.typeof(str) == t_flexistring_ref then
          C.mysqlResultWriteString(self.result, str.mem.buf.ptr)
        else error("unknown arg type to putString")
        end
      end,
      putDate = function(self, val)
        C.mysqlResultWriteDate(self.result, val)
      end,
      putChar = function(self, chr)
        local str = string.char(chr)
        C.mysqlResultWriteString(self.result, str)
      end
   }

   local init_res
   if K.kernel_init then
     init_res = K.kernel_init(ctx.params)
   end
   -- registering result iterator and result writer for futher calls from iterator_entry_point
   ctx.result_iter = K.result_iterator(iters, ctx.params, init_res)
   ctx.result_builder = QueryResult

   if os.getenv("JIT_PROFILE") then
      prof.stop()
   end

   return 0
end

-- called by vdbe loop, returns single result row
function iterator_entry_point(join, kernel_id)
   dbg("hello from iterator_entry_point")

   if os.getenv("JIT_PROFILE") then
      prof.start(os.getenv("JIT_PROFILE"))
   end

   local ctx = assert(g_ctxs[kernel_id], "kernel context found")
   local K = assert(ctx.K, 'kernel found')

   local res = ctx.result_iter:next()

   if res == nil then
      C.mysqlResultEnd(ctx.result_builder.result);
   else
     K.packResponse(ctx.result_builder, res)
     C.mysqlResultSend(ctx.result_builder.result)
   end

   if os.getenv("JIT_PROFILE") then
      prof.stop()
   end

   return 0
end
