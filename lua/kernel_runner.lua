package.path = package.path .. ";src/lua/?.lua;?.lua"

local ffi = require("ffi")

require("iter")

ffi.cdef[[
  typedef unsigned short u16;
  typedef unsigned int u32;
  typedef unsigned char u8;
  typedef long i64;

  typedef struct BtCursor BtCursor;
  int sqlite3BtreeFirst(BtCursor *pCur, int *pRes);

  int sqlite3BtreeNext(BtCursor *pCur, int *pRes);

  int sqlite3BtreeKeySize(BtCursor *pCur, long *pSize);
  const char *sqlite3BtreeDataFetch(BtCursor *pCur, unsigned int *pAmt);

  void vdbe_ParseHeader(const char *row, u32 *aOffset, u32 *aType, int column);

  typedef struct Vdbe Vdbe;
  BtCursor *vdbeGetCursor(Vdbe *v, int k);

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

  typedef flexistring date_t;

  void *memcpy(void *dest, const void *src, size_t n);
  int strcmp(const char *s1, const char *s2);

  // specialized version
  void sqlite3VdbeSerialGetInt(const unsigned char *buf, u32 serial_type, int *place);
  void sqlite3VdbeSerialGetDouble(const unsigned char *buf, u32 serial_type, double *place);
  void sqlite3VdbeSerialGetChunk(const unsigned char *buf, u32 serial_type, str_chunk *chunk);
  void sqlite3VdbeSerialGetFlexiStr(const unsigned char *buf, u32 serial_type, flexistring *fstr);

  void sqlite3VdbeSerialGetDouble(
  const unsigned char *buf,     /* Buffer to deserialize from */
  u32 serial_type,              /* Serial type to deserialize */
  double *place                 /* Place to write value into */
  );

  void sqlite3VdbeSerialGetChunk(
    const unsigned char *buf,     /* Buffer to deserialize from */
    u32 serial_type,              /* Serial type to deserialize */
    str_chunk *chunk
  );

  void ljkWriteDouble(Vdbe *p, int iRes, double val);
  void ljkWriteInt(Vdbe *p, int iRes, int val);
  int ljkWriteStr(Vdbe *p, int iRes, const char *str);
  int ljkWriteFlexiStr(Vdbe *p, int iRes, const flexistring *str);
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

-- called by vdbe on preparation stage before looping
function kernel_entry_point(pVdbe, kernel_id)
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
      local pCrsr = C.vdbeGetCursor(pVdbe, cur.iCsr)
      local table_info = assert(cur.table_info)
      local iter_data = assert(K.input_iterators[iter_id], "table is known to kernel")
      local tbl_iter = createSQLiteTableIter(table_info, pCrsr,
                                             iter_data.unpack_row, iter_data.init_iter_fields)
      iters[iter_id] = tbl_iter
   end

   -- preparing result writer object
   local vdbe_resp = {
      iRes = ctx.iRes,
      iPlace = ctx.iRes,
      pVdbe = pVdbe,
      rewind = function(self) self.iPlace = self.iRes end,
      incPlace = function(self) self.iPlace = self.iPlace+1; end,
      putInt = function(self, num)
         C.ljkWriteInt(self.pVdbe, self.iPlace, num)
         self:incPlace()
      end,
      putDouble = function(self, num)
         C.ljkWriteDouble(self.pVdbe, self.iPlace, num)
         self:incPlace()
      end,
      putString = function(self, str)
        if type(str) == 'string' then
          C.ljkWriteStr(self.pVdbe, self.iPlace, str)
        elseif ffi.typeof(str) == t_flexistring_ref then
          C.ljkWriteFlexiStr(self.pVdbe, self.iPlace, str)
        else error("unknown arg type to putString")
        end
        self:incPlace()
      end,
      putDate = function(self, str)
        C.ljkWriteFlexiStr(self.pVdbe, self.iPlace, str)
        self:incPlace()
      end,
      putChar = function(self, chr)
        local str = string.char(chr)
        C.ljkWriteStr(self.pVdbe, self.iPlace, str)
        self:incPlace()
      end
   }

   local init_res
   if K.kernel_init then
     init_res = K.kernel_init(ctx.params)
   end
   -- registering result iterator and result writer for futher calls from iterator_entry_point
   ctx.result_iter = K.result_iterator(iters, ctx.params, init_res)
   ctx.result_builder = vdbe_resp

   if os.getenv("JIT_PROFILE") then
      prof.stop()
   end

   return 0
end

-- called by vdbe loop, returns single result row
function iterator_entry_point(pVdbe, kernel_id)
   dbg("hello from iterator_entry_point")

   if os.getenv("JIT_PROFILE") then
      prof.start(os.getenv("JIT_PROFILE"))
   end

   local ctx = assert(g_ctxs[kernel_id], "kernel context found")
   local K = assert(ctx.K, 'kernel found')

   local res = ctx.result_iter:next()

   if res == nil then
      C.ljkWriteInt(pVdbe, ctx.iStopFlag, 1);
   else
     ctx.result_builder:rewind()
     K.packResponse(ctx.result_builder, res)
   end

   if os.getenv("JIT_PROFILE") then
      prof.stop()
   end

   return 0
end
