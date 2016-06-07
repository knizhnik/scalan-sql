local ffi = require 'ffi'

ffi.cdef [[
typedef struct Parse Parse;
typedef struct Table Table;

void ljGenReadLock(Parse *pParse, Table *pTab);
int ljGenCursorOpen(Parse *pParse, Table *pTab, int maxCol);
void ljGenCursorClose(Parse *pParse, int iCsr);

void ljGenKernelCall(Parse *pParse, const char *kernel_name, int arg1, int arg2, int arg3);

int ljReserveMem(Parse *pParse, int n_cells);
void ljGenResultRow(Parse *pParse, int place, int n_results);

void ljGenInteger(Parse *pParse, int cell, int val);

int ljMakeLabel(Parse *pParse);
void ljResolveLabel(Parse *pParse, int x);

void ljGenGoto(Parse *pParse, int label);
void ljGenGotoIfPos(Parse *pParse, int cell, int label, int decr);

]]

local C = ffi.C

function dbg(...)
   if os.getenv("DEBUG") == '1' then
      print(...)
   end
end

-- Test Vdbe Building
function print_table(t,off)
   off = off or 0
   local offset = string.rep(' ', (off + 1) * 8)
   for i,v in pairs(t) do
     if type(v) == 'table' then
         io.stderr:write(offset, i, ' TABLE:\n')
         print_table(v, off+1)
      else
         io.stderr:write(offset, i, ' ',  tostring(v),'\n')
      end
   end
end

local function find_max_column(columns_data, used_columns)
   assert(columns_data)
   assert(used_columns)
   local max_idx = 0
   for _,c in ipairs(used_columns) do
     local idx = columns_data[c]
     assert(idx, "unknown column: " .. c)
      if idx > max_idx then
         max_idx = idx
      end
   end
   return max_idx
end

function table.size(tbl)
   local s = 0
   for i,v in pairs(tbl) do
      s = s+1
   end
   return s
end

function common_vdbe_builder(pParse, pSel, kernel_code, kernel_id)
   dbg("hello from common_vdbe_builder")

   g_kernels = g_kernels or {}
   local K
   if not g_kernels[kernel_id] then
      dbg("compiling kernel id ", kernel_id)
      K = loadstring(kernel_code, "kernel_code")()
      g_kernels[kernel_id] = K
      ffi.cdef(K.ffi_decls)
   else
      K = g_kernels[kernel_id]
   end

   local vdbe_tables = getSqlTables(pSel)
   local vdbe_columns = getSqlTableColumns(pSel)
   local params = getKernelParameters()
   local cursors = {}
   -- gen opening cursors
   for k_iter_id, k_tbl_data in pairs(K.input_iterators) do
     local t_name = k_tbl_data.table
     assert(t_name, "input iterator should supply table name")
     assert(vdbe_tables[k_tbl_data.table], "non-existent table for iterator " .. k_iter_id)
     local pTbl = assert(vdbe_tables[k_tbl_data.table].ptr, "got Table ptr")
      C.ljGenReadLock(pParse, pTbl)
      local max_idx = find_max_column(vdbe_columns[t_name], k_tbl_data.accessed_columns)
      local iCsr = C.ljGenCursorOpen(pParse, pTbl, max_idx)
      local tbl_info = {
         maxColumn = max_idx,
         bufferCTypeName = k_tbl_data.buffer_ctype,
         getColumnId = function(self, c) return vdbe_columns[t_name][c] end
      }
      cursors[k_iter_id] = { table_info = tbl_info, iCsr = iCsr }
   end

   C.ljGenKernelCall(pParse, "kernel_entry_point", kernel_id,0,0)
   -- gen place for results
   local n_res = table.size(K.result_columns)
   local res_place = C.ljReserveMem(pParse, n_res)
   -- gen iterator calling loop
   local stop_flag = C.ljReserveMem(pParse, 1)
   local loop_lbl = C.ljMakeLabel(pParse)
   local stop_lbl = C.ljMakeLabel(pParse)

   C.ljGenInteger(pParse, stop_flag, 0)
   C.ljResolveLabel(pParse, loop_lbl)
   C.ljGenKernelCall(pParse, "iterator_entry_point", kernel_id,0,0)
   C.ljGenGotoIfPos(pParse, stop_flag, stop_lbl, 0)
   C.ljGenResultRow(pParse, res_place, n_res)
   C.ljGenGoto(pParse, loop_lbl)
   C.ljResolveLabel(pParse, stop_lbl)
   for _,cur_data in pairs(cursors) do
      C.ljGenCursorClose(pParse, cur_data.iCsr)
   end

   -- storing context
   g_ctxs = g_ctxs or {}
   g_ctxs[kernel_id] = {
      K = K,
      cursors = cursors,
      iStopFlag = stop_flag,
      iRes = res_place,
      params = params
   }

end
