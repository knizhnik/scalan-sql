local ffi = require 'ffi'
local C = ffi.C

local K = {}

function K.packResponse(response, result)
   response:putString(string.char(result.l_returnflag))
   response:putString(string.char(result.l_linestatus))

   response:putDouble(result.sum_qty)
   response:putDouble(result.sum_base_price)
   response:putDouble(result.sum_disc_price)
   response:putDouble(result.sum_charge)
   response:putDouble(result.avg_qty)
   response:putDouble(result.avg_price)
   response:putDouble(result.avg_disc)
   response:putInt(result.count_order)
end

local function initLineitemFields(iter, rowBuffer)
  iter.p_extendedprice = getFieldPtr("double *", "l_extendedprice", rowBuffer)
  iter.n_extendedprice = iter.tableInfo:getColumnId("l_extendedprice")
  iter.p_discount = getFieldPtr("double *", "l_discount", rowBuffer)
  iter.n_discount = iter.tableInfo:getColumnId("l_discount")
  iter.p_tax = getFieldPtr("double *", "l_tax", rowBuffer)
  iter.n_tax = iter.tableInfo:getColumnId("l_tax")
  iter.p_quantity = getFieldPtr("double *", "l_quantity", rowBuffer)
  iter.n_quantity = iter.tableInfo:getColumnId("l_quantity")

  iter.p_shipdate = getFieldPtr("flexistring *", "l_shipdate", rowBuffer)
  iter.n_shipdate = iter.tableInfo:getColumnId("l_shipdate")

  iter.n_returnflag = iter.tableInfo:getColumnId("l_returnflag")
  iter.n_linestatus = iter.tableInfo:getColumnId("l_linestatus")
end

local function unpackLineitemRow(iter, row)
  iter:parseDouble(row, iter.n_extendedprice, iter.p_extendedprice)
  iter:parseDouble(row, iter.n_discount, iter.p_discount)
  iter:parseDouble(row, iter.n_tax, iter.p_tax)
  iter:parseDouble(row, iter.n_quantity, iter.p_quantity)
  iter:parseString(row, iter.n_shipdate, iter.p_shipdate)
  iter:parseByteChunk(row, iter.n_returnflag, iter.pChunk, "l_returnflag")
  iter:parseByteChunk(row, iter.n_linestatus, iter.pChunk, "l_linestatus")
end

K.input_iterators = {
   ["lineitem_iter"] = {
      table = "lineitem",
      accessed_columns = {"l_extendedprice", "l_discount", "l_tax", "l_quantity", "l_shipdate", "l_returnflag", "l_linestatus"},
      init_iter_fields = initLineitemFields,
      unpack_row = unpackLineitemRow,
      buffer_ctype = 'LineitemProjection'
   }
}

K.result_columns = {
   l_returnflag = "string",
   l_linestatus = "string",
   sum_qty = "double",
   sum_base_price = "double",
   sum_disc_price = "double",
   sum_charge = "double",
   avg_qty = "double",
   avg_price = "double",
   avg_disc = "double",
   count_order = "int"
}

K.ffi_decls = [[
typedef unsigned date_t;
typedef char shipmode_t[10];
typedef unsigned long size_t;

typedef struct
{
     double l_extendedprice;
     double l_discount;
     double l_tax;
     double l_quantity;
     flexistring l_shipdate;
     int8_t   l_returnflag;
     int8_t   l_linestatus;
} LineitemProjection;

typedef struct
{
    double sum_qty;
    double sum_base_price;
    double sum_disc_price;
    double sum_charge;
    double avg_qty;
    double avg_price;
    double avg_disc;
    size_t count_order;
    int8_t   l_returnflag;
    int8_t   l_linestatus;
} Result;

typedef struct
{
    int8_t   l_returnflag;
    int8_t   l_linestatus;
} GroupBy;

typedef struct
{
    double sum_qty;
    double sum_base_price;
    double sum_disc_price;
    double sum_charge;
    double sum_disc;
    size_t count_order;
} Aggregate;
]]

local g_params

local function projectLineitemU(out, li)
    out.l_extendedprice = li.l_extendedprice
    out.l_discount = li.l_discount
    out.l_tax = li.l_tax
    out.l_quantity = li.l_quantity
    out.l_shipdate = li.l_shipdate
    out.l_returnflag = li.l_returnflag
    out.l_linestatus = li.l_linestatus
    return out
end

local function mapKey(key, lineitem)
    key.l_returnflag = lineitem.l_returnflag;
    key.l_linestatus = lineitem.l_linestatus;
end

local function newKey()
    local key = ffi.new("GroupBy")
    key.l_returnflag = 0
    key.l_linestatus = 0
    return key
end

local function mapValue(value, lineitem)
    value.sum_qty = lineitem.l_quantity;
    value.sum_base_price = lineitem.l_extendedprice;
    value.sum_disc_price = lineitem.l_extendedprice*(g_params[1]-lineitem.l_discount);
    value.sum_charge = lineitem.l_extendedprice*(g_params[2]-lineitem.l_discount)*(g_params[3]+lineitem.l_tax);
    value.sum_disc = lineitem.l_discount;
    value.count_order = 1;
end

local function newValue()
    local value = ffi.new("Aggregate")
    value.sum_qty = 0
    value.sum_base_price = 0
    value.sum_disc_price = 0
    value.sum_charge = 0
    value.sum_disc = 0
    value.count_order = 0
    return value
end

local function packKey(key)
   return string.char(key.l_returnflag) .. ',' .. string.char(key.l_linestatus)
end

local function reduceValue(dst, src)
    dst.sum_qty = dst.sum_qty + src.sum_qty;
    dst.sum_base_price = dst.sum_base_price + src.sum_base_price;
    dst.sum_disc_price = dst.sum_disc_price + src.sum_disc_price;
    dst.sum_charge = dst.sum_charge + src.sum_charge;
    dst.sum_disc = dst.sum_disc  + src.sum_disc;
    dst.count_order = dst.count_order + src.count_order;
end

local function resultProjectionU(out, res)
    local key = res.key
    local value = res.val

    out.l_returnflag = key.l_returnflag;
    out.l_linestatus = key.l_linestatus;
    out.sum_qty = value.sum_qty;
    out.sum_base_price = value.sum_base_price;
    out.sum_disc_price = value.sum_disc_price;
    out.sum_charge = value.sum_charge;
    out.avg_qty = tonumber(value.sum_qty) / tonumber(value.count_order);
    out.avg_price = tonumber(value.sum_base_price) / tonumber(value.count_order);
    out.avg_disc = tonumber(value.sum_disc) / tonumber(value.count_order);
    out.count_order = value.count_order;
end

local function pred(lineitem)
  return lineitem.l_shipdate < g_params[4]
end

local function order_pred(a1, a2)
  local k1 = a1.key
  local k2 = a2.key

  return k1.l_returnflag < k2.l_returnflag or
    k1.l_returnflag < k2.l_returnflag and k1.l_linestatus < k2.l_linestatus
end

function K.kernel_init(params)
  assert(type(params[4]) == 'string')
  g_params = params

end

function K.result_iterator(iters)
  local lineitems = iters["lineitem_iter"]
  return lineitems:filter(pred):mapReduceU(mapKey, mapValue, reduceValue, packKey, newKey, newValue):sort(order_pred):mapU(resultProjectionU, ffi.new('Result'))
end

return K
