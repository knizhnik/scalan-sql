package.path = package.path .. ";./?.lua"
require("oop")

local ffi = require 'ffi'
local C = ffi.C

Iter = createClass()
function Iter:next() error"NYI: Iter.next" end

-- Print
local PrintIter = Iter:new( {source = {} })
function PrintIter:next()
  local v = self.source:next()
  print(v)
  return v
end

function Iter:print()
  return PrintIter:new({source = self})
end

-- Count
local CountIter = Iter:new( {source = {} })
function PrintIter:next()
  local v = self.source:next()
  if v == nil then
    print(self.label, self.cnt)
  end
  self.cnt = self.cnt + 1

  return v
end

function Iter:count(lbl)
  return PrintIter:new({source = self, label = lbl, cnt = 0})
end

-- Map
local MapIter = Iter:new({ source = {}; mapFunc = nil; })
function Iter:map(f)
    return MapIter:new({ source = self, mapFunc = f })
end

function MapIter:next()
  local elem = self.source:next()
  return elem and self.mapFunc(elem)
end

-- MapU
local MapUIter = Iter:new({ source = {}; mapFunc = nil; })
function Iter:mapU(f, state)
    return MapUIter:new({ source = self, mapFunc = f, state = state })
end

function MapUIter:next()
    local elem = self.source:next()
    if elem then
      self.mapFunc(self.state, elem)
      return self.state
    end
    return nil
end

-- FlatMap
local FlatMapIter = Iter:new({ source = {}; mapFunc = nil; inner = nil })
function Iter:flatMap(f)
    return FlatMapIter:new({ source = self, mapFunc = f })
end

function FlatMapIter:next()
  local elem
  repeat
      if not self.inner then
          local source = self.source:next()
          if source == nil then return nil end
          self.inner = self.mapFunc(source)
      end
      elem = self.inner:next()
      if elem == nil then
          self.inner = nil
      end
  until not (elem == nil)
  return elem
end

-- Filter
local FilterIter = Iter:new({ source = {}; predicateFunc = nil; })
function Iter:filter(p)
    return FilterIter:new({ source = self; predicateFunc = p})
end

function FilterIter:next()
  local elem = self.source:next()
  while elem and not self.predicateFunc(elem) do
    elem = self.source:next()
  end
  return elem
end

-- Reduce
local ReduceIter = Iter:new()
function Iter:reduce(f, init)
   return ReduceIter:new({ source = self; reduceFunc = f; val0 = init() })
end

function ReduceIter:next()
   local val = self.val0
   self.val0 = nil
   while true do
      local elem = self.source:next()
      if elem == nil then break end
      val = self.reduceFunc(val, elem)
   end
   return val
end

-- ReduceU
local ReduceIterU = Iter:new()
function Iter:reduceU(f, init)
   return ReduceIterU:new({ source = self; reduceProc = f; val = init(); done=false })
end

function ReduceIterU:next()
   if self.done then return nil end

   while true do
      local elem = self.source:next()
      if elem == nil then break end
      self.reduceProc(self.val, elem)
   end
   self.done = true
   return self.val
end

-- Join
local JoinIter = Iter:new()
function Iter:join(inner_iter, outer_key, inner_key, clone_inner_elt)
  local inner_tbl = {}
  local in_elt = inner_iter:next()

  repeat
    if in_elt == nil then break end
    local in_key = inner_key(in_elt)
    local val = inner_tbl[in_key]
    local elt_dup = clone_inner_elt(in_elt)

--    if val then -- need to store all variants
      if (type(val) == "table") then
        table.insert(val, elt_dup)
      else
        inner_tbl[in_key] = { elt_dup } -- {val, in_copy}
      end
--    else
--      inner_tbl[in_key] = in_copy
--    end

    in_elt = inner_iter:next()
  until not in_elt

  return JoinIter:new({ outer_iter = self, outer_key = outer_key, inner_tbl = inner_tbl})
end


function JoinIter:next()
  local elem
  repeat
    if not self.in_values then
      self.out_elt = self.outer_iter:next()
      if self.out_elt == nil then
        inner_tbl = nil
        return nil
      end

      local out_k = self.outer_key(self.out_elt)
      self.in_values = self.inner_tbl[out_k]

      if self.in_values then
        self.in_idx = #self.in_values
      end
    end
    local in_val = self.in_values
--    print("iv", in_val, self.in_idx, self.out_elt)
    if type(in_val) == 'table' then
--      print("tbl", #in_val)
--      print_table(in_val)

      elem = { self.out_elt, in_val[self.in_idx] }
      self.in_idx = self.in_idx - 1
--      print("rest", #in_val)
      if self.in_idx == 0 then
--        print("zeroing")
        self.in_values = nil
      end
--    elseif in_val ~= nil then
--      print("ivt", type(in_val))
--      self.in_values = nil
--      elem = { self.out_elt, self.in_val }
    end
  until elem
  return elem
end

-- Sort
local ArrayIter = Iter:new({array = {}, i=0})
function createArrayIter(tbl)
  return ArrayIter:new({array = tbl})
end

function ArrayIter:next()
  self.i = self.i + 1

  return self.array[self.i]
end

-- sort by Less-than predicate
function Iter:sort(pred)
  local tbl = {}
  while true do
    local elt = self:next()
    if not elt then break end
    table.insert(tbl, elt)
  end
  table.sort(tbl, pred)
  return createArrayIter(tbl)
end

-- sorting by c-like predicate
function Iter:sortBy(int_pred)
  local pred = function(a,b)
    return int_pred(a,b) < 0
  end

  return self:sort(pred)
end

-- TableIter
TableIter = Iter:new({ nextFunc = nil, sourceTable = {}, currKey = nil })
function TableIter:next()
    local i,v
    i, v = self.nextFunc(self.sourceTable, self.currKey)
    self.currKey = i
    if i == nil then return nil else return {key = i, val = v} end
end

function createTableIter (t)
    local next, ht1, i = pairs(t)
    return TableIter:new({ nextFunc = next, sourceTable = t, currKey = i})
end

-- MapReduce
local function createMapReduceIter(source, mapKey, mapValue, reduceValue, packKey, cloneKey)
  assert(cloneKey, "Clone function not passed")
  local ht = {}
  local kt = {}
  local elem = source:next()
  while elem do
    local key = mapKey(elem)
    local packedKey = packKey(key)
    local existingV = ht[packedKey]
    if existingV then
      ht[packedKey] = reduceValue(existingV, mapValue(elem))
    else
      ht[packedKey] = mapValue(elem)
      kt[packedKey] = cloneKey(key)
    end
    elem = source:next()
  end

  local res_t = {}
  for pk,value in pairs(ht) do
    table.insert(res_t, {key = kt[pk], val = value})
  end

  return createArrayIter(res_t)
end

function Iter:mapReduce(mapKey, mapValue, reduceValue, packKey, cloneKey)
    return createMapReduceIter(self, mapKey, mapValue, reduceValue, packKey, cloneKey)
end

-- MapReduceU
-- (iter: Row*, mapKey: Row => K, mapValue: Row => V, reduceValue: (V,V) => V, packKey: K => String) => DF[(K,V)]
local function createMapReduceUIter (source, mapKey, mapValue, reduceValue, packKey, newKey, newValue)
  local ht = {}
  local kt = {}

  local elem = source:next()
  while elem do
    local k = newKey()
    mapKey(k, elem)
    local packedKey = packKey(k)
    local existingV = ht[packedKey]
    if existingV then
      local newV = newValue()
      mapValue(newV, elem)
      reduceValue(existingV, newV)
    else
      local newV = newValue()
      mapValue(newV, elem)
      ht[packedKey] = newV
      kt[packedKey] = k
    end
    elem = source:next()
  end

  local res_t = {}
  for pk,value in pairs(ht) do
    table.insert(res_t, {key = kt[pk], val = value})
  end

  return createArrayIter(res_t)
end

function Iter:mapReduceU(mapKey, mapValue, reduceValue, packKey, newKey, newValue)
    return createMapReduceUIter(self, mapKey, mapValue, reduceValue, packKey, newKey, newValue)
end

-- Array
function Iter:toArray()
  local res = {}
  local i = 1
  local elem = self:next()
  while elem do
    res[i] = elem
    i = i + 1
    elem = self:next()
  end
  return res
end

-- Foreach
function Iter:foreach(action)
  local elem = self:next()
  while elem do
    action(elem)
    elem = self:next()
  end
end

-- Range
local RangeIter = Iter:new({ length = 0, position = -1 })
function Iter.range(n) return RangeIter:new({ length = n }) end

function RangeIter:next()
    self.position = self.position + 1
    return self.position < self.length and self.position or nil
end
