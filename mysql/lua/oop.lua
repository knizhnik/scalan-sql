DEBUG = false
function createClass(c, name)
    c = c or {}
--    if name then
--      c.__tostring = function() return name end
--    end
    c.new = function(self, o, name)
        o = o or {}    -- создает таблицу, если пользователь ее не предоставил
--        if name then
--            o.__tostring = function() return name end
--        end
        setmetatable(o, self)
        self.__index = self
        if o.init then o:init() end
        return o
    end
    return c
end

ClosureBase = createClass({}, "ClosureBase")
ClosureBase.__call = function (self, ...)
  return self.func(self.closure, ...)
end

function Closure(closure, f)
  return ClosureBase:new({ closure = closure, func = f })
end

-- ищет 'k' в списке таблиц 'plist'
local function search (k, plist)
    for i = 1, #plist do
        local v = plist[i][k]    -- пробует'i'-ый суперкласс
        if v then return v end
    end
    --        local v = plist[1][k]    -- пробует'i'-ый суперкласс
    --        if v then return v end
    --        local v = plist[2][k]    -- пробует'i'-ый суперкласс
    --        if v then return v end
end

function extendsClasses (...)
    local c = {}    -- новый класс
    local parents = {...}

    -- класс будет искать каждый метод в списке своих родительских классов
    setmetatable(c, {__index = function (t, k)
        local v = search(k, parents)
        t[k] = v -- сохраняет для следующего обращения
        return v
    end})

    -- подготавливает 'c' стать метатаблицей для своих экземпляров
    c.__index = c

    -- определяет новый конструктор для этого нового класс
    function c:new (o)
        o = o or {}
        setmetatable(o, c)
        return o
    end

    return c    -- возвращает новый класс
end


