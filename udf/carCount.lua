local function carCount(rec)
    return rec.carCount
end

local function max(a, b)
    return (a > b) and a or b
end

function findMax(stream)
    return stream : map(carCount) : reduce(max);
end
