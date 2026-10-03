-- wrk script: send N requests per write (HTTP/1.1 pipelining), like the TechEmpower plaintext test.
-- usage: wrk -s scripts/pipeline.lua http://host:port/path -- 16
init = function(args)
  local depth = tonumber(args[1]) or 16
  local r = {}
  for i = 1, depth do r[i] = wrk.format(nil) end
  req = table.concat(r)
end
request = function() return req end
