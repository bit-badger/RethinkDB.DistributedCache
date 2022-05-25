# RethinkDB.DistributedCache

This package provides an [ASP.NET Core cache implementation][cache] backed by [RethinkDB][].

## Quick Start

### C#

```csharp
using RethinkDB.DistributedCache;

IConnection conn = yourRethinkDBConnection; // set up connection first
 
// within ASP.NET Core service configuration
builder.Services.AddDistributedRethinkDBCache(opts => opts.Connection = conn);

// it will be used by other processes, such as sessions
```

### F#

```fsharp
open RethinkDB.DistributedCache

let conn = yourRethinkDBConnection // set up connection first

// within ASP.NET Core service configuration
let _ = builder.Services.AddDistributedRethinkDBCache(fun opts ->
    opts.Connection <- conn)

// it will be used by other processes, such as sessions
```

## More Information

More information and details can be found at the [project site][].

[cache]: https://docs.microsoft.com/en-us/aspnet/core/performance/caching/distributed
[RethinkDB]: https://www.rethinkdb.com
[project site]: https://bitbadger.solutions/open-source/rethinkdb-distributedcache/
