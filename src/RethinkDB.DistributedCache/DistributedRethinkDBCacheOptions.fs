namespace RethinkDB.DistributedCache

open RethinkDb.Driver.Net

/// Options to use to configure the RethinkDB cache
[<AllowNullLiteral>]
type DistributedRethinkDBCacheOptions() =
  /// The RethinkDB connection to use for caching operations
  member val Connection : IConnection = null with get, set

  /// The RethinkDB database to use (leave blank for connection default)
  member val Database = "" with get, set

  /// The RethinkDB table name to use for cache entries (defaults to "Cache")
  member val TableName = "" with get, set

  /// Whether this configuration is valid
  member this.IsValid () =
    seq {
      match this.Connection with null -> yield "Connection cannot be null" | _ -> ()
    }