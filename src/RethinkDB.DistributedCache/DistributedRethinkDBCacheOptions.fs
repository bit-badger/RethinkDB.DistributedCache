namespace RethinkDB.DistributedCache

open System
open Microsoft.Extensions.Options
open RethinkDb.Driver.Net

/// Options to use to configure the RethinkDB cache
[<AllowNullLiteral>]
type DistributedRethinkDBCacheOptions () =
    
    /// The RethinkDB connection to use for caching operations
    member val Connection : IConnection = null with get, set

    /// The RethinkDB database to use; leave blank for connection default
    member val Database = "" with get, set

    /// The RethinkDB table name to use for cache entries; defaults to "Cache"
    member val TableName = "" with get, set
    
    /// How frequently we will delete expired cache items; default is 30 minutes
    member val DeleteExpiredInterval = TimeSpan.FromMinutes 30.0 with get, set
    
    /// The default sliding expiration for items, if none is provided; default is 20 minutes
    member val DefaultSlidingExpiration = TimeSpan.FromMinutes 20.0 with get, set

    /// Whether this configuration is valid
    member this.IsValid () =
        seq {
            if isNull this.Connection then "Connection cannot be null"
            if this.DeleteExpiredInterval <= TimeSpan.Zero then "DeleteExpiredInterval must be positive"
            if this.DefaultSlidingExpiration <= TimeSpan.Zero then "DefaultSlidingExpiration must be positive"
        }
    
    interface IOptions<DistributedRethinkDBCacheOptions> with
        member this.Value = this
        