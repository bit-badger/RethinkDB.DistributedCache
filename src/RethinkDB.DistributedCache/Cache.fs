/// The implementation portion of this cache
module private RethinkDB.DistributedCache.Cache

open System
open System.Threading
open Microsoft.Extensions.Logging
open RethinkDB.DistributedCache
open RethinkDb.Driver.FSharp

/// The database name (blank uses connection default)
let db (cacheOpts : DistributedRethinkDBCacheOptions) = defaultArg (Option.ofObj cacheOpts.Database) ""

/// The table name; default to "Cache" if not provided
let tbl (cacheOpts : DistributedRethinkDBCacheOptions) =
    match defaultArg (Option.ofObj cacheOpts.TableName) "" with "" -> "Cache" | tbl -> tbl

/// The name of the cache
let table cacheOpts = match db cacheOpts with "" -> tbl cacheOpts | d -> $"{d}.{tbl cacheOpts}"

/// Debug message
let debug cacheOpts (log : ILogger) text =
    if log.IsEnabled LogLevel.Debug then log.LogDebug $"[{table cacheOpts}] %s{text ()}" 

/// Convert seconds to .NET ticks
let secondsToTicks seconds = int64 (seconds * 10000000)

/// Calculate ticks from now for the given number of seconds
let ticksFromNow seconds = DateTime.UtcNow.Ticks + (secondsToTicks seconds)


/// Ensure that the necessary environment exists for this cache
module Environment =
    
    /// Make sure the RethinkDB database, table, expiration index exist
    let check cacheOpts log (cancelToken : CancellationToken) = backgroundTask {
        let debug = debug cacheOpts log
        debug <| fun () -> "|> Checking for proper RethinkDB cache environment"
        // Database
        let db = db cacheOpts
        match db with
        | "" -> debug <| fun () -> "   Skipping database check; using connection default"
        | _ ->
            debug <| fun () -> $"   Checking for database {db} existence..."
            let! dbs = rethink<string list> { dbList; result cancelToken; withRetryDefault cacheOpts.Connection }
            if not (dbs |> List.contains db) then
                 debug <| fun () -> sprintf $"   ...creating database {db}..."
                 do! rethink { dbCreate db; write cancelToken; withRetryDefault; ignoreResult cacheOpts.Connection }
            debug <| fun () -> "   ...done"
        // Table
        let tbl   = tbl   cacheOpts
        let table = table cacheOpts
        debug <| fun () -> sprintf $"   Checking for table {tbl} existence..."
        let! tables = rethink<string list> { tableList db; result cancelToken; withRetryDefault cacheOpts.Connection }
        if not (tables |> List.contains tbl) then
            debug <| fun () -> sprintf $"   ...creating table {tbl}..."
            do! rethink { tableCreate table; write cancelToken; withRetryDefault; ignoreResult cacheOpts.Connection }
        debug <| fun () -> "   ...done"
        // Index
        debug <| fun () -> sprintf $"   Checking for index {tbl}.expiresAt..."
        let! indexes = rethink<string list> {
            withTable table
            indexList
            result cancelToken; withRetryDefault cacheOpts.Connection
        }
        if not (indexes |> List.contains expiresAt) then
            debug <| fun () -> sprintf $"   ...creating index {expiresAt} on table {tbl}..."
            do! rethink {
                withTable table
                indexCreate expiresAt
                write cancelToken; withRetryDefault; ignoreResult cacheOpts.Connection
            }
        debug <| fun () -> "   ...done"
        debug <| fun () -> "|> RethinkDB cache environment check complete. Carry on..."
    }


/// Cache entry manipulation functions
module Entry =
    
    open System.Text
    open Microsoft.Extensions.Caching.Distributed
    open RethinkDb.Driver.Model
    
    /// RethinkDB
    let r = RethinkDb.Driver.RethinkDB.R

    /// Remove entries from the cache that are expired
    let purge cacheOpts log lastCheck (cancelToken : CancellationToken) = backgroundTask {
        let table = table cacheOpts
        match DateTime.UtcNow - lastCheck > cacheOpts.DeleteExpiredInterval with
        | true ->
            let tix = ticksFromNow 0
            debug cacheOpts log <| fun () -> $"Purging expired entries (<= %i{tix})"
            do! rethink {
                withTable table
                between (r.Minval ()) tix [ BetweenOptArg.Index expiresAt ]
                delete
                write cancelToken; withRetryDefault; ignoreResult cacheOpts.Connection
            }
            return DateTime.UtcNow
        | false -> return lastCheck
    }
    
    /// Get the cache entry specified, refreshing sliding expiration then checking for expiration
    let get cacheOpts (key : string) (cancelToken : CancellationToken) = backgroundTask {
        let table = table cacheOpts
        let now   = ticksFromNow 0
        let! result = rethink<Result> {
            withTable table
            get key
            update (fun row ->
                r.HashMap(
                    expiresAt,
                    r.Branch(
                        // If we have neither sliding nor absolute expiration, do not change the expiry time
                        row.G(slidingExp).Le(0).Or(row.G(absoluteExp).Le(0)).Or(row.G(absoluteExp).Eq(row.G(expiresAt))),
                        row.G(expiresAt),
                        // If the sliding expiry increment exceeds the absolute expiry, use the absolute
                        row.G(expiresAt).Add(row.G(slidingExp)).Gt(row.G(absoluteExp)),
                        row.G(absoluteExp),
                        // Else adjust for the sliding expiry increment
                        row.G(slidingExp).Add(now))) :> obj) [ ReturnChanges All ]
            write cancelToken; withRetryDefault cacheOpts.Connection
        }
        match result.Changes.Count with
        | 0 -> return None
        | _ ->
            match (box >> Option.ofObj) (result.ChangesAs<CacheEntry>().[0].NewValue) with
            | Some boxedEntry ->
                let entry = unbox boxedEntry
                return if entry.expiresAt > now then Some entry else None
            | _ -> return None
    }
    
    let remove cacheOpts (key : string) (cancelToken : CancellationToken) = backgroundTask {
        let table = table cacheOpts
        do! rethink {
            withTable table
            get key
            delete
            write cancelToken; withRetryDefault; ignoreResult cacheOpts.Connection
        }
    }
    
    /// Set a cache entry
    let set cacheOpts (entryOpts : DistributedCacheEntryOptions) key (payload : byte[])
            (cancelToken : CancellationToken) =
        backgroundTask {
            let table = table cacheOpts
            let addExpiration entry = 
                match true with
                | _ when entryOpts.SlidingExpiration.HasValue ->
                    let expTicks = secondsToTicks entryOpts.SlidingExpiration.Value.Seconds
                    { entry with expiresAt = ticksFromNow 0 + expTicks; slidingExp = expTicks }
                | _ when entryOpts.AbsoluteExpiration.HasValue ->
                    let exp = entryOpts.AbsoluteExpiration.Value.UtcTicks
                    { entry with expiresAt = exp; absoluteExp = exp }
                | _ when entryOpts.AbsoluteExpirationRelativeToNow.HasValue ->
                    let exp = entryOpts.AbsoluteExpirationRelativeToNow.Value.Seconds
                    { entry with expiresAt = exp; absoluteExp = exp }
                | _ ->
                    let expTicks = secondsToTicks cacheOpts.DefaultSlidingExpiration.Seconds
                    { entry with expiresAt = ticksFromNow 0 + expTicks; slidingExp = expTicks }
            let entry =
                { id          = key
                  payload     = UTF8Encoding.UTF8.GetString payload
                  expiresAt   = Int64.MinValue
                  slidingExp  = 0L
                  absoluteExp = 0L
                }
                |> addExpiration
            do! rethink {
                withTable table
                replace entry
                write cancelToken; withRetryDefault; ignoreResult cacheOpts.Connection
            }
        }
