namespace RethinkDB.DistributedCache

open Microsoft.Extensions.Caching.Distributed
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Options
open RethinkDb.Driver
open RethinkDb.Driver.FSharp
open System
open System.Text
open System.Threading
open System.Threading.Tasks


/// Persistence object for a cache entry
[<CLIMutable; NoComparison; NoEquality>]
type CacheEntry =
    {   /// The ID for the cache entry
        id : string
        
        /// The payload for the cache entry (as a UTF-8 string)
        payload : string
        
        /// The ticks at which this entry expires
        expiresAt : int64
        
        /// The number of seconds in the sliding expiration
        slidingExpiration : int
    }


/// IDistributedCache implementation utilizing RethinkDB
[<AllowNullLiteral>]
type DistributedRethinkDBCache (options : IOptions<DistributedRethinkDBCacheOptions>,
                                log     : ILogger<DistributedRethinkDBCache>) =
  
    /// RethinkDB
    static let r = RethinkDB.R

    /// Whether the environment has been checked to ensure that the database, table, and relevant indexes exist
    static let mutable environmentChecked = false
  
    do
        match options with
        | null | _ when isNull options.Value -> nullArg "options"
        | _ when isNull options.Value.Connection -> nullArg "Connection"
        | _ -> ()

    /// Options
    let opts = options.Value

    /// The database name (blank uses connection default)
    let db = defaultArg (Option.ofObj opts.Database) ""
    
    /// The table name; default to "Cache" if not provided
    let tbl = match defaultArg (Option.ofObj opts.TableName) "" with "" -> "Cache" | tbl -> tbl

    /// The name of the cache
    let table = 
        seq {
            match db with "" -> () | _ -> $"{db}."
            tbl
        }
        |> Seq.reduce (+)

    /// Debug message
    let dbug text =
        if log.IsEnabled LogLevel.Debug then log.LogDebug $"[{table}] %s{text ()}" 

    /// Make sure the RethinkDB database, table, expiration index exist
    let environmentCheck (_ : CancellationToken) =
        backgroundTask {
            dbug <| fun () -> "|> Checking for proper RethinkDB cache environment"
            // Database
            match db with
            | "" -> dbug <| fun () -> "   Skipping database check because it was not specified"
            | _ ->
                dbug <| fun () -> $"   Checking for database {db} existence..."
                let! dbs = rethink<string list> { dbList; result; withRetryDefault opts.Connection }
                if not (dbs |> List.contains db) then
                     dbug <| fun () -> sprintf $"   ...creating database {db}..."
                     do! rethink { dbCreate db; write; withRetryDefault; ignoreResult opts.Connection }
                dbug <| fun () -> "   ...done"
            // Table
            dbug <| fun () -> sprintf $"   Checking for table {tbl} existence..."
            let! tables = rethink<string list> { tableList db; result; withRetryDefault opts.Connection }
            if not (tables |> List.contains tbl) then
                dbug <| fun () -> sprintf $"   ...creating table {tbl}..."
                do! rethink { tableCreate table; write; withRetryDefault; ignoreResult opts.Connection }
            dbug <| fun () -> "   ...done"
            // Index
            dbug <| fun () -> sprintf $"   Checking for index {tbl}.expiresAt..."
            let! indexes = rethink<string list> {
                withTable table
                indexList
                result; withRetryDefault opts.Connection
            }
            if not (indexes |> List.contains "expiresAt") then
                dbug <| fun () -> sprintf $"   ...creating index expiresAt on table {tbl}..."
                do! rethink {
                    withTable table
                    indexCreate "expiresAt"
                    write; withRetryDefault; ignoreResult opts.Connection
                }
            dbug <| fun () -> "   ...done"
            dbug <| fun () -> "|> RethinkDB cache environment check complete. Carry on..."
            environmentChecked <- true
        }

    /// Make sure the RethinkDB database, table, expiration index exist
    let checkEnvironment (cnxToken : CancellationToken) =
        backgroundTask {
            match environmentChecked with
            | true -> dbug <| fun () -> "Skipping environment check because it has already been performed"
            | false -> do! environmentCheck cnxToken
        }
    
    /// Remove entries from the cache that are expired
    let purgeExpired (_ : CancellationToken) =
        backgroundTask {
            let tix = DateTime.UtcNow.Ticks - 1L
            dbug <| fun () -> $"Purging expired entries (<= %i{tix})"
            do! rethink {
                withTable table
                between (r.Minval ()) tix [ BetweenOptArg.Index "expiresAt" ]
                delete
                write; withRetryDefault; ignoreResult opts.Connection
            }
        }
  
    /// Calculate ticks from now for the given number of seconds
    let ticksFromNow seconds = DateTime.UtcNow.Ticks + int64 (seconds * 10000000)

    /// Get the cache entry specified
    let getCacheEntry (key : string) (_ : CancellationToken) =
        rethink<CacheEntry> {
            withTable table
            get key
            resultOption; withRetryDefault opts.Connection
        }

    /// Refresh (update expiration based on sliding expiration) the cache entry specified
    let refreshCacheEntry (entry : CacheEntry) (_ : CancellationToken) =
        backgroundTask {
            if entry.slidingExpiration > 0 then
                do! rethink {
                    withTable table
                    get entry.id
                    update [ "expiresAt", ticksFromNow entry.slidingExpiration :> obj ]
                    write; withRetryDefault; ignoreResult opts.Connection
                }
        }

    /// Get the payload for the cache entry
    let getEntry key (cnxToken : CancellationToken) =
        backgroundTask {
            cnxToken.ThrowIfCancellationRequested ()
            do! checkEnvironment cnxToken
            do! purgeExpired     cnxToken
            match! getCacheEntry key cnxToken with
            | None ->
                dbug <| fun () -> $"Cache key {key} not found"
                return null
            | Some entry ->
                dbug <| fun () -> $"Cache key {key} found"
                do! refreshCacheEntry entry cnxToken
                return UTF8Encoding.UTF8.GetBytes entry.payload
        }
  
    /// Update the sliding expiration for a cache entry
    let refreshEntry key (cnxToken : CancellationToken) =
        backgroundTask {
            cnxToken.ThrowIfCancellationRequested ()
            do! checkEnvironment cnxToken
            match! getCacheEntry key cnxToken with None -> () | Some entry -> do! refreshCacheEntry entry cnxToken
            do! purgeExpired cnxToken
            return ()
        }
  
    /// Remove the specified cache entry
    let removeEntry (key : string) (cnxToken : CancellationToken) =
        backgroundTask {
            cnxToken.ThrowIfCancellationRequested ()
            do! checkEnvironment cnxToken
            do! rethink {
                withTable table
                get key
                delete
                write; withRetryDefault; ignoreResult opts.Connection
            }
            do! purgeExpired cnxToken
        }
  
    /// Set the value of a cache entry
    let setEntry key (payload : byte[]) (options : DistributedCacheEntryOptions) (cnxToken : CancellationToken) =
        backgroundTask {
            cnxToken.ThrowIfCancellationRequested ()
            do! checkEnvironment cnxToken
            do! purgeExpired cnxToken
            let addExpiration entry = 
                match true with
                | _ when options.SlidingExpiration.HasValue ->
                    { entry with expiresAt          = ticksFromNow options.SlidingExpiration.Value.Seconds
                                 slidingExpiration  = options.SlidingExpiration.Value.Seconds }
                | _ when options.AbsoluteExpiration.HasValue ->
                    { entry with expiresAt = options.AbsoluteExpiration.Value.UtcTicks }
                | _ when options.AbsoluteExpirationRelativeToNow.HasValue ->
                    { entry with expiresAt = ticksFromNow options.AbsoluteExpirationRelativeToNow.Value.Seconds }
                | _ -> entry
            let entry =
                { id                 = key
                  payload            = UTF8Encoding.UTF8.GetString payload
                  expiresAt          = Int64.MaxValue
                  slidingExpiration  = 0
                }
                |> addExpiration
            do! rethink {
                withTable table
                replace entry
                write; withRetryDefault; ignoreResult opts.Connection
            }
        }
    
    /// Execute a task synchronously
    let runSync (task : CancellationToken -> Task<'T>) =
        task CancellationToken.None |> (Async.AwaitTask >> Async.RunSynchronously)
    
    interface IDistributedCache with
        member this.Get key = getEntry key |> runSync
        member this.GetAsync (key, cnxToken) = getEntry key cnxToken
        member this.Refresh key = refreshEntry key |> runSync
        member this.RefreshAsync (key, cnxToken) = refreshEntry key cnxToken
        member this.Remove key = removeEntry key |> runSync
        member this.RemoveAsync (key, cnxToken) = removeEntry key cnxToken
        member this.Set (key, value, options) = setEntry key value options |> runSync
        member this.SetAsync (key, value, options, cnxToken) = setEntry key value options cnxToken
