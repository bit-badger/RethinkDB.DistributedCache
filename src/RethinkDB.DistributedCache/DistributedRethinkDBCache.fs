namespace RethinkDB.DistributedCache

open System
open System.Text
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Caching.Distributed
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Options

/// IDistributedCache implementation utilizing RethinkDB
[<AllowNullLiteral>]
type DistributedRethinkDBCache (options : IOptions<DistributedRethinkDBCacheOptions>,
                                log     : ILogger<DistributedRethinkDBCache>) =
  
    /// Whether the environment has been checked to ensure that the database, table, and relevant indexes exist
    let mutable environmentChecked = false
    
    /// The last time expired entries were deleted
    let mutable lastExpiredCheck = DateTime.UtcNow - TimeSpan.FromDays 365.0
  
    do
        if isNull options then nullArg "options"
        if isNull options.Value then nullArg "options"
        let validity = options.Value.IsValid () |> Seq.fold (fun it err -> $"{it}\n{err}") ""
        if validity <> "" then invalidArg "options" $"Options are invalid:{validity}"

    /// Options
    let opts = options.Value

    /// Debug message
    let debug = Cache.debug opts log

    /// Make sure the RethinkDB database, table, expiration index exist
    let checkEnvironment cancelToken = backgroundTask {
        match environmentChecked with
        | true -> debug <| fun () -> "Skipping environment check because it has already been performed"
        | false ->
            do! Cache.Environment.check opts log cancelToken
            environmentChecked <- true
    }
    
    /// Remove entries from the cache that are expired
    let purgeExpired cancelToken = backgroundTask {
        let! lastCheck = Cache.Entry.purge opts log lastExpiredCheck cancelToken
        lastExpiredCheck <- lastCheck
    }
    
    /// Get the payload for the cache entry
    let getEntry key cancelToken = backgroundTask {
        do! checkEnvironment cancelToken
        let! result = Cache.Entry.get opts key cancelToken
        do! purgeExpired     cancelToken
        match result with
        | None ->
            debug <| fun () -> $"Cache key {key} not found"
            return null
        | Some entry ->
            debug <| fun () -> $"Cache key {key} found"
            return UTF8Encoding.UTF8.GetBytes entry.payload
    }
  
    /// Update the sliding expiration for a cache entry
    let refreshEntry key cancelToken = backgroundTask {
        do! checkEnvironment cancelToken
        let! _ = Cache.Entry.get opts key cancelToken
        do! purgeExpired cancelToken
    }
  
    /// Remove the specified cache entry
    let removeEntry key cancelToken = backgroundTask {
        do! checkEnvironment cancelToken
        do! Cache.Entry.remove opts key cancelToken
        do! purgeExpired cancelToken
    }
  
    /// Set the value of a cache entry
    let setEntry key payload options cancelToken = backgroundTask {
        do! Cache.Entry.set opts options key payload cancelToken
        do! purgeExpired cancelToken
    }
    
    /// Execute a task synchronously
    let runSync (task : CancellationToken -> Task<'T>) =
        task CancellationToken.None |> (Async.AwaitTask >> Async.RunSynchronously)
    
    interface IDistributedCache with
        member this.Get key = getEntry key |> runSync
        member this.GetAsync (key, cancelToken) = getEntry key cancelToken
        member this.Refresh key = refreshEntry key |> runSync
        member this.RefreshAsync (key, cancelToken) = refreshEntry key cancelToken
        member this.Remove key = removeEntry key |> runSync
        member this.RemoveAsync (key, cancelToken) = removeEntry key cancelToken
        member this.Set (key, value, options) = setEntry key value options |> runSync
        member this.SetAsync (key, value, options, cancelToken) = setEntry key value options cancelToken
