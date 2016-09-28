namespace RethinkDB.DistributedCache

open Microsoft.Extensions.Caching.Distributed
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Options
open Newtonsoft.Json
open RethinkDb.Driver
open RethinkDb.Driver.Net
open System
open System.Text
open System.Threading.Tasks

// H/T: Suave
[<AutoOpen>]
module AsyncExtensions =
  type Microsoft.FSharp.Control.AsyncBuilder with
    /// An extension method that overloads the standard 'Bind' of the 'async' builder. The new overload awaits on
    /// a standard .NET task
    member x.Bind(t : Task<'T>, f:'T -> Async<'R>) : Async<'R> = async.Bind(Async.AwaitTask t, f)
    /// An extension method that overloads the standard 'Bind' of the 'async' builder. The new overload awaits on
    /// a standard .NET task which does not commpute a value
    member x.Bind(t : Task, f : unit -> Async<'R>) : Async<'R> = async.Bind(Async.AwaitTask t, f)

/// Persistence object for a cache entry
type CacheEntry = {
  /// The Id for the cache entry
  [<JsonProperty("id")>]
  Id : string
  /// The payload for the cache entry (as a UTF-8 string)
  Payload : string
  /// The ticks at which this entry expires
  ExpiresAt : int64
  /// The number of seconds in the sliding expiration
  SlidingExpiration : int
}

/// Record to update sliding expiration for an entry
type SlidingExpirationUpdate = { ExpiresAt : int64 }

/// IDistributedCache implementation utilizing RethinkDB
[<AllowNullLiteral>]
type DistributedRethinkDBCache(options : IOptions<DistributedRethinkDBCacheOptions>,
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

  /// Shorthand to get the database
  let database = match String.IsNullOrEmpty opts.Database with true -> r.Db() | db -> r.Db(db) 
  
  /// Default the table name to "Cache" if it is not provided
  let tableName = match String.IsNullOrEmpty opts.Database with true -> "Cache" | _ -> opts.TableName

  /// Shorthand to get the table
  let table = database.Table tableName

  /// The name of the cache
  let cacheName = 
    seq {
      match String.IsNullOrEmpty opts.Database with true -> () | _ -> yield opts.Database; yield "."
      yield tableName
    }
    |> Seq.reduce (+)

  /// Debug message
  let dbug text =
    match log.IsEnabled LogLevel.Debug with
    | true -> text () |> sprintf "[%s] %s" cacheName |> log.LogDebug
    | _ -> ()

  /// Make sure the RethinkDB database, table, expiration index exist
  let checkEnvironment () =
    async {
      match environmentChecked with
      | true -> dbug <| fun () -> "Skipping environment check because it has already been performed"
      | _ ->
          dbug <| fun () -> "|> Checking for proper RethinkDB cache environment"
          // Database
          match opts.Database with
          | "" -> dbug <| fun () -> "   Skipping database check because it was not specified"
          | db -> dbug <| fun () -> sprintf "   Checking for database %s existence..." db
                  let! dbs = r.DbList().RunResultAsync<string list>(opts.Connection)
                  match dbs |> List.contains db with
                  | true -> () 
                  | _ -> dbug <| fun () -> sprintf "   ...creating database %s..." db
                         do! r.DbCreate(db).RunResultAsync(opts.Connection)
                  dbug <| fun () -> "   ...done"
          // Table
          dbug <| fun () -> sprintf "   Checking for table %s existence..." tableName
          let! tables = database.TableList().RunResultAsync<string list>(opts.Connection)
          match tables |> List.contains tableName with
          | true -> ()
          | _ -> dbug <| fun () -> sprintf "   ...creating table %s..." tableName
                 do! database.TableCreate(tableName).RunResultAsync(opts.Connection)
          dbug <| fun () -> "   ...done"
          // Index
          dbug <| fun () -> sprintf "   Checking for index %s.ExpiresAt..." tableName
          let! indexes = table.IndexList().RunResultAsync<string list>(opts.Connection)
          match indexes |> List.contains "ExpiresAt" with
          | true -> ()
          | _ -> dbug <| fun () -> sprintf "   ...creating index ExpiresAt on table %s..." tableName
                 do! table.IndexCreate("ExpiresAt").RunResultAsync(opts.Connection)
          dbug <| fun () -> "   ...done"
          dbug <| fun () -> "|> RethinkDB cache environment check complete. Carry on..."
          environmentChecked <- true
    }

  /// Remove entries from the cache that are expired
  let purgeExpired () =
    async {
      let tix = DateTime.UtcNow.Ticks - 1L
      dbug <| fun () -> sprintf "Purging expired entries (<= %i)" tix
      do! table.Between(r.Minval, tix).OptArg("index", "ExpiresAt").Delete().RunResultAsync(opts.Connection)
    }
  
  /// Calculate ticks from now for the given number of seconds
  let ticksFromNow seconds = DateTime.UtcNow.Ticks + int64 (seconds * 10000000)

  /// Get the cache entry specified
  let getCacheEntry (key : string) =
    async {
      let! entry = table.Get(key).RunResultAsync<CacheEntry>(opts.Connection)
      return entry
    }

  /// Refresh (update expiration based on sliding expiration) the cache entry specified
  let refreshCacheEntry (entry : CacheEntry) =
    async {
      match entry.SlidingExpiration with
      | 0 -> ()
      | seconds -> do! table.Get(entry.Id)
                         .Update({ ExpiresAt = ticksFromNow seconds })
                         .RunResultAsync(opts.Connection)
    }

  /// Get the payload for the cache entry
  let getEntry key =
    async {
      do! checkEnvironment ()
      do! purgeExpired ()
      let! entry = getCacheEntry key
      match box entry with
      | null -> dbug <| fun () -> sprintf "Cache key %s not found" key
                return null
      | _ -> dbug <| fun () -> sprintf "Cache key %s found" key
             do! refreshCacheEntry entry
             return UTF8Encoding.UTF8.GetBytes entry.Payload
    }
  
  /// Update the sliding expiration for a cache entry
  let refreshEntry key =
    async {
      do! checkEnvironment ()
      let! entry = getCacheEntry key
      match box entry with null -> () | _ -> do! refreshCacheEntry entry
      do! purgeExpired ()
      return ()
    }
  
  /// Remove the specified cache entry
  let removeEntry (key : string) =
    async {
      do! checkEnvironment ()
      do! table.Get(key).Delete().RunResultAsync(opts.Connection)
      do! purgeExpired ()
    }
  
  /// Set the value of a cache entry
  let setEntry key payload (options : DistributedCacheEntryOptions) =
    async {
      do! checkEnvironment ()
      do! purgeExpired ()
      let addExpiration entry = 
        match true with
        | _ when options.SlidingExpiration.HasValue ->
            { entry with ExpiresAt          = ticksFromNow options.SlidingExpiration.Value.Seconds
                         SlidingExpiration  = options.SlidingExpiration.Value.Seconds }
        | _ when options.AbsoluteExpiration.HasValue ->
            { entry with ExpiresAt = options.AbsoluteExpiration.Value.UtcTicks }
        | _ when options.AbsoluteExpirationRelativeToNow.HasValue ->
            { entry with ExpiresAt = ticksFromNow options.AbsoluteExpirationRelativeToNow.Value.Seconds }
        | _ -> entry
      let entry = { Id                 = key
                    Payload            = UTF8Encoding.UTF8.GetString payload
                    ExpiresAt          = Int64.MaxValue
                    SlidingExpiration  = 0 }
                  |> addExpiration
      do! match box (getCacheEntry key) with
          | null -> table.Insert(entry).RunResultAsync(opts.Connection)
          | _ -> table.Get(key).Replace(entry).RunResultAsync(opts.Connection)
      return ()
    }

  interface IDistributedCache with
    member this.Get          key = getEntry     key |> Async.RunSynchronously
    member this.GetAsync     key = getEntry     key |> Async.StartAsTask
    member this.Refresh      key = refreshEntry key |> Async.RunSynchronously
    member this.RefreshAsync key = refreshEntry key |> Async.StartAsTask :> Task
    member this.Remove       key = removeEntry  key |> Async.RunSynchronously
    member this.RemoveAsync  key = removeEntry  key |> Async.StartAsTask :> Task
    member this.Set      (key, value, options) = setEntry key value options |> Async.RunSynchronously
    member this.SetAsync (key, value, options) = setEntry key value options |> Async.StartAsTask :> Task
