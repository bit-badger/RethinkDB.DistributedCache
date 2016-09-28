/// Extensions for <see cref="IServiceCollection" /> to add the RethinkDB cache
[<AutoOpen>]
[<System.Runtime.CompilerServices.Extension>]
module RethinkDB.DistributedCache.IServiceCollectionExtensions

open Microsoft.Extensions.Caching.Distributed
open Microsoft.Extensions.DependencyInjection
open System

type IServiceCollection with

  member this.AddDistributedRethinkDBCache(options : Action<DistributedRethinkDBCacheOptions>) =
    match options with null -> nullArg "options" | _ -> ()
    ignore <| this.AddOptions ()
    ignore <| this.Configure options
    ignore <| this.Add (ServiceDescriptor.Transient<IDistributedCache, DistributedRethinkDBCache>())
    this

/// <summary>
/// Add RethinkDB options to the services collection
/// </summary>
/// <param name="options">An action to set the options for the cache</param>
/// <returns>The given <see cref="IServiceCollection" /> for further manipulation</returns>
[<System.Runtime.CompilerServices.Extension>]
let AddDistributedRethinkDBCache (this : IServiceCollection, options : Action<DistributedRethinkDBCacheOptions>) =
  this.AddDistributedRethinkDBCache options