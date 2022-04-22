namespace RethinkDB.DistributedCache

/// Persistence object for a cache entry
[<CLIMutable; NoComparison; NoEquality>]
type CacheEntry =
    {   /// The ID for the cache entry
        id : string
        
        /// The payload for the cache entry (as a UTF-8 string)
        payload : string
        
        /// The ticks at which this entry expires
        expiresAt : int64
        
        /// The number of ticks in the sliding expiration
        slidingExp : int64
        
        /// The ticks for absolute expiration
        absoluteExp : int64
    }

/// Field names for the above
[<AutoOpen>]
module private CacheEntry =
    [<Literal>]
    let expiresAt = "expiresAt"
    [<Literal>]
    let slidingExp = "slidingExp"
    [<Literal>]
    let absoluteExp = "absoluteExp"
