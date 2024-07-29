using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Universalis.DbAccess.Queries.MarketBoard;
using Universalis.Entities.MarketBoard;

namespace Universalis.DbAccess.MarketBoard;

public class AggregatedMarketBoardDataDbAccess : IAggregatedMarketBoardDataDbAccess
{
    private readonly IListingStore _listingStore;
    private readonly ISaleStore _saleStore;

    public AggregatedMarketBoardDataDbAccess(IListingStore listingStore, ISaleStore saleStore)
    {
        _listingStore = listingStore;
        _saleStore = saleStore;
    }

    public Task<MinListing> GetMinListing(int worldId, int itemId, CancellationToken cancellationToken = default)
    {
        return _listingStore.GetMinListing(worldId, itemId, cancellationToken);
    }

    public Task<MinListing.Entry> GetMinListing(string dcRegion, int itemId, CancellationToken cancellationToken = default)
    {
        return _listingStore.GetMinListingForDcOrRegion(dcRegion, itemId, cancellationToken);
    }

    public Task<IEnumerable<MarketItem>> RetrieveWorldUploadTimes(ICollection<MarketItemQuery> queries, CancellationToken cancellationToken)
    {
        return _listingStore.GetCachedUploadTime(queries, cancellationToken);
    }

    public Task<RecentSale> GetMostRecentSaleInWorld(int worldId, int itemId, bool hq, CancellationToken cancellationToken = default)
    {
        return _saleStore.GetMostRecentSaleInWorld(worldId, itemId, hq, cancellationToken);
    }

    public Task<RecentSale> GetMostRecentSaleInDatacenterOrRegion(string dcRegion, int itemId, bool hq, CancellationToken cancellationToken = default)
    {
        return _saleStore.GetMostRecentSaleInDatacenterOrRegion(dcRegion, itemId, hq, cancellationToken);
    }

    public Task<(TradeVelocity Nq, TradeVelocity Hq)> RetrieveUnitTradeVelocity(string worldIdDcRegion, int itemId, DateOnly from, DateOnly to, CancellationToken cancellationToken)
    {
        return _saleStore.RetrieveUnitTradeVelocity(worldIdDcRegion, itemId, from, to, cancellationToken);
    }
}