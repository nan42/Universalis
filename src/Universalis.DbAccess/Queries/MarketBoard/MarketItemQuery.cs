namespace Universalis.DbAccess.Queries.MarketBoard;

public record MarketItemQuery
{
    public required int ItemId { get; init; }

    public required int WorldId { get; init; }
}