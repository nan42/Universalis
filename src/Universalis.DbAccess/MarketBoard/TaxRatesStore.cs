﻿using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Universalis.Entities.MarketBoard;

namespace Universalis.DbAccess.MarketBoard;

public class TaxRatesStore : ITaxRatesStore
{
    private readonly IPersistentRedisMultiplexer _redis;
    private readonly ICacheRedisMultiplexer _cache;
    private readonly ILogger<TaxRatesStore> _logger;

    public TaxRatesStore(IPersistentRedisMultiplexer redis, ICacheRedisMultiplexer cache, ILogger<TaxRatesStore> logger)
    {
        _redis = redis;
        _cache = cache;
        _logger = logger;
    }

    public async Task SetTaxRates(int worldId, TaxRates taxRates)
    {
        using var activity = Util.ActivitySource.StartActivity("TaxRatesStore.SetTaxRates");

        // Store data in the database
        var db = _redis.GetDatabase(RedisDatabases.Instance0.TaxRates);
        var dbTask = StoreTaxRates(db, taxRates, worldId);

        // Write through to the cache
        var cache = _cache.GetDatabase(RedisDatabases.Cache.TaxRates);
        var cacheTask = StoreTaxRates(cache, taxRates, worldId);

        await Task.WhenAll(dbTask, cacheTask);
    }

    public async Task<TaxRates> GetTaxRates(int worldId)
    {
        using var activity = Util.ActivitySource.StartActivity("TaxRatesStore.GetTaxRates");

        // Try to retrieve data from the cache
        var cache = _cache.GetDatabase(RedisDatabases.Cache.TaxRates);
        if (await HasTaxRates(cache, worldId))
        {
            var cachedObject = await FetchTaxRates(cache, worldId);
            if (cachedObject != null)
            {
                return cachedObject;
            }
        }

        // Fetch the tax rates from the database
        var db = _redis.GetDatabase(RedisDatabases.Instance0.TaxRates);
        var taxRates = await FetchTaxRates(db, worldId);

        // Store the result in the cache
        await StoreTaxRates(cache, taxRates, worldId);

        return taxRates;
    }

    private async Task StoreTaxRates(IDatabase db, TaxRates taxRates, int worldId)
    {
        using var activity = Util.ActivitySource.StartActivity("TaxRatesStore.StoreTaxRates");

        try
        {
            await db.HashSetAsync(worldId.ToString(), new[]
            {
                new HashEntry("Limsa Lominsa", taxRates.LimsaLominsa),
                new HashEntry("Gridania", taxRates.Gridania),
                new HashEntry("Ul'dah", taxRates.Uldah),
                new HashEntry("Ishgard", taxRates.Ishgard),
                new HashEntry("Kugane", taxRates.Kugane),
                new HashEntry("Crystarium", taxRates.Crystarium),
                new HashEntry("Old Sharlayan", taxRates.OldSharlayan),
                new HashEntry("Tuliyollal", taxRates.Tuliyollal),
                new HashEntry("source", taxRates.UploadApplicationName),
            }, CommandFlags.FireAndForget);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Failed to store TaxRates \"{TaxRatesCacheKey}\"", worldId);
        }
    }

    private async Task<bool> HasTaxRates(IDatabase db, int worldId)
    {
        using var activity = Util.ActivitySource.StartActivity("TaxRatesStore.HasTaxRates");

        var key = worldId.ToString();
        try
        {
            return await db.KeyExistsAsync(key, CommandFlags.PreferReplica);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Failed to query existence of TaxRates \"{TaxRatesCacheKey}\"", worldId);
            return false;
        }
    }

    private async Task<TaxRates> FetchTaxRates(IDatabase db, int worldId)
    {
        using var activity = Util.ActivitySource.StartActivity("TaxRatesStore.FetchTaxRates");

        var key = worldId.ToString();
        try
        {
            var tasks = new[]
                { "Limsa Lominsa", "Gridania", "Ul'dah", "Ishgard", "Kugane", "Crystarium", "Old Sharlayan",
                    "Tuliyollal", "source" }
            .Select(k => db.HashGetAsync(key, k, CommandFlags.PreferReplica));
            var values = await Task.WhenAll(tasks);
            return new TaxRates
            {
                LimsaLominsa = (int)values[0],
                Gridania = (int)values[1],
                Uldah = (int)values[2],
                Ishgard = (int)values[3],
                Kugane = (int)values[4],
                Crystarium = (int)values[5],
                OldSharlayan = (int)values[6],
                Tuliyollal = (int)values[7],
                UploadApplicationName = values[8],
            };
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Failed to retrieve TaxRates \"{TaxRatesCacheKey}\"", worldId);
            return null;
        }
    }
}