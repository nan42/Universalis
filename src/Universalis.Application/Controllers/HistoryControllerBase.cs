﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Universalis.Application.Common;
using Universalis.Application.Views;
using Universalis.DataTransformations;
using Universalis.DbAccess.MarketBoard;
using Universalis.DbAccess.Queries.MarketBoard;
using Universalis.Entities.MarketBoard;
using Universalis.GameData;

namespace Universalis.Application.Controllers
{
    public class HistoryControllerBase : WorldDcControllerBase
    {
        protected readonly IHistoryDbAccess History;

        public HistoryControllerBase(IGameDataProvider gameData, IHistoryDbAccess historyDb) : base(gameData)
        {
            History = historyDb;
        }

        protected async Task<(bool, HistoryView)> GetHistoryView(WorldDc worldDc, uint[] worldIds, uint itemId, int entries)
        {
            var data = (await History.RetrieveMany(new HistoryManyQuery
            {
                WorldIds = worldIds,
                ItemId = itemId,
            })).ToList();

            var resolved = data.Count > 0;

            var worlds = GameData.AvailableWorlds();

            var history = data.Aggregate(new HistoryView(), (agg, next) =>
            {
                // Handle undefined arrays
                next.Sales ??= new List<MinimizedSale>();

                agg.Sales = next.Sales
                    .Select(s => new MinimizedSaleView
                    {
                        Hq = s.Hq,
                        PricePerUnit = s.PricePerUnit,
                        Quantity = s.Quantity,
                        TimestampUnixSeconds = s.SaleTimeUnixSeconds,
                        WorldId = worldDc.IsDc ? next.WorldId : null,
                        WorldName = worldDc.IsDc ? worlds[next.WorldId] : null,
                    })
                    .Concat(agg.Sales)
                    .ToList();
                agg.LastUploadTimeUnixMilliseconds = Math.Max(next.LastUploadTimeUnixMilliseconds, agg.LastUploadTimeUnixMilliseconds);

                return agg;
            });

            history.Sales.Sort((a, b) => (int)b.TimestampUnixSeconds - (int)a.TimestampUnixSeconds);
            history.Sales = history.Sales.Take(entries).ToList();

            var nqSales = history.Sales.Where(s => !s.Hq).ToList();
            var hqSales = history.Sales.Where(s => s.Hq).ToList();
            return (resolved, new HistoryView
            {
                Sales = history.Sales,
                ItemId = itemId,
                WorldId = worldDc.IsWorld ? worldDc.WorldId : null,
                WorldName = worldDc.IsWorld ? worldDc.WorldName : null,
                DcName = worldDc.IsDc ? worldDc.DcName : null,
                LastUploadTimeUnixMilliseconds = history.LastUploadTimeUnixMilliseconds,
                StackSizeHistogram = new SortedDictionary<int, int>(Statistics.GetDistribution(history.Sales
                    .Select(s => s.Quantity)
                    .Select(q => (int)q))),
                StackSizeHistogramNq = new SortedDictionary<int, int>(Statistics.GetDistribution(nqSales
                    .Select(s => s.Quantity)
                    .Select(q => (int)q))),
                StackSizeHistogramHq = new SortedDictionary<int, int>(Statistics.GetDistribution(hqSales
                    .Select(s => s.Quantity)
                    .Select(q => (int)q))),
                SaleVelocity = Statistics.WeekVelocityPerDay(history.Sales
                    .Select(s => (long)s.TimestampUnixSeconds * 1000)),
                SaleVelocityNq = Statistics.WeekVelocityPerDay(nqSales
                    .Select(s => (long)s.TimestampUnixSeconds * 1000)),
                SaleVelocityHq = Statistics.WeekVelocityPerDay(hqSales
                    .Select(s => (long)s.TimestampUnixSeconds * 1000)),
            });
        }
    }
}