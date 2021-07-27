﻿using System.Threading.Tasks;
using MongoDB.Driver;
using Universalis.DbAccess.Queries;
using Universalis.Entities.Uploaders;

namespace Universalis.DbAccess
{
    public class WorldUploadCountDbAccess : DbAccessService<WorldUploadCount, WorldUploadCountQuery>, IWorldUploadCountDbAccess
    {
        public WorldUploadCountDbAccess() : base("universalis", "extraData") { }

        public async Task Increment(WorldUploadCountQuery query)
        {
            if (await Retrieve(query) == null)
            {
                await Create(new WorldUploadCount
                {
                    Count = 1,
                    SetName = WorldUploadCountQuery.SetName,
                    WorldName = query.WorldName,
                });
                return;
            }

            var updateBuilder = Builders<WorldUploadCount>.Update;
            var update = updateBuilder.Inc(o => o.Count, 1U);
            await Collection.UpdateOneAsync(query.ToFilterDefinition(), update);
        }
    }
}