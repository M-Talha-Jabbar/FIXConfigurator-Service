using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RedisCacheService
{
    interface IAppSettings
    {
        IRedisSettings redisSettings { get; set; }
    }
}
