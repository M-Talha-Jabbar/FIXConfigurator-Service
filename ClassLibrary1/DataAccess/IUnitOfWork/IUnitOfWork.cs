using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.DataAccess.IUnitOfWork
{
    public interface IUnitOfWork<TContext> : IDisposable where TContext : DbContext
    {
        TContext Context
        {
            get;
        }
        Task<bool> CreateTransactionAsync();
        Task<bool> CommitAsync();
        Task RollbackAsync();
        Task<bool> SaveAsync();
    }
}
