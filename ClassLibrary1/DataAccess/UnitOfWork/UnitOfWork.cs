using FIXMonitorBusinessLogicLayer.DataAccess.IUnitOfWork;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CoreLogging;

namespace FIXMonitorBusinessLogicLayer.DataAccess.UnitOfWork
{
    public class UnitOfWork<TContext> : IUnitOfWork<TContext> where TContext : DbContext, new()
    {
        public TContext Context { get; private set; }
        private IDbContextTransaction Transaction;

        public UnitOfWork() 
        {
            Context = new TContext();
        }

        public async Task<bool> CreateTransactionAsync()
        {
            Logging.LogMessage(LOGTYPE.Info, $"Method CreateTransactionAsync started...");
            try
            {
                Transaction = await Context.Database.BeginTransactionAsync();
                return true;
            }
            catch (Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Error, $"Cannot CreateTransactionAsync transaction {ex.Message}");
                return false;
            }
            finally {
                Logging.LogMessage(LOGTYPE.Info, $"Method CreateTransactionAsync completed...");
            }
        }

        public async Task<bool> CommitAsync()
        {

            Logging.LogMessage(LOGTYPE.Info, $"Method CommitAsync in UnitOfWork started...");
            try
            {
                await Transaction.CommitAsync();
                return true;
            }
            catch (Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Error, $"Cannot CommitAsync transaction {ex.Message}");
                return false;
            }
            finally
            {
                Logging.LogMessage(LOGTYPE.Info, $"Method CommitAsync in UnitOfWork completed...");
            }  
        }

        public async Task RollbackAsync()
        {
            Logging.LogMessage(LOGTYPE.Info, $"Method RollbackAsync in UnitOfWork started...");
            try
            {
                await Transaction.RollbackAsync();
            }
            catch (Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Error, $"Cannot RollbackAsync transaction {ex.Message}");
            }
            finally
            {
                Logging.LogMessage(LOGTYPE.Info, $"Method RollbackAsync in UnitOfWork completed...");
            }
        }

        public async Task<bool> SaveAsync()
        {
            Logging.LogMessage(LOGTYPE.Info, $"Method SaveAsync in UnitOfWork started...");
            try
            {
                await Context.SaveChangesAsync();
                return true;
            }
            catch (Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Error, $"Cannot SaveAsync transaction {ex.Message}");
                return false;
            }
            finally
            {
                Logging.LogMessage(LOGTYPE.Info, $"Method SaveAsync in UnitOfWork completed...");
            }
        }

        public void Dispose() 
        { 
            Context.Dispose();
        }
    }
}
