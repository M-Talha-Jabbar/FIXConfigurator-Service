using FIXMonitorBusinessLogicLayer.DataAccess.IUnitOfWork;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CoreLogging;
using FIXMonitorBusinessLogicLayer.Utilities;

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
            catch (Exception e)
            {
                Logging.LogMessage(LOGTYPE.Error, "Cannot CreateTransactionAsync transaction");
                ExceptionLoggingUtility.LogException(e);
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
            catch (Exception e)
            {
                Logging.LogMessage(LOGTYPE.Error, "Cannot CommitAsync transaction");
                ExceptionLoggingUtility.LogException(e);
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
            catch (Exception e)
            {
                Logging.LogMessage(LOGTYPE.Error, "Cannot RollbackAsync transaction");
                ExceptionLoggingUtility.LogException(e);
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
                var res = await Context.SaveChangesAsync();
                return res > 0 ? true : false;
            }
            catch (Exception e)
            {
                Logging.LogMessage(LOGTYPE.Error, "Cannot SaveAsync transaction");
                ExceptionLoggingUtility.LogException(e);
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
