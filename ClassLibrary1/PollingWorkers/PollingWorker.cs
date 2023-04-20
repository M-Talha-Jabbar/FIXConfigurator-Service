using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using FIXMonitorBusinessLogicLayer.ResponseDataModels;


namespace FIXMonitorBusinessLogicLayer.PollingWorkers
{
    public class PollingWorker
    {
        public TimeSpan pollingStartTime;
        private Action _intervalEvent;
        private Action _onCompletionEvent;
        private int _pollingDurationSeconds;
        private int _intervalTimeSeconds;
        private Timer timer;

        public PollingWorker(Action intervalEvent, Action onCompletionEvent, int pollingDurationSeconds, int intervalTimeSeconds)
        {
            _intervalEvent = intervalEvent;
            _onCompletionEvent = onCompletionEvent;
            _pollingDurationSeconds = pollingDurationSeconds;
            _intervalTimeSeconds = intervalTimeSeconds;
            timer = new Timer();
        }

        public virtual void Poll() 
        {
            pollingStartTime = DateTime.Now.TimeOfDay;
            timer.Interval = _intervalTimeSeconds * 1000;
            timer.Elapsed += IntervalEvent;
            timer.Start();
        }

        protected void IntervalEvent(object source, ElapsedEventArgs e)
        {
            // Check if 2 minutes have elapsed
           
            if (e.SignalTime.TimeOfDay - pollingStartTime > TimeSpan.FromSeconds(_pollingDurationSeconds))
            {
                timer.Stop();
                _onCompletionEvent();               
            }
            else
            {
                _intervalEvent();
            }
        }

        public void Stop() 
        {
            timer.Stop();
        }

        public bool isPolling() 
        {
            return timer.Enabled;
        }

    }
}
