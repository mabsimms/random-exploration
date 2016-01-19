using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace BatchingDispatcher
{
    public class BatchingDispatcher<T> : IDisposable
    {
        private readonly BufferBlock<T> bufferBlock;
        private readonly ActionBlock<IEnumerable<T>> actionBlock;
        private readonly BatchBlock<T> batchBlock;

        public int MaxBufferedEvents { get; private set; }

        public int BatchSize { get; private set; }

        public int BatchTimeoutMilliseconds { get; private set; }

        public int MaxConcurrentWrites { get; private set; }

        private Timer flushTimer;

        private IDisposable bufferLink;
        private IDisposable actionLink;

        private readonly Func<IEnumerable<T>, Task> actionFunc;

        public BatchingDispatcher(
            Func<IEnumerable<T>, Task> action,             
            int maxBufferedEvents = 1000,
            int batchSize = 100,
            int maxBatchFlushTimeMs = 6000,
            int concurrentPublish = 1)
        {
            this.actionFunc = action;
            this.MaxBufferedEvents = maxBufferedEvents;
            this.BatchSize = batchSize;
            this.BatchTimeoutMilliseconds = maxBatchFlushTimeMs;
            this.MaxConcurrentWrites = concurrentPublish;

            bufferBlock = new BufferBlock<T>(
              new DataflowBlockOptions()
              {
                  BoundedCapacity = maxBufferedEvents
              });

            batchBlock = new BatchBlock<T>(
                batchSize: BatchSize, dataflowBlockOptions: new GroupingDataflowBlockOptions()
                {
                    Greedy = true, BoundedCapacity = BatchSize * 2
                });

            actionBlock = new ActionBlock<IEnumerable<T>>(
                async (msgs) => await actionFunc(msgs),
                new ExecutionDataflowBlockOptions()
                {
                    MaxDegreeOfParallelism = MaxConcurrentWrites
                });

            var bufferLink = bufferBlock.LinkTo(batchBlock);
            var actionLink = batchBlock.LinkTo(actionBlock);

            flushTimer = new Timer(FlushBuffer, null, 
                TimeSpan.FromMilliseconds(BatchTimeoutMilliseconds), 
                TimeSpan.FromMilliseconds(BatchTimeoutMilliseconds));


        }
        
        public void Post(T obj)
        {
            if (!bufferBlock.Post(obj))
            {
                // [TODO]; log this

                // Buffer is backed up - throw away stored events 
                // Break the link to the standard action block
                bufferLink.Dispose();

                // Pull and throw away the stored items
                IList<T> items;
                if (bufferBlock.TryReceiveAll(out items))
                {
                    // Restore the link
                    bufferLink = bufferBlock.LinkTo(batchBlock);
                }                
            }    
        }

        private void FlushBuffer(object state)
        {
            if (batchBlock != null)
                batchBlock.TriggerBatch();
        }

        public void Dispose()
        {
            if (flushTimer != null)
            {
                flushTimer.Dispose();
                flushTimer = null;
            }


            if (actionLink != null)
            {
                actionLink.Dispose();
                actionLink = null;
            }

            if (bufferLink != null)
            {
                bufferLink.Dispose();
                bufferLink = null;
            }

        }
    }
}
