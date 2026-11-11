using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Easy
{
    public enum EasyTaskState
    {
        Pending,
        Completed,
        Canceled,
        Faulted
    }

    public enum Timing
    {
        Auto = 0,
        Main = 10,
        Thread = 100,
    }

    public enum ThreadMode
    {
        RunInThreadPool,
        RunInOneThread,
    }

    public interface IEasyTaskInterface
    {
        Timing GetTiming();
        void MoveNext();
        bool IsCompleted { get; }
        EasyTaskState EasyTaskState { get; }
        EasyCancellationToken EasyCancellationToken { get; }
        void SetException(Exception e);
        void Reuse(Timing timing, bool autoRecycle);
        void Unuse();
        void Retain();
        void Release();
        void Forget();
        bool IsFree();
    }

    internal interface IEasyTaskScheduleState
    {
        void MarkScheduled();
        void RemoveFromRunner();
    }

    internal interface IEasyTaskException
    {
        Exception Exception { get; }
    }

    public class EasyCancelException : OperationCanceledException
    {
        public override string Message { get { return "EasyTask Canceled"; } }
    }

    public class EasyTaskRunnerThreadException : Exception
    {
        public override string Message { get { return "EasyTaskRunner ThreadMode Not Start"; } }
    }

    public class EasyTaskPoolException : Exception
    {
        public override string Message { get { return "EasyTask Has Been Recycled"; } }
    }

    public sealed class EasyCancellationToken
    {
        private int isCanceled;

        public bool IsCanceled
        {
            get { return Volatile.Read(ref isCanceled) != 0; }
        }

        public void Cancel()
        {
            Interlocked.Exchange(ref isCanceled, 1);
        }
    }

    public static class EasyTaskRunner
    {
        public delegate void EasyTaskLog(string message);

        private sealed class ScheduledTask
        {
            internal IEasyTaskInterface Task;
            internal long Generation;
        }

        private sealed class TaskQueue
        {
            internal readonly ConcurrentQueue<ScheduledTask> Pending = new ConcurrentQueue<ScheduledTask>();
            internal int Ticking;
        }

        private static readonly object lifecycleLock = new object();
        private static readonly ConcurrentDictionary<Timing, TaskQueue> easyTasks =
            new ConcurrentDictionary<Timing, TaskQueue>();
        private static readonly ConcurrentQueue<ThreadAction> threadAction =
            new ConcurrentQueue<ThreadAction>();
        private static readonly int mainThreadId = Thread.CurrentThread.ManagedThreadId;

        [ThreadStatic]
        private static bool isThreadPump;
        private static long runnerGeneration;
        private static bool isStartThreadTiming;
        private static bool isResetting;
        private static int unlockedTaskOperations;
        private static CancellationTokenSource threadCancellation;
        private static Task threadPoolPump;

        private sealed class ThreadAction
        {
            internal Action Action;
            internal EasyCancellationToken CancellationToken;
            internal long Generation;
        }

        public static EasyTaskLog Log = delegate { };
        public static Thread thread;

        public static bool IsStartThreadTiming
        {
            get { return Volatile.Read(ref isStartThreadTiming); }
            private set { Volatile.Write(ref isStartThreadTiming, value); }
        }

        internal static bool RequiresTaskSynchronization
        {
            get { return IsStartThreadTiming || Volatile.Read(ref isResetting); }
        }

        internal static void EnterUnlockedTaskOperation()
        {
            Interlocked.Increment(ref unlockedTaskOperations);
        }

        internal static void ExitUnlockedTaskOperation()
        {
            Interlocked.Decrement(ref unlockedTaskOperations);
        }

        private static void WaitForUnlockedTaskOperations()
        {
            SpinWait spin = new SpinWait();
            while (Volatile.Read(ref unlockedTaskOperations) != 0)
            {
                spin.SpinOnce();
            }
        }

        internal static bool IsMainThread
        {
            get { return Thread.CurrentThread.ManagedThreadId == mainThreadId; }
        }

        internal static void SafeLog(string message)
        {
            try
            {
                EasyTaskLog log = Log;
                if (log != null) log(message);
            }
            catch
            {
                // Logging must never break task completion or the scheduler.
            }
        }

        public static void AddTask(IEasyTaskInterface task)
        {
            if (task == null) throw new ArgumentNullException("task");

            IEasyTaskScheduleState scheduleState = task as IEasyTaskScheduleState;
            if (scheduleState != null)
            {
                scheduleState.MarkScheduled();
            }

            TaskQueue queue = easyTasks.GetOrAdd(task.GetTiming(), delegate { return new TaskQueue(); });
            long generation = Volatile.Read(ref runnerGeneration);
            queue.Pending.Enqueue(new ScheduledTask { Task = task, Generation = generation });

            // If Reset raced with publication, leave the stale entry for Tick to
            // remove, but wake the awaiter immediately instead of losing the task.
            if (generation != Volatile.Read(ref runnerGeneration))
            {
                task.SetException(new EasyCancelException());
            }
        }

        public static void ResetTask()
        {
            bool calledFromRunner = isThreadPump || Thread.CurrentThread == thread;
            CancellationTokenSource cancellation;
            Task pump;
            Thread runnerThread;

            lock (lifecycleLock)
            {
                if (isResetting)
                {
                    return;
                }
                isResetting = true;
                IsStartThreadTiming = false;
                WaitForUnlockedTaskOperations();
                cancellation = threadCancellation;
                pump = threadPoolPump;
                runnerThread = thread;
                threadCancellation = null;
                threadPoolPump = null;
                thread = null;
                Interlocked.Increment(ref runnerGeneration);
            }

            try
            {
                if (cancellation != null)
                {
                    cancellation.Cancel();
                }

                // Never wait while holding lifecycleLock: runner callbacks are
                // allowed to call StartThreadTiming or ResetTask themselves.
                if (!calledFromRunner && runnerThread != null && runnerThread.IsAlive)
                {
                    runnerThread.Join();
                }
                if (!calledFromRunner && pump != null && !pump.IsCompleted)
                {
                    try
                    {
                        pump.GetAwaiter().GetResult();
                    }
                    catch (Exception error)
                    {
                        SafeLog(error.ToString());
                    }
                }

                ThreadAction ignored;
                while (threadAction.TryDequeue(out ignored))
                {
                }

                var canceledTasks = new List<IEasyTaskInterface>();
                foreach (TaskQueue queue in easyTasks.Values)
                {
                    ScheduledTask entry;
                    while (queue.Pending.TryDequeue(out entry))
                    {
                        canceledTasks.Add(entry.Task);
                    }
                }

                for (int i = 0; i < canceledTasks.Count; i++)
                {
                    CancelAndRemove(canceledTasks[i]);
                }
            }
            finally
            {
                if (cancellation != null)
                {
                    cancellation.Dispose();
                }
                lock (lifecycleLock)
                {
                    isResetting = false;
                }
            }
        }

        public static void StartThreadTiming(ThreadMode threadMode = ThreadMode.RunInThreadPool, int tickInterval = 15)
        {
            if (tickInterval <= 0)
            {
                throw new ArgumentOutOfRangeException("tickInterval", "The tick interval must be greater than zero.");
            }
            if (threadMode != ThreadMode.RunInThreadPool && threadMode != ThreadMode.RunInOneThread)
            {
                throw new ArgumentOutOfRangeException("threadMode");
            }

            lock (lifecycleLock)
            {
                if (isResetting)
                {
                    throw new InvalidOperationException("EasyTaskRunner is being reset. Retry after ResetTask returns.");
                }
                if (IsStartThreadTiming)
                {
                    return;
                }

                threadCancellation = new CancellationTokenSource();
                CancellationToken token = threadCancellation.Token;
                IsStartThreadTiming = true;
                WaitForUnlockedTaskOperations();

                if (threadMode == ThreadMode.RunInThreadPool)
                {
                    threadPoolPump = Task.Run(() => RunThreadPoolPump(token, tickInterval));
                }
                else
                {
                    thread = new Thread(() => RunSingleThreadPump(token, tickInterval));
                    thread.IsBackground = true;
                    thread.Name = "EasyTaskRunner";
                    thread.Start();
                }
            }
        }

        private static async Task RunThreadPoolPump(CancellationToken token, int tickInterval)
        {
            try
            {
                while (!token.IsCancellationRequested)
                {
                    await Task.Delay(tickInterval, token).ConfigureAwait(false);
                    RunThreadIteration();
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
            }
            catch (Exception error)
            {
                IsStartThreadTiming = false;
                SafeLog(error.ToString());
            }
        }

        private static void RunSingleThreadPump(CancellationToken token, int tickInterval)
        {
            try
            {
                while (!token.IsCancellationRequested)
                {
                    RunThreadIteration();
                    if (token.WaitHandle.WaitOne(tickInterval))
                    {
                        break;
                    }
                }
            }
            catch (ObjectDisposedException) when (token.IsCancellationRequested)
            {
            }
            catch (Exception error)
            {
                IsStartThreadTiming = false;
                SafeLog(error.ToString());
            }
        }

        private static void RunThreadIteration()
        {
            isThreadPump = true;
            try
            {
                DrainThreadActions();
                Tick(Timing.Thread);
            }
            finally
            {
                isThreadPump = false;
            }
        }

        private static void DrainThreadActions()
        {
            ThreadAction entry;
            while (threadAction.TryDequeue(out entry))
            {
                if (!IsStartThreadTiming || entry.Generation != Volatile.Read(ref runnerGeneration))
                {
                    continue;
                }
                if (entry.CancellationToken != null && entry.CancellationToken.IsCanceled)
                {
                    continue;
                }

                try
                {
                    entry.Action();
                }
                catch (Exception error)
                {
                    SafeLog(error.ToString());
                }
            }
        }

        public static EasyYieldTask Yield(EasyCancellationToken cancellationToken = null)
        {
            var task = EasyYieldTask.Create(true);
            task.SetCancellationToken(cancellationToken);
            task.Start();
            return task;
        }

        public static EasyVoidTask Delay(int milli, EasyCancellationToken cancellationToken = null)
        {
            var task = EasyDelayTask.Create(true);
            task.SetDelayTime(milli);
            task.SetCancellationToken(cancellationToken);
            task.Start();
            return task;
        }

        public static EasyVoidTask WaitUntil(Func<bool> condition, EasyCancellationToken cancellationToken = null)
        {
            var task = EasyWaitUntilTask.Create(true);
            task.SetCondition(condition);
            task.SetCancellationToken(cancellationToken);
            task.Start();
            return task;
        }

        public static EasyWailAllTask WaitAll(IEasyTaskInterface[] tasks, EasyCancellationToken cancellationToken = null)
        {
            var task = EasyWailAllTask.Create(true);
            task.SetWaitTasks(tasks);
            task.SetCancellationToken(cancellationToken);
            task.Start();
            return task;
        }

        public static EasyWailAllTask WaitAll(params IEasyTaskInterface[] tasks)
        {
            return WaitAll(tasks, null);
        }

        public static EasyWaitAnyTask WaitAny(IEasyTaskInterface[] tasks, EasyCancellationToken cancellationToken = null)
        {
            var task = EasyWaitAnyTask.Create(true);
            task.SetWaitTasks(tasks);
            task.SetCancellationToken(cancellationToken);
            task.Start();
            return task;
        }

        public static EasyWaitAnyTask WaitAny(params IEasyTaskInterface[] tasks)
        {
            return WaitAny(tasks, null);
        }

        public static EasyRunTask<T> Run<T>(Func<T> action, EasyCancellationToken cancellationToken = null)
        {
            var task = EasyRunTask<T>.Create(true);
            task.SetFunc(action);
            task.SetCancellationToken(cancellationToken);
            task.Start();
            return task;
        }

        public static EasyRunTask Run(Action action, EasyCancellationToken cancellationToken = null)
        {
            var task = EasyRunTask.Create(true);
            task.SetFunc(action);
            task.SetCancellationToken(cancellationToken);
            task.Start();
            return task;
        }

        public static void ExecInThread(Action action, EasyCancellationToken cancellationToken = null)
        {
            if (action == null) throw new ArgumentNullException("action");
            lock (lifecycleLock)
            {
                if (isResetting || !IsStartThreadTiming)
                {
                    throw new EasyTaskRunnerThreadException();
                }

                threadAction.Enqueue(new ThreadAction
                {
                    Action = action,
                    CancellationToken = cancellationToken,
                    Generation = Volatile.Read(ref runnerGeneration)
                });
            }
        }

        public static void Tick(Timing timing)
        {
            if (timing != Timing.Main && timing != Timing.Thread)
            {
                throw new ArgumentOutOfRangeException("timing");
            }

            TaskQueue queue;
            if (!easyTasks.TryGetValue(timing, out queue) ||
                Interlocked.CompareExchange(ref queue.Ticking, 1, 0) != 0)
            {
                return;
            }

            try
            {
                int count = queue.Pending.Count;
                for (int i = 0; i < count; i++)
                {
                    ScheduledTask entry;
                    if (!queue.Pending.TryDequeue(out entry))
                    {
                        break;
                    }

                    IEasyTaskInterface task = entry.Task;
                    // Reset can run concurrently with MoveNext. Hold a temporary
                    // reference so this entry cannot be recycled until processing ends.
                    task.Retain();
                    try
                    {
                        if (entry.Generation != Volatile.Read(ref runnerGeneration))
                        {
                            task.SetException(new EasyCancelException());
                        }
                        else if (!task.IsCompleted)
                        {
                            if (task.EasyCancellationToken != null && task.EasyCancellationToken.IsCanceled)
                            {
                                task.SetException(new EasyCancelException());
                            }
                            else
                            {
                                task.MoveNext();
                            }
                        }

                        if (entry.Generation != Volatile.Read(ref runnerGeneration) && !task.IsCompleted)
                        {
                            task.SetException(new EasyCancelException());
                        }

                        if (task.IsCompleted)
                        {
                            RemoveFromRunner(task);
                        }
                        else
                        {
                            queue.Pending.Enqueue(entry);
                        }
                    }
                    catch (Exception error)
                    {
                        SafeLog(error.ToString());
                        try
                        {
                            task.SetException(error);
                        }
                        finally
                        {
                            RemoveFromRunner(task);
                        }
                    }
                    finally
                    {
                        task.Release();
                    }
                }
            }
            finally
            {
                Interlocked.Exchange(ref queue.Ticking, 0);
            }
        }

        private static void CancelAndRemove(IEasyTaskInterface task)
        {
            try
            {
                if (!task.IsCompleted)
                {
                    task.SetException(new EasyCancelException());
                }
            }
            catch (Exception error)
            {
                SafeLog(error.ToString());
            }
            finally
            {
                RemoveFromRunner(task);
            }
        }

        private static void RemoveFromRunner(IEasyTaskInterface task)
        {
            IEasyTaskScheduleState scheduleState = task as IEasyTaskScheduleState;
            if (scheduleState != null)
            {
                scheduleState.RemoveFromRunner();
            }
            else
            {
                task.Unuse();
            }
        }
    }
}
