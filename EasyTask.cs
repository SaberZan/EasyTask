using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;

namespace Easy
{
    [AsyncMethodBuilder(typeof(EasyAsyncGenericTaskMethodBuider<>))]
    public class EasyTask<T> : IEasyTaskInterface, IEasyTaskException, ICriticalNotifyCompletion, IEasyTaskScheduleState
    {
        protected static readonly ConcurrentQueue<EasyTask<T>> queue = new ConcurrentQueue<EasyTask<T>>();

        private readonly object syncRoot = new object();
        private int easyTaskState;
        private ExceptionDispatchInfo exception;
        private Action callback;
        private bool hasBeenUsed;
        private bool isScheduled;

        protected T result;
        protected bool autoRecycle;
        protected Timing timing;
        protected int reference;
        protected bool isInPool;
        protected bool getResultCalled;

        protected EasyTask()
        {
            easyTaskState = (int)EasyTaskState.Pending;
            timing = Timing.Main;
        }

        public static Timing CheckTiming(Timing timing)
        {
            if (timing == Timing.Auto)
            {
                return EasyTaskRunner.IsStartThreadTiming && !EasyTaskRunner.IsMainThread
                    ? Timing.Thread
                    : Timing.Main;
            }

            if (timing == Timing.Thread && !EasyTaskRunner.IsStartThreadTiming)
            {
                throw new EasyTaskRunnerThreadException();
            }

            if (timing != Timing.Main && timing != Timing.Thread)
            {
                throw new ArgumentOutOfRangeException("timing");
            }

            return timing;
        }

        public static EasyTask<T> Create(bool autoRecycle = false, Timing timing = Timing.Auto)
        {
            Timing checkedTiming = CheckTiming(timing);
            EasyTask<T> task;
            if (!queue.TryDequeue(out task))
            {
                task = new EasyTask<T>();
            }

            task.Reuse(checkedTiming, autoRecycle);
            return task;
        }

        public EasyTaskState EasyTaskState
        {
            get { return (EasyTaskState)Volatile.Read(ref easyTaskState); }
        }

        public EasyCancellationToken EasyCancellationToken
        {
            get { lock (syncRoot) { return cancellationToken; } }
        }

        private EasyCancellationToken cancellationToken;

        public Exception Exception
        {
            get { lock (syncRoot) { return exception == null ? null : exception.SourceException; } }
        }

        public bool IsCompleted
        {
            get { return EasyTaskState != EasyTaskState.Pending; }
        }

        public virtual void Reuse(Timing timing, bool autoRecycle)
        {
            Timing checkedTiming = CheckTiming(timing);
            lock (syncRoot)
            {
                if (isScheduled || (hasBeenUsed && !isInPool))
                {
                    throw new InvalidOperationException("Only a new or pooled task can be reused.");
                }

                hasBeenUsed = true;
                isInPool = false;
                this.timing = checkedTiming;
                this.autoRecycle = autoRecycle;
                reference = 1;
                getResultCalled = false;
                result = default(T);
                exception = null;
                callback = null;
                cancellationToken = null;
                Volatile.Write(ref easyTaskState, (int)EasyTaskState.Pending);
            }
        }

        // A task must be fully configured before it is published to the runner.
        public void Start()
        {
            EasyTaskRunner.AddTask(this);
        }

        void IEasyTaskScheduleState.MarkScheduled()
        {
            lock (syncRoot)
            {
                if (isScheduled)
                {
                    throw new InvalidOperationException("The task has already been scheduled.");
                }
                if (isInPool || IsCompleted)
                {
                    throw new InvalidOperationException("A pooled or completed task cannot be scheduled.");
                }

                isScheduled = true;
            }
        }

        void IEasyTaskScheduleState.RemoveFromRunner()
        {
            lock (syncRoot)
            {
                isScheduled = false;
            }
            Unuse();
        }

        public virtual void Unuse()
        {
            lock (syncRoot)
            {
                if (isScheduled || isInPool || !autoRecycle || reference != 0 || !IsCompleted)
                {
                    return;
                }

                OnRecycle();
                callback = null;
                exception = null;
                result = default(T);
                cancellationToken = null;
                getResultCalled = false;
                autoRecycle = false;
                Volatile.Write(ref easyTaskState, (int)EasyTaskState.Pending);
                isInPool = true;
            }

            ReturnToPool();
        }

        protected virtual void ReturnToPool()
        {
            queue.Enqueue(this);
        }

        protected virtual void OnRecycle()
        {
        }

        // Called once after the terminal state is published and before awaiters resume.
        protected virtual void OnTerminate()
        {
        }

        protected object SyncRoot
        {
            get { return syncRoot; }
        }

        // Must be called while holding SyncRoot.
        protected void EnsureConfigurable()
        {
            if (isScheduled || IsCompleted || isInPool)
            {
                throw new InvalidOperationException("Configure the task before starting it.");
            }
        }

        public void Retain()
        {
            lock (syncRoot)
            {
                if (isInPool)
                {
                    throw new EasyTaskPoolException();
                }
                reference++;
            }
        }

        public void Release()
        {
            lock (syncRoot)
            {
                if (reference <= 0)
                {
                    throw new InvalidOperationException("The task reference count cannot be released below zero.");
                }
                reference--;
            }

            // The runner may already have removed this completed task. In that
            // case the final releaser is responsible for returning it to the pool.
            Unuse();
        }

        public bool IsFree()
        {
            lock (syncRoot) { return reference <= 0; }
        }

        public EasyTask<T> GetAwaiter()
        {
            return this;
        }

        public virtual T GetResult()
        {
            T value;
            ExceptionDispatchInfo error;

            lock (syncRoot)
            {
                if (isInPool)
                {
                    throw new EasyTaskPoolException();
                }
                if (!IsCompleted)
                {
                    throw new InvalidOperationException("The task has not completed.");
                }
                if (getResultCalled)
                {
                    throw new InvalidOperationException("An EasyTask can only be awaited once.");
                }

                getResultCalled = true;
                value = result;
                error = exception;
                if (reference <= 0)
                {
                    throw new InvalidOperationException("The task has no consumer reference to release.");
                }
                reference--;
            }

            Unuse();
            if (error != null)
            {
                error.Throw();
            }
            return value;
        }

        public void SetResult(T value)
        {
            Action continuation = null;
            lock (syncRoot)
            {
                if (IsCompleted || isInPool)
                {
                    return;
                }

                result = value;
                continuation = callback;
                callback = null;
                Volatile.Write(ref easyTaskState, (int)EasyTaskState.Completed);
            }

            NotifyTerminated();
            InvokeContinuation(continuation);
        }

        public void SetException(Exception error)
        {
            if (error == null)
            {
                throw new ArgumentNullException("error");
            }

            Action continuation = null;
            EasyTaskState completedState;
            lock (syncRoot)
            {
                if (IsCompleted || isInPool)
                {
                    return;
                }

                exception = ExceptionDispatchInfo.Capture(error);
                completedState = error is OperationCanceledException
                    ? EasyTaskState.Canceled
                    : EasyTaskState.Faulted;
                continuation = callback;
                callback = null;
                Volatile.Write(ref easyTaskState, (int)completedState);
            }

            if (completedState == EasyTaskState.Faulted)
            {
                EasyTaskRunner.SafeLog(error.ToString());
            }
            NotifyTerminated();
            InvokeContinuation(continuation);
        }

        private void NotifyTerminated()
        {
            try
            {
                OnTerminate();
            }
            catch (Exception error)
            {
                EasyTaskRunner.SafeLog(error.ToString());
            }
        }

        private static void InvokeContinuation(Action continuation)
        {
            if (continuation == null)
            {
                return;
            }

            try
            {
                continuation();
            }
            catch (Exception error)
            {
                // A continuation must not terminate the scheduler or a ThreadPool worker.
                EasyTaskRunner.SafeLog(error.ToString());
            }
        }

        public EasyTask<T> SetCancellationToken(EasyCancellationToken token)
        {
            lock (syncRoot)
            {
                EnsureConfigurable();
                cancellationToken = token;
            }
            return this;
        }

        public void OnCompleted(Action continuation)
        {
            UnsafeOnCompleted(continuation);
        }

        public void UnsafeOnCompleted(Action continuation)
        {
            if (continuation == null)
            {
                throw new ArgumentNullException("continuation");
            }

            lock (syncRoot)
            {
                if (isInPool)
                {
                    throw new InvalidOperationException("An EasyTask can only be awaited once; the pooled task has already been consumed.");
                }
                if (!IsCompleted)
                {
                    if (callback != null)
                    {
                        throw new InvalidOperationException("An EasyTask supports only one awaiter.");
                    }
                    callback = continuation;
                    return;
                }
            }

            InvokeContinuation(continuation);
        }

        public Timing GetTiming()
        {
            lock (syncRoot) { return timing; }
        }

        public virtual void MoveNext()
        {
        }
    }

    [AsyncMethodBuilder(typeof(EasyAsyncGenericTaskMethodBuider))]
    public class EasyVoidTask : EasyTask<object>
    {
        protected new static readonly ConcurrentQueue<EasyVoidTask> queue = new ConcurrentQueue<EasyVoidTask>();

        protected EasyVoidTask()
        {
        }

        public new static EasyVoidTask Create(bool autoRecycle = false, Timing timing = Timing.Auto)
        {
            Timing checkedTiming = CheckTiming(timing);
            EasyVoidTask task;
            if (!queue.TryDequeue(out task))
            {
                task = new EasyVoidTask();
            }
            task.Reuse(checkedTiming, autoRecycle);
            return task;
        }

        protected override void ReturnToPool()
        {
            queue.Enqueue(this);
        }
    }

    public class EasyYieldTask : EasyVoidTask
    {
        protected new static readonly ConcurrentQueue<EasyYieldTask> queue = new ConcurrentQueue<EasyYieldTask>();

        protected EasyYieldTask()
        {
        }

        public new static EasyYieldTask Create(bool autoRecycle = false, Timing timing = Timing.Auto)
        {
            Timing checkedTiming = CheckTiming(timing);
            EasyYieldTask task;
            if (!queue.TryDequeue(out task))
            {
                task = new EasyYieldTask();
            }
            task.Reuse(checkedTiming, autoRecycle);
            return task;
        }

        protected override void ReturnToPool()
        {
            queue.Enqueue(this);
        }

        public override void MoveNext()
        {
            SetResult(null);
        }
    }

    public class EasyExecFunTask : EasyVoidTask
    {
        protected new static readonly ConcurrentQueue<EasyExecFunTask> queue = new ConcurrentQueue<EasyExecFunTask>();
        private Action func;

        protected EasyExecFunTask()
        {
        }

        public new static EasyExecFunTask Create(bool autoRecycle = false, Timing timing = Timing.Auto)
        {
            Timing checkedTiming = CheckTiming(timing);
            EasyExecFunTask task;
            if (!queue.TryDequeue(out task))
            {
                task = new EasyExecFunTask();
            }
            task.Reuse(checkedTiming, autoRecycle);
            return task;
        }

        public EasyExecFunTask SetFunc(Action func)
        {
            if (func == null) throw new ArgumentNullException("func");
            lock (SyncRoot)
            {
                EnsureConfigurable();
                this.func = func;
            }
            return this;
        }

        protected override void OnRecycle()
        {
            func = null;
        }

        protected override void ReturnToPool()
        {
            queue.Enqueue(this);
        }

        public override void MoveNext()
        {
            try
            {
                func();
                SetResult(null);
            }
            catch (Exception error)
            {
                SetException(error);
            }
        }
    }

    public class EasyRunTask<T> : EasyTask<T>
    {
        protected new static readonly ConcurrentQueue<EasyRunTask<T>> queue = new ConcurrentQueue<EasyRunTask<T>>();
        private Func<T> func;
        private int isRunning;

        protected EasyRunTask()
        {
        }

        public new static EasyRunTask<T> Create(bool autoRecycle = false, Timing timing = Timing.Auto)
        {
            Timing checkedTiming = CheckTiming(timing);
            EasyRunTask<T> task;
            if (!queue.TryDequeue(out task))
            {
                task = new EasyRunTask<T>();
            }
            task.Reuse(checkedTiming, autoRecycle);
            return task;
        }

        public EasyRunTask<T> SetFunc(Func<T> func)
        {
            if (func == null) throw new ArgumentNullException("func");
            lock (SyncRoot)
            {
                EnsureConfigurable();
                this.func = func;
            }
            return this;
        }

        protected override void OnRecycle()
        {
            func = null;
            isRunning = 0;
        }

        protected override void ReturnToPool()
        {
            queue.Enqueue(this);
        }

        public override void MoveNext()
        {
            if (IsCompleted || Interlocked.CompareExchange(ref isRunning, 1, 0) != 0)
            {
                return;
            }

            // Cancellation may complete the awaitable before the worker exits.
            // Keep a worker reference so the object cannot be reused in between.
            Retain();
            if (!ThreadPool.QueueUserWorkItem(Run))
            {
                Interlocked.Exchange(ref isRunning, 0);
                SetException(new InvalidOperationException("The work item could not be queued."));
                Release();
            }
        }

        private void Run(object state)
        {
            try
            {
                Func<T> currentFunc = func;
                SetResult(currentFunc());
            }
            catch (Exception error)
            {
                SetException(error);
            }
            finally
            {
                Release();
            }
        }
    }

    public class EasyRunTask : EasyVoidTask
    {
        protected new static readonly ConcurrentQueue<EasyRunTask> queue = new ConcurrentQueue<EasyRunTask>();
        private Action func;
        private int isRunning;

        protected EasyRunTask()
        {
        }

        public new static EasyRunTask Create(bool autoRecycle = false, Timing timing = Timing.Auto)
        {
            Timing checkedTiming = CheckTiming(timing);
            EasyRunTask task;
            if (!queue.TryDequeue(out task))
            {
                task = new EasyRunTask();
            }
            task.Reuse(checkedTiming, autoRecycle);
            return task;
        }

        public EasyRunTask SetFunc(Action func)
        {
            if (func == null) throw new ArgumentNullException("func");
            lock (SyncRoot)
            {
                EnsureConfigurable();
                this.func = func;
            }
            return this;
        }

        protected override void OnRecycle()
        {
            func = null;
            isRunning = 0;
        }

        protected override void ReturnToPool()
        {
            queue.Enqueue(this);
        }

        public override void MoveNext()
        {
            if (IsCompleted || Interlocked.CompareExchange(ref isRunning, 1, 0) != 0)
            {
                return;
            }

            Retain();
            if (!ThreadPool.QueueUserWorkItem(Run))
            {
                Interlocked.Exchange(ref isRunning, 0);
                SetException(new InvalidOperationException("The work item could not be queued."));
                Release();
            }
        }

        private void Run(object state)
        {
            try
            {
                Action currentFunc = func;
                currentFunc();
                SetResult(null);
            }
            catch (Exception error)
            {
                SetException(error);
            }
            finally
            {
                Release();
            }
        }
    }

    public class EasyDelayTask : EasyVoidTask
    {
        protected new static readonly ConcurrentQueue<EasyDelayTask> queue = new ConcurrentQueue<EasyDelayTask>();
        private long overTimestamp;

        protected EasyDelayTask()
        {
        }

        public new static EasyDelayTask Create(bool autoRecycle = false, Timing timing = Timing.Auto)
        {
            Timing checkedTiming = CheckTiming(timing);
            EasyDelayTask task;
            if (!queue.TryDequeue(out task))
            {
                task = new EasyDelayTask();
            }
            task.Reuse(checkedTiming, autoRecycle);
            return task;
        }

        public EasyDelayTask SetDelayTime(int milli)
        {
            if (milli < 0) throw new ArgumentOutOfRangeException("milli");
            lock (SyncRoot)
            {
                EnsureConfigurable();
                overTimestamp = System.Diagnostics.Stopwatch.GetTimestamp()
                    + (long)milli * System.Diagnostics.Stopwatch.Frequency / 1000L;
            }
            return this;
        }

        protected override void OnRecycle()
        {
            overTimestamp = 0;
        }

        protected override void ReturnToPool()
        {
            queue.Enqueue(this);
        }

        public override void MoveNext()
        {
            if (!IsCompleted && System.Diagnostics.Stopwatch.GetTimestamp() >= overTimestamp)
            {
                SetResult(null);
            }
        }
    }

    public class EasyWaitUntilTask : EasyVoidTask
    {
        protected new static readonly ConcurrentQueue<EasyWaitUntilTask> queue = new ConcurrentQueue<EasyWaitUntilTask>();
        private Func<bool> condition;

        protected EasyWaitUntilTask()
        {
        }

        public new static EasyWaitUntilTask Create(bool autoRecycle = false, Timing timing = Timing.Auto)
        {
            Timing checkedTiming = CheckTiming(timing);
            EasyWaitUntilTask task;
            if (!queue.TryDequeue(out task))
            {
                task = new EasyWaitUntilTask();
            }
            task.Reuse(checkedTiming, autoRecycle);
            return task;
        }

        public EasyWaitUntilTask SetCondition(Func<bool> condition)
        {
            if (condition == null) throw new ArgumentNullException("condition");
            lock (SyncRoot)
            {
                EnsureConfigurable();
                this.condition = condition;
            }
            return this;
        }

        protected override void OnRecycle()
        {
            condition = null;
        }

        protected override void ReturnToPool()
        {
            queue.Enqueue(this);
        }

        public override void MoveNext()
        {
            if (IsCompleted)
            {
                return;
            }

            try
            {
                if (condition())
                {
                    SetResult(null);
                }
            }
            catch (Exception error)
            {
                SetException(error);
            }
        }
    }

    public class EasyWailAllTask : EasyVoidTask
    {
        protected new static readonly ConcurrentQueue<EasyWailAllTask> queue = new ConcurrentQueue<EasyWailAllTask>();
        private IEasyTaskInterface[] easyTasks;
        private bool waitTasksReleased;

        protected EasyWailAllTask()
        {
        }

        public new static EasyWailAllTask Create(bool autoRecycle = false, Timing timing = Timing.Auto)
        {
            Timing checkedTiming = CheckTiming(timing);
            EasyWailAllTask task;
            if (!queue.TryDequeue(out task))
            {
                task = new EasyWailAllTask();
            }
            task.Reuse(checkedTiming, autoRecycle);
            return task;
        }

        public EasyWailAllTask SetWaitTasks(params IEasyTaskInterface[] tasks)
        {
            IEasyTaskInterface[] copy = ValidateAndCopyTasks(tasks, false);
            lock (SyncRoot)
            {
                EnsureConfigurable();
                if (easyTasks != null)
                {
                    throw new InvalidOperationException("Wait tasks can only be assigned once.");
                }
                easyTasks = copy;
                RetainTasks(easyTasks);
                waitTasksReleased = false;
            }
            return this;
        }

        protected override void OnTerminate()
        {
            ReleaseWaitTasks();
        }

        protected override void OnRecycle()
        {
            ReleaseWaitTasks();
            easyTasks = null;
        }

        private void ReleaseWaitTasks()
        {
            IEasyTaskInterface[] tasks;
            lock (SyncRoot)
            {
                if (waitTasksReleased)
                {
                    return;
                }
                waitTasksReleased = true;
                tasks = easyTasks;
            }

            if (tasks == null)
            {
                return;
            }
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i].Release();
            }
        }

        protected override void ReturnToPool()
        {
            queue.Enqueue(this);
        }

        public override void MoveNext()
        {
            if (IsCompleted)
            {
                return;
            }

            int terminalCount = 0;
            int canceledCount = 0;
            List<Exception> errors = null;

            for (int i = 0; i < easyTasks.Length; i++)
            {
                switch (easyTasks[i].EasyTaskState)
                {
                    case EasyTaskState.Completed:
                        terminalCount++;
                        break;
                    case EasyTaskState.Canceled:
                        terminalCount++;
                        canceledCount++;
                        break;
                    case EasyTaskState.Faulted:
                        terminalCount++;
                        if (errors == null) errors = new List<Exception>();
                        IEasyTaskException exceptionSource = easyTasks[i] as IEasyTaskException;
                        errors.Add(exceptionSource == null || exceptionSource.Exception == null
                            ? new Exception("A task faulted without an exception.")
                            : exceptionSource.Exception);
                        break;
                }
            }

            if (terminalCount != easyTasks.Length)
            {
                return;
            }
            if (errors != null)
            {
                SetException(errors.Count == 1 ? errors[0] : new AggregateException(errors));
            }
            else if (canceledCount != 0)
            {
                SetException(new EasyCancelException());
            }
            else
            {
                SetResult(null);
            }
        }

        internal static IEasyTaskInterface[] ValidateAndCopyTasks(IEasyTaskInterface[] tasks, bool requireNonEmpty)
        {
            if (tasks == null) throw new ArgumentNullException("tasks");
            if (requireNonEmpty && tasks.Length == 0)
            {
                throw new ArgumentException("WaitAny requires at least one task.", "tasks");
            }

            var copy = new IEasyTaskInterface[tasks.Length];
            for (int i = 0; i < tasks.Length; i++)
            {
                if (tasks[i] == null)
                {
                    throw new ArgumentException("The task array cannot contain null.", "tasks");
                }
                copy[i] = tasks[i];
            }
            return copy;
        }

        internal static void RetainTasks(IEasyTaskInterface[] tasks)
        {
            int retained = 0;
            try
            {
                for (; retained < tasks.Length; retained++)
                {
                    tasks[retained].Retain();
                }
            }
            catch
            {
                for (int i = 0; i < retained; i++)
                {
                    tasks[i].Release();
                }
                throw;
            }
        }
    }

    public class EasyWaitAnyTask : EasyTask<int>
    {
        protected new static readonly ConcurrentQueue<EasyWaitAnyTask> queue = new ConcurrentQueue<EasyWaitAnyTask>();
        private IEasyTaskInterface[] easyTasks;
        private bool waitTasksReleased;

        protected EasyWaitAnyTask()
        {
        }

        public new static EasyWaitAnyTask Create(bool autoRecycle = false, Timing timing = Timing.Auto)
        {
            Timing checkedTiming = CheckTiming(timing);
            EasyWaitAnyTask task;
            if (!queue.TryDequeue(out task))
            {
                task = new EasyWaitAnyTask();
            }
            task.Reuse(checkedTiming, autoRecycle);
            return task;
        }

        public EasyWaitAnyTask SetWaitTasks(params IEasyTaskInterface[] tasks)
        {
            IEasyTaskInterface[] copy = EasyWailAllTask.ValidateAndCopyTasks(tasks, true);
            lock (SyncRoot)
            {
                EnsureConfigurable();
                if (easyTasks != null)
                {
                    throw new InvalidOperationException("Wait tasks can only be assigned once.");
                }
                easyTasks = copy;
                EasyWailAllTask.RetainTasks(easyTasks);
                waitTasksReleased = false;
            }
            return this;
        }

        protected override void OnTerminate()
        {
            ReleaseWaitTasks();
        }

        protected override void OnRecycle()
        {
            ReleaseWaitTasks();
            easyTasks = null;
        }

        private void ReleaseWaitTasks()
        {
            IEasyTaskInterface[] tasks;
            lock (SyncRoot)
            {
                if (waitTasksReleased)
                {
                    return;
                }
                waitTasksReleased = true;
                tasks = easyTasks;
            }

            if (tasks == null)
            {
                return;
            }
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i].Release();
            }
        }

        protected override void ReturnToPool()
        {
            queue.Enqueue(this);
        }

        public override void MoveNext()
        {
            if (IsCompleted)
            {
                return;
            }

            for (int i = 0; i < easyTasks.Length; i++)
            {
                if (easyTasks[i].EasyTaskState != EasyTaskState.Pending)
                {
                    SetResult(i);
                    return;
                }
            }
        }
    }
}
