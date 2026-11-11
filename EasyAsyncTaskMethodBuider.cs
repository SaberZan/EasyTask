using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Easy
{
    internal sealed class EasyAsyncTaskLifetime
    {
        internal int Completed;
    }

    public struct EasyAsyncGenericTaskMethodBuider
    {
        private EasyVoidTask easyTask;
        private EasyAsyncTaskLifetime lifetime;

        public EasyVoidTask Task { get { return easyTask; } }

        public static EasyAsyncGenericTaskMethodBuider Create()
        {
            EasyVoidTask task = EasyVoidTask.Create(true);
            task.Start();
            task.Retain();
            return new EasyAsyncGenericTaskMethodBuider
            {
                easyTask = task,
                lifetime = new EasyAsyncTaskLifetime()
            };
        }

        public void SetException(Exception exception)
        {
            if (Interlocked.Exchange(ref lifetime.Completed, 1) != 0)
            {
                return;
            }
            try
            {
                easyTask.SetException(exception);
            }
            finally
            {
                easyTask.Release();
            }
        }

        public void SetResult()
        {
            if (Interlocked.Exchange(ref lifetime.Completed, 1) != 0)
            {
                return;
            }
            try
            {
                easyTask.SetResult(null);
            }
            finally
            {
                easyTask.Release();
            }
        }

        public void AwaitOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
            where TAwaiter : INotifyCompletion
            where TStateMachine : IAsyncStateMachine
        {
            awaiter.OnCompleted(stateMachine.MoveNext);
        }

        public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
            where TAwaiter : ICriticalNotifyCompletion
            where TStateMachine : IAsyncStateMachine
        {
            awaiter.UnsafeOnCompleted(stateMachine.MoveNext);
        }

        public void Start<TStateMachine>(ref TStateMachine stateMachine)
            where TStateMachine : IAsyncStateMachine
        {
            stateMachine.MoveNext();
        }

        public void SetStateMachine(IAsyncStateMachine stateMachine)
        {
        }
    }

    public class EasyAsyncGenericTaskMethodBuider<T>
    {
        private EasyTask<T> easyTask;
        private EasyAsyncTaskLifetime lifetime;

        public EasyTask<T> Task { get { return easyTask; } }

        public static EasyAsyncGenericTaskMethodBuider<T> Create()
        {
            EasyTask<T> task = EasyTask<T>.Create(true);
            task.Start();
            task.Retain();
            return new EasyAsyncGenericTaskMethodBuider<T>
            {
                easyTask = task,
                lifetime = new EasyAsyncTaskLifetime()
            };
        }

        public void SetException(Exception exception)
        {
            if (Interlocked.Exchange(ref lifetime.Completed, 1) != 0)
            {
                return;
            }
            try
            {
                easyTask.SetException(exception);
            }
            finally
            {
                easyTask.Release();
            }
        }

        public void SetResult(T ret)
        {
            if (Interlocked.Exchange(ref lifetime.Completed, 1) != 0)
            {
                return;
            }
            try
            {
                easyTask.SetResult(ret);
            }
            finally
            {
                easyTask.Release();
            }
        }

        public void AwaitOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
            where TAwaiter : INotifyCompletion
            where TStateMachine : IAsyncStateMachine
        {
            awaiter.OnCompleted(stateMachine.MoveNext);
        }

        public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
            where TAwaiter : ICriticalNotifyCompletion
            where TStateMachine : IAsyncStateMachine
        {
            awaiter.UnsafeOnCompleted(stateMachine.MoveNext);
        }

        public void Start<TStateMachine>(ref TStateMachine stateMachine)
            where TStateMachine : IAsyncStateMachine
        {
            stateMachine.MoveNext();
        }

        public void SetStateMachine(IAsyncStateMachine stateMachine)
        {
        }
    }
}
