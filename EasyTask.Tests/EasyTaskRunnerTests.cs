using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Easy;

namespace EasyTask.Tests;

public sealed class EasyTaskRunnerTests : RunnerTestBase
{
    [Fact]
    public async Task ConcurrentThreadTaskCreationDoesNotLoseTasks()
    {
        EasyTaskRunner.StartThreadTiming(tickInterval: 1);

        EasyRunTask<int>[] tasks = await Task.WhenAll(Enumerable.Range(0, 200).Select(i => Task.Run(() =>
            EasyTaskRunner.Run(() => i))));

        int[] results = await PumpMain(ConsumeRunResults(tasks));
        Assert.Equal(Enumerable.Range(0, 200), results);
    }

    [Fact]
    public async Task InitializationCannotBeObservedBeforeRunDelegateIsInstalled()
    {
        EasyTaskRunner.StartThreadTiming(tickInterval: 1);

        for (int i = 0; i < 300; i++)
        {
            int expected = i;
            EasyRunTask<int> task = await Task.Run(() => EasyTaskRunner.Run(() => expected));
            Assert.Equal(expected, await WithTimeout(Consume(task)));
        }
    }

    [Fact]
    public async Task RepeatedStartDoesNotDuplicateActionExecution()
    {
        EasyTaskRunner.StartThreadTiming(ThreadMode.RunInOneThread, 1);
        EasyTaskRunner.StartThreadTiming(ThreadMode.RunInOneThread, 1);
        int count = 0;

        EasyTaskRunner.ExecInThread(() => Interlocked.Increment(ref count));
        await WithTimeout(Task.Run(async () =>
        {
            while (Volatile.Read(ref count) == 0) await Task.Delay(1);
        }));
        await Task.Delay(50);

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task ActionExceptionDoesNotStopThreadPump()
    {
        EasyTaskRunner.StartThreadTiming(tickInterval: 1);
        int count = 0;

        EasyTaskRunner.ExecInThread(() => throw new InvalidOperationException("action"));
        EasyTaskRunner.ExecInThread(() => Interlocked.Increment(ref count));

        await WithTimeout(Task.Run(async () =>
        {
            while (Volatile.Read(ref count) == 0) await Task.Delay(1);
        }));
        Assert.Equal(1, count);
    }

    [Theory]
    [InlineData(ThreadMode.RunInThreadPool)]
    [InlineData(ThreadMode.RunInOneThread)]
    public async Task ResetFromThreadActionDoesNotDeadlock(ThreadMode mode)
    {
        EasyTaskRunner.StartThreadTiming(mode, 1);
        using var completed = new ManualResetEventSlim(false);

        EasyTaskRunner.ExecInThread(() =>
        {
            EasyTaskRunner.ResetTask();
            completed.Set();
        });

        await WithTimeout(Task.Run(() => completed.Wait()));
        Assert.False(EasyTaskRunner.IsStartThreadTiming);
    }

    [Fact]
    public async Task ResetCancelsPendingAwaiter()
    {
        var task = EasyTaskRunner.Delay(60000);
        Task<object> consumer = Consume(task);
        await Task.Delay(20);

        EasyTaskRunner.ResetTask();

        await Assert.ThrowsAsync<EasyCancelException>(() => WithTimeout(consumer));
    }

    [Fact]
    public async Task LateRunResultCannotOverwriteCancellation()
    {
        using var gate = new ManualResetEventSlim(false);
        using var started = new ManualResetEventSlim(false);
        var token = new EasyCancellationToken();
        var task = EasyTaskRunner.Run(() =>
        {
            started.Set();
            gate.Wait();
            return 10;
        }, token);
        Task<int> consumer = Consume(task);
        Task pump = Task.Run(async () =>
        {
            while (!consumer.IsCompleted)
            {
                EasyTaskRunner.Tick(Timing.Main);
                await Task.Delay(1);
            }
        });

        started.Wait();
        token.Cancel();
        await Assert.ThrowsAsync<EasyCancelException>(() => WithTimeout(consumer));
        gate.Set();
        await pump;
    }

    [Fact]
    public async Task CanceledRunCannotBeReusedUntilOldWorkerExits()
    {
        using var oldStarted = new ManualResetEventSlim(false);
        using var releaseOld = new ManualResetEventSlim(false);
        var oldPayload = new Payload();
        var newPayload = new Payload();
        var token = new EasyCancellationToken();
        var oldTask = EasyTaskRunner.Run(() =>
        {
            oldStarted.Set();
            releaseOld.Wait();
            return oldPayload;
        }, token);
        Task<Payload> oldConsumer = Consume(oldTask);
        Task oldPump = Task.Run(async () =>
        {
            while (!oldConsumer.IsCompleted)
            {
                EasyTaskRunner.Tick(Timing.Main);
                await Task.Delay(1);
            }
        });

        oldStarted.Wait();
        token.Cancel();
        await Assert.ThrowsAsync<EasyCancelException>(() => WithTimeout(oldConsumer));
        await oldPump;

        int newFuncCalls = 0;
        var newTask = EasyTaskRunner.Run(() =>
        {
            Interlocked.Increment(ref newFuncCalls);
            return newPayload;
        });
        Task<Payload> newConsumer = Consume(newTask);
        Task newPump = Task.Run(async () =>
        {
            while (!newConsumer.IsCompleted)
            {
                EasyTaskRunner.Tick(Timing.Main);
                await Task.Delay(1);
            }
        });

        try
        {
            Assert.NotSame(oldTask, newTask);
            await WithTimeout(Task.Run(async () =>
            {
                while (Volatile.Read(ref newFuncCalls) == 0) await Task.Delay(1);
            }));
        }
        finally
        {
            releaseOld.Set();
        }

        Assert.Same(newPayload, await WithTimeout(newConsumer));
        await newPump;
        Assert.Equal(1, newFuncCalls);
    }

    [Fact]
    public async Task FinalReleaseAfterRunnerRemovalReturnsTaskToPool()
    {
        var expected = new Payload();
        var first = EasyTask<Payload>.Create(true);
        first.Start();
        first.SetResult(expected);
        EasyTaskRunner.Tick(Timing.Main);

        Assert.Same(expected, await Consume(first));
        var second = EasyTask<Payload>.Create(true);
        Assert.Same(first, second);

        second.Start();
        second.SetResult(expected);
        Assert.Same(expected, await Consume(second));
        EasyTaskRunner.Tick(Timing.Main);
    }

    [Fact]
    public async Task ResetCannotRecycleAsyncBuilderWhileExternalAwaiterIsPending()
    {
        var awaitable = new ManualAwaitable<BuilderPayload>();
        var oldPayload = new BuilderPayload();
        var newPayload = new BuilderPayload();
        var oldTask = AwaitExternal(awaitable);
        Task<BuilderPayload> oldConsumer = Consume(oldTask);

        EasyTaskRunner.ResetTask();
        await Assert.ThrowsAsync<EasyCancelException>(() => WithTimeout(oldConsumer));

        var newTask = EasyTask<BuilderPayload>.Create(true);
        bool reusedWhileOldStateMachineWasAlive = ReferenceEquals(oldTask, newTask);
        newTask.Start();
        Task<BuilderPayload> newConsumer = Consume(newTask);

        // This synchronously resumes the old state machine. Its builder must only
        // release the old lifetime, never complete the newly created task.
        awaitable.Complete(oldPayload);
        if (!newConsumer.IsCompleted)
        {
            newTask.SetResult(newPayload);
        }

        Assert.False(reusedWhileOldStateMachineWasAlive);
        Assert.Same(newPayload, await WithTimeout(newConsumer));
        EasyTaskRunner.Tick(Timing.Main);
    }

    [Fact]
    public async Task OldTickDoesNotCancelTaskPublishedAfterReset()
    {
        using var entered = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        var blocker = EasyWaitUntilTask.Create(false);
        _ = blocker.SetCondition(() =>
        {
            entered.Set();
            release.Wait();
            return false;
        });
        blocker.Start();
        var dummy = EasyTaskRunner.Delay(60000);
        Task oldTick = Task.Run(() => EasyTaskRunner.Tick(Timing.Main));

        entered.Wait();
        EasyTaskRunner.ResetTask();
        var newTask = EasyTask<int>.Create(false);
        newTask.Start();
        release.Set();
        await WithTimeout(oldTick);

        Assert.Equal(EasyTaskState.Pending, newTask.EasyTaskState);
        newTask.SetResult(9);
        Assert.Equal(9, await Consume(newTask));
    }

    [Theory]
    [InlineData(ThreadMode.RunInThreadPool)]
    [InlineData(ThreadMode.RunInOneThread)]
    public async Task ExternalResetDoesNotDeadlockWithStartInsideAction(ThreadMode mode)
    {
        EasyTaskRunner.StartThreadTiming(mode, 1);
        using var entered = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        EasyTaskRunner.ExecInThread(() =>
        {
            entered.Set();
            release.Wait();
            try
            {
                EasyTaskRunner.StartThreadTiming(mode, 1);
            }
            catch (InvalidOperationException)
            {
                // A reset is already in progress; the action must still be able to exit.
            }
        });

        entered.Wait();
        Task externalReset = Task.Run(() => EasyTaskRunner.ResetTask());
        await WithTimeout(Task.Run(async () =>
        {
            while (EasyTaskRunner.IsStartThreadTiming) await Task.Delay(1);
        }));
        release.Set();

        await WithTimeout(externalReset);
        Assert.False(EasyTaskRunner.IsStartThreadTiming);
    }

    [Fact]
    public async Task ConcurrentMainTicksProduceEachResultExactlyOnce()
    {
        EasyTaskRunner.StartThreadTiming(tickInterval: 1);
        var tasks = new ConcurrentBag<EasyRunTask<int>>();
        for (int i = 0; i < 200; i++)
        {
            int value = i;
            tasks.Add(EasyTaskRunner.Run(() => value));
        }

        Task<int[]> results = PumpMain(ConsumeRunResults(tasks.ToArray()));
        Task[] ticks = Enumerable.Range(0, 8).Select(_ => Task.Run(() =>
        {
            for (int i = 0; i < 100; i++) EasyTaskRunner.Tick(Timing.Main);
        })).ToArray();

        int[] values = await results;
        await Task.WhenAll(ticks);
        Assert.Equal(Enumerable.Range(0, 200), values.OrderBy(value => value));
    }

    private static async EasyTask<int[]> ConsumeRunResults(EasyRunTask<int>[] tasks)
    {
        await EasyTaskRunner.WaitAll(tasks);
        var results = new int[tasks.Length];
        for (int i = 0; i < tasks.Length; i++)
        {
            results[i] = await tasks[i];
        }
        return results;
    }

    private static async EasyTask<BuilderPayload> AwaitExternal(ManualAwaitable<BuilderPayload> awaitable)
    {
        return await awaitable;
    }

    private sealed class ManualAwaitable<T> : System.Runtime.CompilerServices.INotifyCompletion
    {
        private Action continuation;
        private T result;

        public ManualAwaitable<T> GetAwaiter()
        {
            return this;
        }

        public bool IsCompleted
        {
            get { return false; }
        }

        public void OnCompleted(Action continuation)
        {
            this.continuation = continuation;
        }

        public void Complete(T value)
        {
            result = value;
            Action current = continuation ?? throw new InvalidOperationException("The awaiter was not registered.");
            current();
        }

        public T GetResult()
        {
            return result;
        }
    }

    private sealed class Payload
    {
    }

    private sealed class BuilderPayload
    {
    }
}
