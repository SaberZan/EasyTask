using System;
using System.Threading.Tasks;
using Easy;

namespace EasyTask.Tests;

public sealed class EasyTaskForgetTests : RunnerTestBase
{
    [Fact]
    public void MainModeDoesNotRequireTaskSynchronization()
    {
        var mainTask = EasyTask<int>.Create(false);
        Assert.False(EasyTaskRunner.RequiresTaskSynchronization);
        using (var taskLock = mainTask.CreateLock())
        {
            Assert.False(taskLock.IsLockTaken);
        }

        EasyTaskRunner.StartThreadTiming(tickInterval: 1);
        Assert.True(EasyTaskRunner.RequiresTaskSynchronization);
        using (var taskLock = mainTask.CreateLock())
        {
            Assert.True(taskLock.IsLockTaken);
        }

        EasyTaskRunner.ResetTask();
        Assert.False(EasyTaskRunner.RequiresTaskSynchronization);
        // Tasks that participated in threaded mode remain synchronized. A new
        // single-thread task returns to the no-Monitor fast path.
        using (var taskLock = mainTask.CreateLock())
        {
            Assert.True(taskLock.IsLockTaken);
        }
        var freshMainTask = EasyTask<int>.Create(false);
        using (var taskLock = freshMainTask.CreateLock())
        {
            Assert.False(taskLock.IsLockTaken);
        }
    }

    [Fact]
    public async Task ForgottenRunTaskExecutesAndRecyclesWithoutAwait()
    {
        bool executed = false;
        EasyRunTask<int> task = EasyTaskRunner.Run(() =>
        {
            executed = true;
            return 42;
        });

        task.Forget();
        for (int i = 0; i < 5000 && !task.IsInPool; i++)
        {
            EasyTaskRunner.Tick(Timing.Main);
            await Task.Delay(1);
        }

        Assert.True(executed);
        Assert.True(task.IsInPool);
    }

    [Fact]
    public void ForgottenCompletedUnscheduledTaskRecyclesImmediately()
    {
        var task = EasyTask<int>.Create(true);
        task.SetResult(42);

        task.Forget();

        Assert.True(task.IsInPool);
    }

    [Fact]
    public void ForgottenPendingCompletionSourceRecyclesWhenCompleted()
    {
        var task = EasyTask<int>.Create(true);
        task.Forget();

        task.SetResult(42);

        Assert.True(task.IsInPool);
    }

    [Fact]
    public void ForgottenAsyncEasyTaskRecyclesAfterCompletionAndRunnerRemoval()
    {
        EasyTask<int> task = ReturnAfterYieldForForget();
        task.Forget();

        EasyTaskRunner.Tick(Timing.Main); // Resume the async state machine.
        EasyTaskRunner.Tick(Timing.Main); // Remove the completed outer task.

        Assert.True(task.IsInPool);
    }

    [Fact]
    public async Task ForgottenFaultedTaskRecyclesWithoutEscalatingToScheduler()
    {
        Func<int> action = () => throw new InvalidOperationException("forgotten failure");
        EasyRunTask<int> task = EasyTaskRunner.Run(action);
        task.Forget();

        for (int i = 0; i < 5000 && !task.IsInPool; i++)
        {
            EasyTaskRunner.Tick(Timing.Main);
            await Task.Delay(1);
        }

        Assert.True(task.IsInPool);
    }

    [Fact]
    public async Task ForgottenThreadRunRecyclesAfterWorkerExits()
    {
        EasyTaskRunner.StartThreadTiming(tickInterval: 1);
        bool executed = false;
        var task = EasyRunTask<int>.Create(true, Timing.Thread);
        _ = task.SetFunc(() =>
        {
            executed = true;
            return 42;
        });
        task.Start();
        task.Forget();

        for (int i = 0; i < 5000 && !task.IsInPool; i++)
        {
            await Task.Delay(1);
        }

        Assert.True(executed);
        Assert.True(task.IsInPool);
    }

    [Fact]
    public async Task ForgottenTaskCannotBeAwaited()
    {
        EasyRunTask<int> inner = EasyTaskRunner.Run(() => 42);
        inner.Forget();

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => PumpMain(AwaitForgotten(inner)));

        Assert.Contains("forgotten", error.Message);
    }

    [Fact]
    public void ForgetTwiceThrows()
    {
        var task = EasyTask<int>.Create(false);
        task.Forget();

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(task.Forget);
        Assert.Contains("already been forgotten", error.Message);
    }

    [Fact]
    public async Task TaskWithRegisteredAwaiterCannotBeForgotten()
    {
        var task = EasyTask<int>.Create(false);
        var awaiter = task.GetAwaiter();
        awaiter.UnsafeOnCompleted(() => { });

        InvalidOperationException error = Assert.Throws<InvalidOperationException>(task.Forget);
        Assert.Contains("registered awaiter", error.Message);

        task.SetResult(5);
        Assert.Equal(5, await PumpMain(task));
    }

    [Fact]
    public void ExtractedAwaiterStillPreventsForget()
    {
        var task = EasyTask<int>.Create(false);
        var awaiter = task.GetAwaiter();
        Exception forgetError = null;
        int observed = 0;
        awaiter.UnsafeOnCompleted(() =>
        {
            try
            {
                task.Forget();
            }
            catch (Exception error)
            {
                forgetError = error;
            }
#pragma warning disable xUnit1031 // Deliberately test the extracted awaiter before GetResult.
            observed = awaiter.GetResult();
#pragma warning restore xUnit1031
        });

        task.SetResult(9);

        Assert.IsType<InvalidOperationException>(forgetError);
        Assert.Equal(9, observed);
    }

    [Fact]
    public async Task ForgottenWaitAnyLoserContinuesAndRecycles()
    {
        bool secondReady = false;
        var first = EasyWaitUntilTask.Create(true);
        _ = first.SetCondition(() => true);
        first.Start();
        var second = EasyWaitUntilTask.Create(true);
        _ = second.SetCondition(() => secondReady);
        second.Start();
        second.Forget();

        int index = await PumpMain(EasyTaskRunner.WaitAny(first, second));
        Assert.Equal(0, index);
        Assert.False(second.IsCompleted);

        Assert.Equal(default(object), await PumpMain(first));
        secondReady = true;
        EasyTaskRunner.Tick(Timing.Main);

        Assert.True(second.IsInPool);
    }

    [Fact]
    public void RecycleCallbackCanReenterRunnerLifecycleWithoutSelfDeadlock()
    {
        var task = new ReentrantRecycleTask();
        task.SetResult(42);

        task.Forget();

        Assert.True(task.IsInPool);
        Assert.False(EasyTaskRunner.IsStartThreadTiming);
    }

    private sealed class ReentrantRecycleTask : EasyTask<int>
    {
        internal ReentrantRecycleTask()
        {
            Reuse(Timing.Main, true);
        }

        protected override void OnRecycle()
        {
            EasyTaskRunner.ResetTask();
        }

        protected override void ReturnToPool()
        {
            // This test only needs the lifecycle callback, not another pool.
        }
    }

    private static async EasyTask<int> ReturnAfterYieldForForget()
    {
        await EasyTaskRunner.Yield();
        return 42;
    }

    private static async EasyVoidTask AwaitForgotten(EasyTask<int> task)
    {
        await task;
    }
}
