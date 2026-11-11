using System;
using System.Threading.Tasks;
using Easy;

namespace EasyTask.Tests;

public sealed class EasyTaskAwaiterIntegrationTests : RunnerTestBase
{
    [Fact]
    public async Task EasyTaskMethodAwaitsYieldDelayAndRun()
    {
        Assert.Equal(42, await PumpMain(AwaitCoreEasyTasks()));
    }

    [Fact]
    public async Task EasyTaskMethodAwaitsEasyVoidTask()
    {
        Assert.Equal(7, await PumpMain(AwaitEasyVoidTask()));
    }

    [Fact]
    public async Task EasyTaskMethodAwaitsWaitAllAndThenChildren()
    {
        Assert.Equal(30, await PumpMain(AwaitEasyWaitAll()));
    }

    [Fact]
    public async Task EasyTaskMethodPropagatesWaitAllFailures()
    {
        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => PumpMain(AwaitFailedEasyWaitAll()));

        Assert.Equal("wait all failure", error.Message);
    }

    [Fact]
    public async Task EasyTaskMethodAwaitsWaitAnyAndThenChildren()
    {
        Assert.Equal(23, await PumpMain(AwaitEasyWaitAny()));
    }

    [Fact]
    public async Task AwaitingNonPooledTaskTwiceThrowsInsideEasyTaskMethod()
    {
        var inner = EasyTask<int>.Create(false);
        inner.Start();
        inner.SetResult(5);

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => PumpMain(AwaitSameTaskTwice(inner)));

        Assert.Contains("only be awaited once", error.Message);
    }

    [Fact]
    public async Task AwaitingAutoPooledTaskTwiceThrowsInsteadOfHanging()
    {
        var inner = EasyTask<int>.Create(true);
        inner.Start();
        inner.SetResult(5);
        EasyTaskRunner.Tick(Timing.Main);

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => PumpMain(AwaitSameTaskTwice(inner)));

        Assert.Contains("only be awaited once", error.Message);
    }

    [Fact]
    public async Task TwoPendingAwaitersAreRejectedInsideEasyTaskMethods()
    {
        var inner = EasyTask<int>.Create(false);
        inner.Start();
        EasyTask<int> first = AwaitValue(inner);
        EasyTask<int> second = AwaitValue(inner);

        inner.SetResult(11);

        Assert.Equal(11, await PumpMain(first));
        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => PumpMain(second));
        Assert.Contains("only one awaiter", error.Message);
    }

    private static async EasyTask<int> AwaitCoreEasyTasks()
    {
        await EasyTaskRunner.Yield();
        await EasyTaskRunner.Delay(1);
        int value = await EasyTaskRunner.Run(() => 42);
        return value;
    }

    private static async EasyTask<int> AwaitEasyVoidTask()
    {
        await YieldVoid();
        return 7;
    }

    private static async EasyVoidTask YieldVoid()
    {
        await EasyTaskRunner.Yield();
    }

    private static async EasyTask<int> AwaitEasyWaitAll()
    {
        EasyTask<int> first = EasyTaskRunner.Run(() => 10);
        EasyTask<int> second = ReturnAfterYield(20);

        await EasyTaskRunner.WaitAll(first, second);

        int firstValue = await first;
        int secondValue = await second;
        return firstValue + secondValue;
    }

    private static async EasyVoidTask AwaitFailedEasyWaitAll()
    {
        EasyTask<int> first = ThrowAfterYield();
        EasyTask<int> second = ReturnAfterYield(20);
        await EasyTaskRunner.WaitAll(first, second);
    }

    private static async EasyTask<int> AwaitEasyWaitAny()
    {
        EasyTask<int> slow = ReturnAfterDelay(30, 3);
        EasyTask<int> fast = ReturnAfterYield(2);

        int index = await EasyTaskRunner.WaitAny(slow, fast);
        if (index != 1)
        {
            throw new InvalidOperationException("The fast EasyTask should finish first.");
        }

        int fastValue = await fast;
        int slowValue = await slow;
        return fastValue * 10 + slowValue;
    }

    private static async EasyVoidTask AwaitSameTaskTwice(EasyTask<int> task)
    {
        int first = await task;
        int second = await task;
        if (first != second)
        {
            throw new InvalidOperationException("Unreachable duplicate result.");
        }
    }

    private static async EasyTask<int> AwaitValue(EasyTask<int> task)
    {
        return await task;
    }

    private static async EasyTask<int> ReturnAfterYield(int value)
    {
        await EasyTaskRunner.Yield();
        return value;
    }

    private static async EasyTask<int> ReturnAfterDelay(int milliseconds, int value)
    {
        await EasyTaskRunner.Delay(milliseconds);
        return value;
    }

    private static async EasyTask<int> ThrowAfterYield()
    {
        await EasyTaskRunner.Yield();
        throw new InvalidOperationException("wait all failure");
    }
}
