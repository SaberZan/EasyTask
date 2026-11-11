using System;
using System.Linq;
using System.Threading.Tasks;
using Easy;

namespace EasyTask.Tests;

public sealed class EasyTaskCombinationTests : RunnerTestBase
{
    [Fact]
    public async Task WaitAllWaitsForEveryTask()
    {
        var first = EasyTask<int>.Create(true);
        var second = EasyTask<int>.Create(true);
        first.Start();
        second.Start();
        var all = EasyTaskRunner.WaitAll(first, second);
        Task<object> consumer = PumpMain(all);

        first.SetResult(1);
        await Task.Delay(30);
        Assert.False(consumer.IsCompleted);

        second.SetResult(2);
        Assert.Null(await consumer);
    }

    [Fact]
    public async Task WaitAllAggregatesFailuresAfterAllTasksFinish()
    {
        var first = EasyTask<int>.Create(true);
        var second = EasyTask<int>.Create(true);
        first.Start();
        second.Start();
        var all = EasyTaskRunner.WaitAll(first, second);

        first.SetException(new InvalidOperationException("first"));
        await Task.Delay(20);
        Assert.Equal(EasyTaskState.Pending, all.EasyTaskState);
        second.SetException(new ArgumentException("second"));

        AggregateException error = await Assert.ThrowsAsync<AggregateException>(() => PumpMain(all));
        Assert.Equal(2, error.InnerExceptions.Count);
    }

    [Fact]
    public async Task WaitAllReportsCancellationAsCanceled()
    {
        var first = EasyTask<int>.Create(true);
        var second = EasyTask<int>.Create(true);
        first.Start();
        second.Start();
        var all = EasyTaskRunner.WaitAll(first, second);

        first.SetException(new EasyCancelException());
        second.SetResult(2);

        await Assert.ThrowsAsync<EasyCancelException>(() => PumpMain(all));
    }

    [Fact]
    public async Task WaitAnyReturnsWhenTaskFaultsOrCancels()
    {
        var first = EasyTask<int>.Create(true);
        var second = EasyTask<int>.Create(true);
        first.Start();
        second.Start();
        var any = EasyTaskRunner.WaitAny(first, second);

        second.SetException(new InvalidOperationException("done"));

        Assert.Equal(1, await PumpMain(any));
    }

    [Fact]
    public async Task WaitAllCopiesInputArray()
    {
        var first = EasyTask<int>.Create(true);
        var second = EasyTask<int>.Create(true);
        var replacement = EasyTask<int>.Create(true);
        first.Start();
        second.Start();
        replacement.Start();
        var tasks = new IEasyTaskInterface[] { first, second };
        var all = EasyTaskRunner.WaitAll(tasks);
        tasks[0] = replacement;

        first.SetResult(1);
        second.SetResult(2);

        await PumpMain(all);
        Assert.Equal(EasyTaskState.Pending, replacement.EasyTaskState);
    }

    [Fact]
    public async Task EmptyWaitAllSucceedsAndEmptyWaitAnyIsRejected()
    {
        await PumpMain(EasyTaskRunner.WaitAll(Array.Empty<IEasyTaskInterface>()));
        Assert.Throws<ArgumentException>(() => EasyTaskRunner.WaitAny(Array.Empty<IEasyTaskInterface>()));
    }

    [Fact]
    public async Task CommonWaitAllUsageDoesNotConsumeChildResults()
    {
        EasyTask<int>[] children = Enumerable.Range(0, 20)
            .Select(i => EasyTaskRunner.Run(() => i))
            .ToArray();

        await PumpMain(EasyTaskRunner.WaitAll(children));

        for (int i = 0; i < children.Length; i++)
        {
            Assert.Equal(i, await Consume(children[i]));
        }
    }
}
