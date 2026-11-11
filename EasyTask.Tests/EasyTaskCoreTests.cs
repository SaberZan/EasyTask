using System;
using System.Threading;
using System.Threading.Tasks;
using Easy;

namespace EasyTask.Tests;

public sealed class EasyTaskCoreTests : RunnerTestBase
{
    [Fact]
    public async Task RunReturnsValue()
    {
        Assert.Equal(42, await PumpMain(EasyTaskRunner.Run(() => 42)));
    }

    [Fact]
    public async Task PendingFaultResumesAwaiterAndThrowsOriginalException()
    {
        var task = EasyTask<int>.Create(true);
        task.Start();
        Task<int> consumer = Consume(task);
        var expected = new InvalidOperationException("pending failure");

        await Task.Delay(20);
        task.SetException(expected);

        InvalidOperationException actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => WithTimeout(consumer));
        Assert.Same(expected, actual);
    }

    [Fact]
    public async Task AlreadyFaultedTaskThrowsInsteadOfReturningDefault()
    {
        var task = EasyTask<int>.Create(true);
        task.Start();
        var expected = new InvalidOperationException("synchronous failure");
        task.SetException(expected);

        InvalidOperationException actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => PumpMain(task));
        Assert.Same(expected, actual);
    }

    [Fact]
    public async Task PendingCancellationResumesAwaiter()
    {
        var token = new EasyCancellationToken();
        var task = EasyTask<int>.Create(true);
        _ = task.SetCancellationToken(token);
        task.Start();
        Task<int> consumer = Consume(task);

        await Task.Delay(20);
        token.Cancel();
        Task pump = Task.Run(async () =>
        {
            while (!consumer.IsCompleted)
            {
                EasyTaskRunner.Tick(Timing.Main);
                await Task.Delay(1);
            }
        });

        await Assert.ThrowsAsync<EasyCancelException>(() => WithTimeout(consumer));
        await pump;
    }

    [Fact]
    public async Task RunDelegateExceptionIsCaptured()
    {
        var expected = new InvalidOperationException("run failure");
        InvalidOperationException actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => PumpMain(EasyTaskRunner.Run<int>(() => throw expected)));
        Assert.Same(expected, actual);
    }

    [Fact]
    public async Task WaitUntilExceptionIsCaptured()
    {
        var expected = new InvalidOperationException("condition failure");
        InvalidOperationException actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => PumpMain(EasyTaskRunner.WaitUntil(() => throw expected)));
        Assert.Same(expected, actual);
    }

    [Fact]
    public async Task CompletionAndRegistrationCanRaceWithoutLosingContinuation()
    {
        EasyTaskRunner.StartThreadTiming(tickInterval: 1);
        for (int i = 0; i < 500; i++)
        {
            var task = EasyTask<int>.Create(false);
            task.Start();
            using var start = new ManualResetEventSlim(false);

            Task completion = Task.Run(() =>
            {
                start.Wait();
                task.SetResult(i);
            });
            Task<int> consumer = Task.Run(async () =>
            {
                start.Wait();
                return await task;
            });

            start.Set();
            Assert.Equal(i, await WithTimeout(consumer));
            await completion;
        }
    }

    [Fact]
    public async Task PoolDoesNotLeakDerivedRunBehaviorIntoAsyncTask()
    {
        Assert.Equal(123, await PumpMain(EasyTaskRunner.Run(() => 123)));

        for (int i = 0; i < 100; i++)
        {
            Assert.Equal(777, await PumpMain(ReturnAfterYield(777)));
        }
    }

    [Fact]
    public async Task AsyncBuilderPropagatesExceptionsBeforeAndAfterAwait()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(() => PumpMain(ThrowBeforeAwait()));
        await Assert.ThrowsAsync<InvalidOperationException>(() => PumpMain(ThrowAfterAwait()));
    }

    [Fact]
    public async Task AsyncBuilderMapsOperationCanceledToCancellation()
    {
        await Assert.ThrowsAsync<OperationCanceledException>(() => PumpMain(CancelAfterAwait()));
    }

    [Fact]
    public async Task SecondConsumptionIsRejectedForNonPooledTask()
    {
        var task = EasyTask<int>.Create(false);
        task.Start();
        task.SetResult(5);

        Assert.Equal(5, await Consume(task));
        await Assert.ThrowsAsync<InvalidOperationException>(() => Consume(task));
    }

    [Fact]
    public void GetResultBeforeCompletionIsRejected()
    {
        var task = EasyTask<int>.Create(false);
        Assert.Throws<InvalidOperationException>(() => task.GetResult());
    }

    [Fact]
    public void InvalidArgumentsAreRejected()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => EasyTaskRunner.Delay(-1));
        Assert.Throws<ArgumentNullException>(() => EasyTaskRunner.Run((Action)null));
        Assert.Throws<ArgumentNullException>(() => EasyTaskRunner.WaitUntil(null));
        Assert.Throws<ArgumentOutOfRangeException>(() => EasyTaskRunner.StartThreadTiming(tickInterval: 0));
    }

    private static async EasyTask<int> ReturnAfterYield(int value)
    {
        await EasyTaskRunner.Yield();
        return value;
    }

    private static async EasyTask<int> ThrowBeforeAwait()
    {
        throw new InvalidOperationException("before await");
#pragma warning disable CS0162
        await EasyTaskRunner.Yield();
#pragma warning restore CS0162
    }

    private static async EasyTask<int> ThrowAfterAwait()
    {
        await EasyTaskRunner.Yield();
        throw new InvalidOperationException("after await");
    }

    private static async EasyTask<int> CancelAfterAwait()
    {
        await EasyTaskRunner.Yield();
        throw new OperationCanceledException("requested");
    }
}
