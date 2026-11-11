using System;
using System.Threading;
using System.Threading.Tasks;
using Easy;
using Xunit;

namespace EasyTask.Tests;

public abstract class RunnerTestBase : IDisposable
{
    protected static async Task<T> PumpMain<T>(EasyTask<T> task, int timeoutMilliseconds = 5000)
    {
        Task<T> consumer = Consume(task);
        using var stopPump = new CancellationTokenSource();
        Task pump = Task.Run(async () =>
        {
            while (!task.IsCompleted && !stopPump.IsCancellationRequested)
            {
                EasyTaskRunner.Tick(Timing.Main);
                await Task.Delay(1);
            }
            EasyTaskRunner.Tick(Timing.Main);
        });

        Task finished = await Task.WhenAny(consumer, Task.Delay(timeoutMilliseconds));
        stopPump.Cancel();
        await pump;
        if (finished != consumer)
        {
            throw new TimeoutException("The EasyTask did not complete within the test timeout.");
        }
        return await consumer;
    }

    protected static async Task<T> Consume<T>(EasyTask<T> task)
    {
        return await task;
    }

    protected static async Task<T> WithTimeout<T>(Task<T> task, int timeoutMilliseconds = 5000)
    {
        Task finished = await Task.WhenAny(task, Task.Delay(timeoutMilliseconds));
        if (finished != task)
        {
            throw new TimeoutException("The operation did not complete within the test timeout.");
        }
        return await task;
    }

    protected static async Task WithTimeout(Task task, int timeoutMilliseconds = 5000)
    {
        Task finished = await Task.WhenAny(task, Task.Delay(timeoutMilliseconds));
        if (finished != task)
        {
            throw new TimeoutException("The operation did not complete within the test timeout.");
        }
        await task;
    }

    public void Dispose()
    {
        EasyTaskRunner.ResetTask();
        EasyTaskRunner.Tick(Timing.Main);
        EasyTaskRunner.Tick(Timing.Thread);
        GC.SuppressFinalize(this);
    }
}
