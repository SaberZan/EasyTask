# EasyTask

一个支持自定义 `async EasyTask<T> / EasyVoidTask` 方法的轻量任务调度库。

## 构建与测试

```powershell
dotnet build EasyTask.csproj --configuration Release
dotnet test EasyTask.Tests/EasyTask.Tests.csproj --configuration Release
```

库目标框架为 `netstandard2.1`；测试目标框架为 `net8.0`。

## 基本用法

主线程任务需要在游戏循环中持续调用：

```csharp
void Update()
{
    EasyTaskRunner.Tick(Timing.Main);
}
```

常用任务：

```csharp
async EasyTask<int> LoadValueAsync()
{
    await EasyTaskRunner.Delay(100);
    int value = await EasyTaskRunner.Run(() => 42);
    return value;
}

async EasyVoidTask LoadAsync()
{
    await EasyTaskRunner.Yield();
    await EasyTaskRunner.WaitUntil(() => IsReady);
}
```

可选后台调度器：

```csharp
EasyTaskRunner.StartThreadTiming(ThreadMode.RunInThreadPool, tickInterval: 15);
EasyTaskRunner.StartThreadTiming(ThreadMode.RunInOneThread, tickInterval: 15);

// 停止后台调度并取消所有仍在队列中的任务：
EasyTaskRunner.ResetTask();
```

重复调用 `StartThreadTiming` 不会重复启动。Reset 期间不允许重新启动，调用方可在 `ResetTask()` 返回后重试。

## 完成、异常与取消

- 成功、失败和取消都会唤醒 awaiter。
- `Faulted` 会重新抛出原异常，并保留原堆栈。
- `OperationCanceledException` 映射为 `Canceled`。
- `WaitAll` 等待所有子任务进入终态；多个失败通过 `AggregateException` 返回，任一取消且没有失败时返回取消。
- `WaitAny` 返回第一个进入终态的子任务索引，子任务失败或取消也会结束等待；它不会自动消费子任务结果。
- `WaitAny` 不接受空数组，`WaitAll` 接受空数组并立即成功。

取消示例：

```csharp
var cancellation = new EasyCancellationToken();
var task = EasyTaskRunner.Delay(1000, cancellation);
cancellation.Cancel();
```

## 手动创建任务

手动 `Create` 不再自动调度。必须先完成配置，再调用 `Start()`：

```csharp
var task = EasyWaitUntilTask.Create(autoRecycle: true);
task.SetCondition(() => IsReady);
task.SetCancellationToken(cancellation);
task.Start();
```

`Start()` 之后不能再修改条件、委托、延迟时间、取消令牌或组合任务数组。

## 单次等待与对象池约定

`EasyTask` 是低分配、可池化的**单次消费任务**，不是 `System.Threading.Tasks.Task` 的完全替代品：

- 每个任务只能 `await/GetResult` 一次，也只能有一个 Pending continuation。
- `autoRecycle: true` 的任务被消费后可能立即复用；不要再读取其状态、异常或保存旧 awaiter。
- 子任务可以在 `WaitAll/WaitAny` 完成后分别消费一次；组合任务负责释放自己持有的引用。
- 不要手动重复使用已消费任务；需要新任务时重新 `Create`。
- Reset 会取消队列中的任务，但不会强行终止正在执行的用户委托或外部 `Task`。内部 worker/async builder 引用会阻止旧执行过程污染复用对象。

## 兼容性调整

本次正确性修复包含以下有意的行为变化：

- `Create()` 后必须显式 `Start()`；`EasyTaskRunner` 工厂方法已经自动调用。
- `GetResult()` 在 Pending 时会抛异常，不再返回默认值或提前释放引用。
- 已完成且失败/取消的任务不再静默返回 `default`。
- 多次等待或注册多个 Pending continuation 会明确抛异常。
- `EasyCancellationToken.IsCanceled` 改为只读，通过 `Cancel()` 取消。
- 不再使用 `Thread.Abort()`。
- `EasyWailAllTask` 和 `EasyAsyncGenericTaskMethodBuider` 等现有公开名称暂时保留，以避免不必要的 API 重命名破坏。
