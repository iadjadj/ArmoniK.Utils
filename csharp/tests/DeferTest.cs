// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2022-2023.All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

namespace ArmoniK.Utils.Tests;

public class DeferTest
{
  ////////////////////////////
  // Synchronous Disposable //
  ////////////////////////////
  [Test]
  public void DeferEmptyShouldWork()
  {
    using var defer = Deferrer.Empty;
  }

  [Test]
  [TestCase(false)]
  [TestCase(true)]
  public void DeferShouldWork(bool async)
  {
    var i = 1;
    using (DisposableCreate(async,
                            0,
                            () => i += 1))
    {
      Assert.That(i,
                  Is.EqualTo(1));
    }

    Assert.That(i,
                Is.EqualTo(2));
  }


  [Test]
  [TestCase(false)]
  [TestCase(true)]
  public void RedundantDeferShouldWork(bool async)
  {
    var i = 1;

    var defer = DisposableCreate(async,
                                 0,
                                 () => i += 1);

    Assert.That(i,
                Is.EqualTo(1));

    defer.Dispose();

    Assert.That(i,
                Is.EqualTo(2));

    defer.Dispose();

    Assert.That(i,
                Is.EqualTo(2));
  }

  [Test]
  [TestCase(false)]
  [TestCase(true)]
  public async Task DeferShouldBeRaceConditionFree(bool async)
  {
    var i = 1;

    var defer = DisposableCreate(async,
                                 100,
                                 () => Interlocked.Increment(ref i));

    var task1 = Task.Run(() => defer.Dispose());
    var task2 = Task.Run(() => defer.Dispose());

    await task1;
    await task2;

    Assert.That(i,
                Is.EqualTo(2));
  }

  [Test]
  [TestCase(false)]
  [TestCase(true)]
  public void RedundantCopyDeferShouldWork(bool async)
  {
    var i = 1;

    {
      using var defer1 = DisposableCreate(async,
                                          100,
                                          () => i += 1);
      using var defer2 = defer1;

      Assert.That(i,
                  Is.EqualTo(1));
    }

    Assert.That(i,
                Is.EqualTo(2));
  }

  private static WeakReference WeakRefDisposable(Func<IDisposable> f)
    => new(f());

  [Test]
  [TestCase(false)]
  [TestCase(true)]
  public void DeferShouldWorkWhenCollected(bool async)
  {
    var i = 1;

    IDisposable reference;

    var weakRef = WeakRefDisposable(() =>
                                    {
                                      reference = DisposableCreate(async,
                                                                   0,
                                                                   () => i += 1);
                                      return reference;
                                    });

    GC.Collect();
    GC.WaitForPendingFinalizers();

    Assert.That(weakRef.IsAlive,
                Is.True);
    Assert.That(i,
                Is.EqualTo(1));

    reference = Deferrer.Empty;

    GC.Collect();
    GC.WaitForPendingFinalizers();

    Assert.That(weakRef.IsAlive,
                Is.False);

    Assert.That(i,
                Is.EqualTo(2));
  }

  [Test]
  [TestCase(false)]
  [TestCase(true)]
  public void WrappedDeferShouldWork(bool async)
  {
    var i = 1;
    using (new DisposableWrapper(DisposableCreate(async,
                                                  0,
                                                  () => i += 1)))
    {
      Assert.That(i,
                  Is.EqualTo(1));
    }

    Assert.That(i,
                Is.EqualTo(2));
  }

  /////////////////////////////
  // Asynchronous Disposable //
  /////////////////////////////
  [Test]
  public async Task AsyncDeferEmptyShouldWork()
  {
    await using var defer = Deferrer.Empty;
  }

  [Test]
  [TestCase(false)]
  [TestCase(true)]
  public async Task AsyncDeferShouldWork(bool async)
  {
    var i = 1;
    await using (AsyncDisposableCreate(async,
                                       0,
                                       () => i += 1))
    {
      Assert.That(i,
                  Is.EqualTo(1));
    }

    Assert.That(i,
                Is.EqualTo(2));
  }


  [Test]
  [TestCase(false)]
  [TestCase(true)]
  public async Task RedundantAsyncDeferShouldWork(bool async)
  {
    var i = 1;

    var defer = AsyncDisposableCreate(async,
                                      0,
                                      () => i += 1);

    Assert.That(i,
                Is.EqualTo(1));

    await defer.DisposeAsync()
               .ConfigureAwait(false);

    Assert.That(i,
                Is.EqualTo(2));

    await defer.DisposeAsync()
               .ConfigureAwait(false);

    Assert.That(i,
                Is.EqualTo(2));
  }

  [Test]
  [TestCase(false)]
  [TestCase(true)]
  public async Task AsyncDeferShouldBeRaceConditionFree(bool async)
  {
    var i = 1;

    var defer = AsyncDisposableCreate(async,
                                      100,
                                      () => Interlocked.Increment(ref i));

    var task1 = Task.Run(async () => await defer.DisposeAsync()
                                                .ConfigureAwait(false));
    var task2 = Task.Run(async () => await defer.DisposeAsync()
                                                .ConfigureAwait(false));

    await task1;
    await task2;

    Assert.That(i,
                Is.EqualTo(2));
  }

  [Test]
  [TestCase(false)]
  [TestCase(true)]
  public async Task RedundantCopyAsyncDeferShouldWork(bool async)
  {
    var i = 1;

    {
      await using var defer1 = AsyncDisposableCreate(async,
                                                     100,
                                                     () => i += 1);
      await using var defer2 = defer1;

      Assert.That(i,
                  Is.EqualTo(1));
    }

    Assert.That(i,
                Is.EqualTo(2));
  }

  private static WeakReference WeakRefAsyncDisposable(Func<IAsyncDisposable> f)
    => new(f());

  [Test]
  [TestCase(false)]
  [TestCase(true)]
  public void AsyncDeferShouldWorkWhenCollected(bool async)
  {
    var i = 1;

    IAsyncDisposable reference;

    var weakRef = WeakRefAsyncDisposable(() =>
                                         {
                                           reference = AsyncDisposableCreate(async,
                                                                             0,
                                                                             () => i += 1);
                                           return reference;
                                         });

    GC.Collect();
    GC.WaitForPendingFinalizers();

    Assert.That(weakRef.IsAlive,
                Is.True);
    Assert.That(i,
                Is.EqualTo(1));

    reference = Deferrer.Empty;

    GC.Collect();
    GC.WaitForPendingFinalizers();

    Assert.That(weakRef.IsAlive,
                Is.False);

    Assert.That(i,
                Is.EqualTo(2));
  }

  [Test]
  [TestCase(false)]
  [TestCase(true)]
  public async Task WrappedAsyncDeferShouldWork(bool async)
  {
    var i = 1;
    await using (new AsyncDisposableWrapper(AsyncDisposableCreate(async,
                                                                  0,
                                                                  () => i += 1)))
    {
      Assert.That(i,
                  Is.EqualTo(1));
    }

    Assert.That(i,
                Is.EqualTo(2));
  }

  private static Deferrer DeferrerCreate(bool   async,
                                         int    delay,
                                         Action f)
  {
    if (async)
    {
      return new Deferrer(async () =>
                          {
                            if (delay > 0)
                            {
                              await Task.Delay(delay);
                            }
                            else
                            {
                              await Task.Yield();
                            }

                            f();
                          });
    }

    return new Deferrer(() =>
                        {
                          if (delay > 0)
                          {
                            Thread.Sleep(delay);
                          }
                          else
                          {
                            Thread.Yield();
                          }

                          f();
                        });
  }

  private static IDisposable DisposableCreate(bool   async,
                                              int    delay,
                                              Action f)
    => DeferrerCreate(async,
                      delay,
                      f);

  private static IAsyncDisposable AsyncDisposableCreate(bool   async,
                                                        int    delay,
                                                        Action f)
    => DeferrerCreate(async,
                      delay,
                      f);

  ///////////
  // Utils //
  ///////////
  private record DisposableWrapper(IDisposable Disposable) : IDisposable
  {
    public void Dispose()
      => Disposable.Dispose();
  }

  private record AsyncDisposableWrapper(IAsyncDisposable AsyncDisposable) : IAsyncDisposable
  {
    public ValueTask DisposeAsync()
      => AsyncDisposable.DisposeAsync();
  }
}
