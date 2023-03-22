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
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

namespace ArmoniK.Utils.Tests;

public class ParallelSelectExtTest
{
  [Test]
  [TestCase(-1,
            0)]
  [TestCase(-1,
            1)]
  [TestCase(-1,
            4)]
  [TestCase(-1,
            1000)]
  [TestCase(0,
            0)]
  [TestCase(0,
            1)]
  [TestCase(0,
            4)]
  [TestCase(0,
            100)]
  [TestCase(1,
            0)]
  [TestCase(1,
            1)]
  [TestCase(1,
            4)]
  [TestCase(1,
            10)]
  [TestCase(2,
            0)]
  [TestCase(2,
            1)]
  [TestCase(2,
            4)]
  [TestCase(2,
            20)]
  public async Task ParallelSelectShouldSucceed(int parallelism,
                                                int n)
  {
    var x = await GenerateInts(n)
                  .ParallelSelect(new ParallelTaskOptions(parallelism),
                                  AsyncIdentity(0,
                                                10))
                  .ToListAsync()
                  .ConfigureAwait(false);
    var y = GenerateInts(n)
      .ToList();
    Assert.That(x,
                Is.EqualTo(y));
  }

  [Test]
  [TestCase(-1,
            0)]
  [TestCase(-1,
            1)]
  [TestCase(-1,
            4)]
  [TestCase(-1,
            1000)]
  [TestCase(0,
            0)]
  [TestCase(0,
            1)]
  [TestCase(0,
            4)]
  [TestCase(0,
            100)]
  [TestCase(1,
            0)]
  [TestCase(1,
            1)]
  [TestCase(1,
            4)]
  [TestCase(1,
            10)]
  [TestCase(2,
            0)]
  [TestCase(2,
            1)]
  [TestCase(2,
            4)]
  [TestCase(2,
            20)]
  public async Task ParallelSelectAsyncShouldSucceed(int parallelism,
                                                     int n)
  {
    var x = await GenerateIntsAsync(n)
                  .ParallelSelect(new ParallelTaskOptions(parallelism),
                                  AsyncIdentity(0,
                                                10))
                  .ToListAsync()
                  .ConfigureAwait(false);
    var y = GenerateInts(n)
      .ToList();
    Assert.That(x,
                Is.EqualTo(y));
  }

  [Test]
  [TestCase(-1,
            10000)]
  [TestCase(0,
            100)]
  [TestCase(1,
            10)]
  [TestCase(2,
            20)]
  [TestCase(10,
            100)]
  [TestCase(100,
            1000)]
  public async Task ParallelSelectLimitShouldSucceed(int parallelism,
                                                     int n)
  {
    var counter    = 0;
    var maxCounter = 0;

    // We use a larger wait for infinite parallelism to ensure we can actually spawn thousands of tasks in parallel
    var identity = parallelism < 0
                     ? AsyncIdentity(200,
                                     400)
                     : AsyncIdentity(50,
                                     100);

    async Task<int> F(int x)
    {
      var count = Interlocked.Increment(ref counter);
      InterlockedMax(ref maxCounter,
                     count);

      await identity(x)
        .ConfigureAwait(false);
      Interlocked.Decrement(ref counter);
      return x;
    }

    var x = await GenerateInts(n)
                  .ParallelSelect(new ParallelTaskOptions(parallelism),
                                  F)
                  .ToListAsync()
                  .ConfigureAwait(false);
    var y = GenerateInts(n)
      .ToList();
    Assert.That(x,
                Is.EqualTo(y));

    switch (parallelism)
    {
      case > 0:
        Assert.That(maxCounter,
                    Is.EqualTo(parallelism));
        break;
      case 0:
        Assert.That(maxCounter,
                    Is.EqualTo(Environment.ProcessorCount));
        break;
      case < 0:
        Assert.That(maxCounter,
                    Is.EqualTo(n));
        break;
    }
  }

  [Test]
  [TestCase(-1,
            10000)]
  [TestCase(0,
            100)]
  [TestCase(1,
            10)]
  [TestCase(2,
            20)]
  [TestCase(10,
            100)]
  [TestCase(100,
            1000)]
  public async Task ParallelSelectAsyncLimitShouldSucceed(int parallelism,
                                                          int n)
  {
    var counter    = 0;
    var maxCounter = 0;

    // We use a larger wait for infinite parallelism to ensure we can actually spawn thousands of tasks in parallel
    var identity = parallelism < 0
                     ? AsyncIdentity(200,
                                     400)
                     : AsyncIdentity(50,
                                     100);

    async Task<int> F(int x)
    {
      var count = Interlocked.Increment(ref counter);
      InterlockedMax(ref maxCounter,
                     count);

      await identity(x)
        .ConfigureAwait(false);
      Interlocked.Decrement(ref counter);
      return x;
    }

    var x = await GenerateIntsAsync(n)
                  .ParallelSelect(new ParallelTaskOptions(parallelism),
                                  F)
                  .ToListAsync()
                  .ConfigureAwait(false);
    var y = GenerateInts(n)
      .ToList();

    switch (parallelism)
    {
      case > 0:
        Assert.That(maxCounter,
                    Is.EqualTo(parallelism));
        break;
      case 0:
        Assert.That(maxCounter,
                    Is.EqualTo(Environment.ProcessorCount));
        break;
      case < 0:
        Assert.That(maxCounter,
                    Is.EqualTo(n));
        break;
    }

    Assert.That(x,
                Is.EqualTo(y));
  }

  [Test]
  public async Task UnorderedCompletionShouldSucceed()
  {
    var firstDone = false;
    var x = await GenerateInts(1000)
                  .ParallelSelect(new ParallelTaskOptions(2),
                                  async i =>
                                  {
                                    if (i != 0)
                                    {
                                      return !firstDone;
                                    }

                                    await Task.Delay(100)
                                              .ConfigureAwait(false);
                                    firstDone = true;
                                    return true;
                                  })
                  .ToListAsync()
                  .ConfigureAwait(false);
    Assert.That(x,
                Is.All.True);
  }

  [Test]
  public async Task UnorderedAsyncCompletionShouldSucceed()
  {
    var firstDone = false;
    var x = await GenerateIntsAsync(1000)
                  .ParallelSelect(new ParallelTaskOptions(2),
                                  async i =>
                                  {
                                    if (i != 0)
                                    {
                                      return !firstDone;
                                    }

                                    await Task.Delay(100);
                                    firstDone = true;
                                    return true;
                                  })
                  .ToListAsync()
                  .ConfigureAwait(false);
    Assert.That(x,
                Is.All.True);
  }

  [Test]
  [TestCase(false,
            false)]
  [TestCase(false,
            true)]
  [TestCase(true,
            false)]
  [TestCase(true,
            true)]
  public async Task CancellationShouldSucceed(bool cancellationAware,
                                              bool cancelLast)
  {
    var maxEntered = -1;
    var maxExited  = -1;
    var cancelAt   = 100;
    var cts        = new CancellationTokenSource();
    Assert.ThrowsAsync<TaskCanceledException>(async () =>
                                              {
                                                await foreach (var x in GenerateInts(cancelLast
                                                                                       ? cancelAt + 1
                                                                                       : 100000)
                                                                        .ParallelSelect(new ParallelTaskOptions(-1,
                                                                                                                cts.Token),
                                                                                        async x =>
                                                                                        {
                                                                                          if (x == cancelAt)
                                                                                          {
                                                                                            cts.Cancel();
                                                                                          }

                                                                                          InterlockedMax(ref maxEntered,
                                                                                                         x);

                                                                                          await Task.Delay(100,
                                                                                                           cancellationAware
                                                                                                             ? cts.Token
                                                                                                             : CancellationToken.None)
                                                                                                    .ConfigureAwait(false);

                                                                                          InterlockedMax(ref maxExited,
                                                                                                         x);
                                                                                          return x;
                                                                                        })
                                                                        .WithCancellation(CancellationToken.None))
                                                {
                                                }
                                              });

    await Task.Delay(200,
                     CancellationToken.None)
              .ConfigureAwait(false);

    Assert.That(maxEntered,
                Is.EqualTo(cancelAt));
    Assert.That(maxExited,
                Is.EqualTo(cancellationAware
                             ? -1
                             : cancelAt));
  }

  [Test]
  [TestCase(false,
            false)]
  [TestCase(false,
            true)]
  [TestCase(true,
            false)]
  [TestCase(true,
            true)]
  public async Task CancellationAsyncShouldSucceed(bool cancellationAware,
                                                   bool cancelLast)
  {
    var maxEntered = -1;
    var maxExited  = -1;
    var cancelAt   = 100;
    var cts        = new CancellationTokenSource();
    Assert.ThrowsAsync<TaskCanceledException>(async () =>
                                              {
                                                await foreach (var x in GenerateIntsAsync(cancelLast
                                                                                            ? cancelAt + 1
                                                                                            : 100000,
                                                                                          0,
                                                                                          CancellationToken.None)
                                                                        .ParallelSelect(new ParallelTaskOptions(-1,
                                                                                                                cts.Token),
                                                                                        async x =>
                                                                                        {
                                                                                          if (x == cancelAt)
                                                                                          {
                                                                                            cts.Cancel();
                                                                                          }

                                                                                          InterlockedMax(ref maxEntered,
                                                                                                         x);

                                                                                          await Task.Delay(100,
                                                                                                           cancellationAware
                                                                                                             ? cts.Token
                                                                                                             : CancellationToken.None)
                                                                                                    .ConfigureAwait(false);

                                                                                          InterlockedMax(ref maxExited,
                                                                                                         x);
                                                                                          return x;
                                                                                        })
                                                                        .WithCancellation(CancellationToken.None))
                                                {
                                                }
                                              });

    await Task.Delay(200,
                     CancellationToken.None)
              .ConfigureAwait(false);

    Assert.That(maxEntered,
                Is.EqualTo(cancelAt));
    Assert.That(maxExited,
                Is.EqualTo(cancellationAware
                             ? -1
                             : cancelAt));
  }


  private static IEnumerable<int> GenerateInts(int n)
  {
    for (var i = 0; i < n; ++i)
    {
      yield return i;
    }
  }

  private static async IAsyncEnumerable<int> GenerateIntsAsync(int                                        n,
                                                               int                                        delay             = 0,
                                                               [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    for (var i = 0; i < n; ++i)
    {
      if (delay > 0)
      {
        await Task.Delay(delay,
                         cancellationToken)
                  .ConfigureAwait(false);
      }
      else
      {
        await Task.Yield();
      }

      yield return i;
    }
  }

  private static Func<int, Task<int>> AsyncIdentity(int               delayMin          = 0,
                                                    int               delayMax          = 0,
                                                    CancellationToken cancellationToken = default)
    => async x =>
       {
         if (delayMax <= delayMin)
         {
           delayMax = delayMin + 1;
         }

         var rng = new Random(x);
         var delay = rng.Next(delayMin,
                              delayMax);
         if (delay > 0)
         {
           await Task.Delay(delay,
                            cancellationToken)
                     .ConfigureAwait(false);
         }
         else
         {
           await Task.Yield();
         }

         return x;
       };

  private static void InterlockedMax(ref int location,
                                     int     value)
  {
    // This is a typical instance of the CAS loop pattern:
    // https://learn.microsoft.com/en-us/dotnet/api/system.threading.interlocked.compareexchange#system-threading-interlocked-compareexchange(system-single@-system-single-system-single)

    // Red the current max at location
    var max = location;

    // repeat as long as current max in less than new value
    while (max < value)
    {
      // Tries to store the new value if max has not changed
      max = Interlocked.CompareExchange(ref location,
                                        value,
                                        max);
    }
  }
}
