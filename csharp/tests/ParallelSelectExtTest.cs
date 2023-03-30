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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Utils.LowLevel;

using NUnit.Framework;

namespace ArmoniK.Utils.Tests;

public class ParallelSelectExtTest
{
  private static readonly bool[] Booleans =
  {
    false,
    true,
  };

  private static IEnumerable ParallelSelectCases()
  {
    var parallelisms = new int?[]
                       {
                         null,
                         -1,
                         0,
                         1,
                         2,
                       };
    var sizes = new[]
                {
                  0,
                  1,
                  4,
                };

    TestCaseData Case(bool useLowLevel,
                      bool useAsync,
                      int? parallelism,
                      int  size)
      => new TestCaseData(useLowLevel,
                          useAsync,
                          parallelism,
                          size).SetArgDisplayNames($"int[{size}], {parallelism}{(useAsync ? ", async" : "")}{(useLowLevel ? ", low level" : "")}");

    foreach (var useLowLevel in Booleans)
    {
      foreach (var useAsync in Booleans)
      {
        foreach (var parallelism in parallelisms)
        {
          foreach (var size in sizes)
          {
            yield return Case(useLowLevel,
                              useAsync,
                              parallelism,
                              size);
          }
        }

        yield return Case(useLowLevel,
                          useAsync,
                          null,
                          100);
        yield return Case(useLowLevel,
                          useAsync,
                          -1,
                          1000);
        yield return Case(useLowLevel,
                          useAsync,
                          0,
                          100);
        yield return Case(useLowLevel,
                          useAsync,
                          1,
                          10);
        yield return Case(useLowLevel,
                          useAsync,
                          1,
                          20);
      }
    }
  }

  [Test]
  [TestCaseSource(nameof(ParallelSelectCases))]
  public async Task ParallelSelectShouldSucceed(bool useLowLevel,
                                                bool useAsync,
                                                int? parallelism,
                                                int  n)
  {
    var enumerable = GenerateAndSelect(useLowLevel,
                                       useAsync,
                                       parallelism,
                                       null,
                                       n,
                                       AsyncIdentity(0,
                                                     10));
    var x = await enumerable.ToListAsync()
                            .ConfigureAwait(false);
    var y = GenerateInts(n)
      .ToList();
    Assert.That(x,
                Is.EqualTo(y));
  }

  private static IEnumerable ParallelSelectLimitCases()
  {
    TestCaseData Case(bool useLowLevel,
                      bool useAsync,
                      int? parallelism,
                      int  size)
      => new TestCaseData(useLowLevel,
                          useAsync,
                          parallelism,
                          size).SetArgDisplayNames($"int[{size}], {parallelism}{(useAsync ? ", async" : "")}{(useLowLevel ? ", low level" : "")}");

    foreach (var useLowLevel in Booleans)
    {
      foreach (var useAsync in Booleans)
      {
        yield return Case(useLowLevel,
                          useAsync,
                          null,
                          100);
        yield return Case(useLowLevel,
                          useAsync,
                          0,
                          100);
        yield return Case(useLowLevel,
                          useAsync,
                          1,
                          10);
        yield return Case(useLowLevel,
                          useAsync,
                          2,
                          20);
        yield return Case(useLowLevel,
                          useAsync,
                          10,
                          100);
        yield return Case(useLowLevel,
                          useAsync,
                          100,
                          1000);
        yield return Case(useLowLevel,
                          useAsync,
                          -1,
                          10000);
      }
    }
  }

  [Test]
  [TestCaseSource(nameof(ParallelSelectLimitCases))]
  public async Task ParallelSelectLimitShouldSucceed(bool useLowLevel,
                                                     bool useAsync,
                                                     int? parallelism,
                                                     int  n)
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

    var enumerable = GenerateAndSelect(useLowLevel,
                                       useAsync,
                                       parallelism,
                                       null,
                                       n,
                                       F);
    var x = await enumerable.ToListAsync()
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
      case null:
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


  private static IEnumerable UnorderedCompletionCases()
    =>
      from useLowLevel in Booleans
      from useAsync in Booleans
      select new TestCaseData(useLowLevel,
                              useAsync).SetArgDisplayNames($"{(useAsync ? "async " : "")}{(useLowLevel ? "low level" : "")}");

  [Test]
  [TestCaseSource(nameof(UnorderedCompletionCases))]
  public async Task UnorderedCompletionShouldSucceed(bool useLowLevel,
                                                     bool useAsync)
  {
    var firstDone = false;

    async Task<bool> F(int i)
    {
      if (i != 0)
      {
        return !firstDone;
      }

      await Task.Delay(100)
                .ConfigureAwait(false);
      firstDone = true;
      return true;
    }

    var enumerable = GenerateAndSelect(useLowLevel,
                                       useAsync,
                                       2,
                                       null,
                                       1000,
                                       F);
    var x = await enumerable.ToListAsync()
                            .ConfigureAwait(false);
    Assert.That(x,
                Is.All.True);
  }

  private static IEnumerable CancellationCases()
    =>
      from useLowLevel in Booleans
      from useAsync in Booleans
      from cancellationAware in Booleans
      from cancelLast in Booleans
      select new TestCaseData(useLowLevel,
                              useAsync,
                              cancellationAware,
                              cancelLast)
        .SetArgDisplayNames($"{(cancellationAware ? "aware" : "oblivious")}{(cancelLast ? ", last" : "")}{(useAsync ? ", async" : "")}{(useLowLevel ? ", low level" : "")}");

  [Test]
  [TestCaseSource(nameof(CancellationCases))]
  public async Task CancellationShouldSucceed(bool useLowLevel,
                                              bool useAsync,
                                              bool cancellationAware,
                                              bool cancelLast)
  {
    const int cancelAt = 100;

    var maxEntered = -1;
    var maxExited  = -1;
    var cts        = new CancellationTokenSource();

    async Task<int> F(int x)
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
    }

    var enumerable = GenerateAndSelect(useLowLevel,
                                       useAsync,
                                       -1,
                                       cts.Token,
                                       cancelLast
                                         ? cancelAt + 1
                                         : 100000,
                                       F);
    Assert.ThrowsAsync<TaskCanceledException>(async () =>
                                              {
                                                await foreach (var _ in enumerable.WithCancellation(CancellationToken.None))
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
  [TestCaseSource(nameof(CancellationCases))]
  public async Task ThrowingShouldSucceed(bool useLowLevel,
                                          bool useAsync,
                                          bool cancellationAware,
                                          bool throwLast)
  {
    const int throwAt = 100;

    async Task<int> F(int x)
    {
      if (x == throwAt)
      {
        throw new ApplicationException();
      }

      await Task.Delay(100)
                .ConfigureAwait(false);
      return x;
    }

    var enumerable = GenerateAndSelect(useLowLevel,
                                       useAsync,
                                       -1,
                                       null,
                                       throwLast
                                         ? throwAt + 1
                                         : 100000,
                                       F);

    await using var enumerator = enumerable.GetAsyncEnumerator();

    for (var i = 0; i < throwAt; ++i)
    {
      Assert.That(await enumerator.MoveNextAsync()
                                  .ConfigureAwait(false),
                  Is.EqualTo(true));
      Assert.That(enumerator.Current,
                  Is.EqualTo(i));
    }

    Assert.ThrowsAsync<ApplicationException>(async () => await enumerator.MoveNextAsync()
                                                                         .ConfigureAwait(false));
  }

  private static IAsyncEnumerable<T> GenerateAndSelect<T>(bool               useLowLevel,
                                                          bool               useAsync,
                                                          int?               parallelism,
                                                          CancellationToken? cancellationToken,
                                                          int                n,
                                                          Func<int, Task<T>> f)
  {
    // ReSharper disable function ConvertTypeCheckPatternToNullCheck
    ParallelTaskOptions? options = (parallelism, cancellationToken) switch
                                   {
                                     (null, null)                => null,
                                     (int p, null)               => new ParallelTaskOptions(p),
                                     (null, CancellationToken c) => new ParallelTaskOptions(c),
                                     (int p, CancellationToken c) => new ParallelTaskOptions(p,
                                                                                             c),
                                   };

    return (useLowLevel, useAsync, options) switch
           {
             (false, false, null) => GenerateInts(n)
               .ParallelSelect(f),
             (false, false, ParallelTaskOptions opt) => GenerateInts(n)
               .ParallelSelect(opt,
                               f),
             (false, true, null) => GenerateIntsAsync(n)
               .ParallelSelect(f),
             (false, true, ParallelTaskOptions opt) => GenerateIntsAsync(n)
               .ParallelSelect(opt,
                               f),

             (true, false, null) => GenerateInts(n)
                                    .Select(f)
                                    .ParallelWait(),
             (true, false, ParallelTaskOptions opt) => GenerateInts(n)
                                                       .Select(f)
                                                       .ParallelWait(opt),
             (true, true, null) => GenerateIntsAsync(n)
                                   .Select(f)
                                   .ParallelWait(),
             (true, true, ParallelTaskOptions opt) => GenerateIntsAsync(n)
                                                      .Select(f)
                                                      .ParallelWait(opt),
           };
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
