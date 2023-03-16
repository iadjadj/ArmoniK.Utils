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

using JetBrains.Annotations;

namespace ArmoniK.Utils;

[PublicAPI]
public static class ParallelSelectExt
{
  /// <summary>
  ///   Iterates over the input enumerable and spawn multiple parallel tasks.
  ///   At most `parallelism` tasks will be running at any given time.
  ///   If `parallelism` is 0 or negative, number of threads is used.
  ///   All results are collected in-order.
  /// </summary>
  /// <param name="enumerable">Enumerable to iterate on</param>
  /// <param name="parallelism">Maximum number of parallel tasks to be spawned at any moment</param>
  /// <param name="cancellationToken">Token to cancel the enumeration</param>
  /// <typeparam name="T">Return type of the tasks</typeparam>
  /// <returns>Asynchronous results of tasks</returns>
  [PublicAPI]
  public static async IAsyncEnumerable<T> ParallelWait<T>(this IEnumerable<Task<T>>                  enumerable,
                                                          int                                        parallelism,
                                                          [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    // If parallelism is 0 or negative, use number of threads
    if (parallelism < 1)
    {
      parallelism = Environment.ProcessorCount;
    }

    // cancellation task for early exit
    var cancelled = cancellationToken.AsTask<T>();
    // Circular buffer of tasks for the queue
    var buffer = new Task<T>[parallelism];
    // number of tasks processed
    var n = 0;

    foreach (var x in enumerable)
    {
      var i = n % parallelism;

      // We should dequeue tasks only if we enqueued enough
      if (n >= parallelism)
      {
        // Dequeue a new task and wait for its result
        // Early exit if cancellation is requested
        yield return await Task.WhenAny(buffer[i],
                                        cancelled)
                               .Unwrap()
                               .ConfigureAwait(false);
      }

      // Enqueue a new task
      buffer[i] =  x;
      n         += 1;

      // not strictly required, but can help stop earlier if the enumerable is slow to move
      cancellationToken.ThrowIfCancellationRequested();
    }

    // Dequeue remaining tasks
    for (var i = Math.Max(0,
                          n - parallelism); i < n; i += 1)
    {
      // Dequeue a new task and wait for its result
      // Early exit if cancellation is requested
      yield return await Task.WhenAny(buffer[i % parallelism],
                                      cancelled)
                             .Unwrap()
                             .ConfigureAwait(false);
    }
  }


  /// <summary>
  ///   Iterates over the input enumerable and spawn multiple parallel tasks.
  ///   At most `parallelism` tasks will be running at any given time.
  ///   If `parallelism` is 0 or negative, number of threads is used.
  ///   All results are collected in-order.
  /// </summary>
  /// <param name="enumerable">Enumerable to iterate on</param>
  /// <param name="parallelism">Maximum number of parallel tasks to be spawned at any moment</param>
  /// <param name="cancellationToken">Token to cancel the enumeration</param>
  /// <typeparam name="T">Return type of the tasks</typeparam>
  /// <returns>Asynchronous results of tasks</returns>
  [PublicAPI]
  public static async IAsyncEnumerable<T> ParallelWait<T>(this IAsyncEnumerable<Task<T>>             enumerable,
                                                          int                                        parallelism,
                                                          [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    // If parallelism is 0 or negative, use number of threads
    if (parallelism < 1)
    {
      parallelism = Environment.ProcessorCount;
    }

    // cancellation tasks for early exit
    var cancelledV = cancellationToken.AsTask<T>();
    var cancelledB = cancellationToken.AsTask<bool>();
    // Circular buffer of tasks for the queue
    var buffer = new Task<T>[parallelism];
    // number of tasks processed
    var n = 0;

    // Manual enumeration allow for overlapping gets and yields
    await using var enumerator = enumerable.GetAsyncEnumerator(cancellationToken);
    // Start first move
    var moveTask = enumerator.MoveNextAsync()
                             .AsTask();

    // Iterate over the enumerator, with early exit on cancellation
    while (await Task.WhenAny(moveTask,
                              cancelledB)
                     .Unwrap()
                     .ConfigureAwait(false))
    {
      var x = enumerator.Current;
      var i = n % parallelism;

      // Start next move
      moveTask = enumerator.MoveNextAsync()
                           .AsTask();

      // We should dequeue tasks only if we enqueued enough
      if (n >= parallelism)
      {
        // Dequeue a new task and wait for its result
        // Early exit if cancellation is requested
        yield return await Task.WhenAny(buffer[i],
                                        cancelledV)
                               .Unwrap()
                               .ConfigureAwait(false);
      }

      // Enqueue a new task
      buffer[i] =  x;
      n         += 1;
    }

    // Dequeue remaining tasks
    for (var i = Math.Max(0,
                          n - parallelism); i < n; i += 1)
    {
      // Dequeue a new task and wait for its result
      // Early exit if cancellation is requested
      yield return await Task.WhenAny(buffer[i % parallelism],
                                      cancelledV)
                             .Unwrap()
                             .ConfigureAwait(false);
    }
  }


  /// <summary>
  ///   Iterates over the input enumerable and spawn multiple parallel tasks.
  ///   At most "number of thread" tasks will be running at any given time.
  ///   All results are collected in-order.
  /// </summary>
  /// <param name="enumerable">Enumerable to iterate on</param>
  /// <param name="cancellationToken">Token to cancel the enumeration</param>
  /// <typeparam name="T">Return type of the tasks</typeparam>
  /// <returns>Asynchronous results of tasks</returns>
  [PublicAPI]
  public static IAsyncEnumerable<T> ParallelWait<T>(this IEnumerable<Task<T>> enumerable,
                                                    CancellationToken         cancellationToken = default)
    => ParallelWait(enumerable,
                    0,
                    cancellationToken);

  /// <summary>
  ///   Iterates over the input enumerable and spawn multiple parallel tasks.
  ///   At most "number of thread" tasks will be running at any given time.
  ///   All results are collected in-order.
  /// </summary>
  /// <param name="enumerable">Enumerable to iterate on</param>
  /// <param name="cancellationToken">Token to cancel the enumeration</param>
  /// <typeparam name="T">Return type of the tasks</typeparam>
  /// <returns>Asynchronous results of tasks</returns>
  [PublicAPI]
  public static IAsyncEnumerable<T> ParallelWait<T>(this IAsyncEnumerable<Task<T>> enumerable,
                                                    CancellationToken              cancellationToken = default)
    => ParallelWait(enumerable,
                    0,
                    cancellationToken);


  /// <summary>
  ///   Iterates over the input enumerable and spawn multiple parallel tasks that call `func`.
  ///   At most `parallelism` tasks will be running at any given time.
  ///   If `parallelism` is 0 or negative, number of threads is used.
  ///   All results are collected in-order.
  /// </summary>
  /// <param name="enumerable">Enumerable to iterate on</param>
  /// <param name="parallelism">Maximum number of parallel tasks to be spawned at any moment</param>
  /// <param name="func">Function to spawn on the enumerable input</param>
  /// <param name="cancellationToken">Token to cancel the enumeration</param>
  /// <typeparam name="TU">Type of the inputs</typeparam>
  /// <typeparam name="TV">Type of the outputs</typeparam>
  /// <returns>Asynchronous results of func over the inputs</returns>
  [PublicAPI]
  public static IAsyncEnumerable<TV> ParallelSelect<TU, TV>(this IEnumerable<TU> enumerable,
                                                            int                  parallelism,
                                                            Func<TU, Task<TV>>   func,
                                                            CancellationToken    cancellationToken = default)
    => ParallelWait(enumerable.Select(func),
                    parallelism,
                    cancellationToken);

  /// <summary>
  ///   Iterates over the input enumerable and spawn multiple parallel tasks that call `func`.
  ///   At most `parallelism` tasks will be running at any given time.
  ///   If `parallelism` is 0 or negative, number of threads is used.
  ///   All results are collected in-order.
  /// </summary>
  /// <param name="enumerable">Enumerable to iterate on</param>
  /// <param name="parallelism">Maximum number of parallel tasks to be spawned at any moment</param>
  /// <param name="func">Function to spawn on the enumerable input</param>
  /// <param name="cancellationToken">Token to cancel the enumeration</param>
  /// <typeparam name="TU">Type of the inputs</typeparam>
  /// <typeparam name="TV">Type of the outputs</typeparam>
  /// <returns>Asynchronous results of func over the inputs</returns>
  [PublicAPI]
  public static IAsyncEnumerable<TV> ParallelSelect<TU, TV>(this IAsyncEnumerable<TU> enumerable,
                                                            int                       parallelism,
                                                            Func<TU, Task<TV>>        func,
                                                            CancellationToken         cancellationToken = default)
    => ParallelWait(enumerable.Select(func),
                    parallelism,
                    cancellationToken);

  /// <summary>
  ///   Iterates over the input enumerable and spawn multiple parallel tasks that call `func`.
  ///   At most "number of thread" tasks will be running at any given time.
  ///   All results are collected in-order.
  /// </summary>
  /// <param name="enumerable">Enumerable to iterate on</param>
  /// <param name="func">Function to spawn on the enumerable input</param>
  /// <param name="cancellationToken">Token to cancel the enumeration</param>
  /// <typeparam name="TU">Type of the inputs</typeparam>
  /// <typeparam name="TV">Type of the outputs</typeparam>
  /// <returns>Asynchronous results of func over the inputs</returns>
  [PublicAPI]
  public static IAsyncEnumerable<TV> ParallelSelect<TU, TV>(this IEnumerable<TU> enumerable,
                                                            Func<TU, Task<TV>>   func,
                                                            CancellationToken    cancellationToken = default)
    => ParallelWait(enumerable.Select(func),
                    cancellationToken);

  /// <summary>
  ///   Iterates over the input enumerable and spawn multiple parallel tasks that call `func`.
  ///   At most "number of thread" tasks will be running at any given time.
  ///   All results are collected in-order.
  /// </summary>
  /// <param name="enumerable">Enumerable to iterate on</param>
  /// <param name="func">Function to spawn on the enumerable input</param>
  /// <param name="cancellationToken">Token to cancel the enumeration</param>
  /// <typeparam name="TU">Type of the inputs</typeparam>
  /// <typeparam name="TV">Type of the outputs</typeparam>
  /// <returns>Asynchronous results of func over the inputs</returns>
  [PublicAPI]
  public static IAsyncEnumerable<TV> ParallelSelect<TU, TV>(this IAsyncEnumerable<TU> enumerable,
                                                            Func<TU, Task<TV>>        func,
                                                            CancellationToken         cancellationToken = default)
    => ParallelWait(enumerable.Select(func),
                    cancellationToken);
}
