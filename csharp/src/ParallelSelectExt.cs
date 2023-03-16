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
  ///   If `parallelism` is 0, number of threads is used.
  ///   If `parallelism` is negative, no limit is used.
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
    // If parallelism is 0, use number of threads
    if (parallelism == 0)
    {
      parallelism = Environment.ProcessorCount;
    }

    // If parallelism is negative, launch as many tasks as possible
    if (parallelism < 0)
    {
      parallelism = int.MaxValue;
    }

    // cancellation task for early exit
    var cancelled = cancellationToken.AsTask<T>();
    // Queue of tasks
    var queue  = new Queue<Task<T>>();
    // Semaphore to limit the parallelism
    using var sem = new SemaphoreSlim(parallelism);

    // Prepare acquire of the semaphore
    var semAcquire = sem.WaitAsync(cancellationToken);

    // Iterate over the enumerable
    foreach (var x in enumerable)
    {
      // Dequeue tasks as long as the semaphore is not acquired yet
      while (true) {
        var front = queue.Count != 0 ? queue.Peek() : null;
        // Select the first task to be ready
        var which = await Task.WhenAny(cancelled, front ?? cancelled,
                                       semAcquire)
                              .ConfigureAwait(false);

        // Cancellation has been requested
        if (which.IsCanceled)
        {
          throw new TaskCanceledException();
        }

        // Semaphore has been acquired so we can enqueue a new task
        if (ReferenceEquals(which,
                            semAcquire))
        {
          break;
        }

        // Front task was ready, so we can get its result and yield it
        yield return await queue.Dequeue().ConfigureAwait(false);
      }

      // Async closure that waits for the task x and releases the semaphore
      async Task<T> TaskLambda()
      {
        var res = await x.ConfigureAwait(false);
        x.Dispose();
        sem.Release();
        return res;
      }

      // We can enqueue the task
      queue.Enqueue(TaskLambda());
      // and prepare the new semaphore acquisition
      semAcquire = sem.WaitAsync(cancellationToken);
    }

    // We finished iterating over the inputs and
    // must now wait for all the tasks in the queue
    foreach (var task in queue)
    {
      // Dequeue a new task and wait for its result
      // Early exit if cancellation is requested
      yield return await Task.WhenAny(task,
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
    // If parallelism is 0, use number of threads
    if (parallelism == 0)
    {
      parallelism = Environment.ProcessorCount;
    }

    // If parallelism is negative, launch as many tasks as possible
    if (parallelism < 0)
    {
      parallelism = int.MaxValue;
    }

    // cancellation tasks for early exit
    var cancelled = cancellationToken.AsTask<T>();
    // Queue of tasks
    var queue = new Queue<Task<T>>();
    // Semaphore to limit the parallelism
    using var sem = new SemaphoreSlim(parallelism);

    // Prepare acquire of the semaphore
    var semAcquire = sem.WaitAsync(cancellationToken);

    // Manual enumeration allow for overlapping gets and yields
    await using var enumerator = enumerable.GetAsyncEnumerator(cancellationToken);
    // Start first move
    var move = enumerator.MoveNextAsync()
                             .AsTask();

    // Current task that is ready: initialized to an invalid task as it will be overwritten before being awaited
    var current = Task.FromException<T>(new InvalidOperationException("Unreachable"));
    // what to do next: either semAcquire or move
    Task next = move;

    while (true)
    {
      var front = queue.Count != 0 ? queue.Peek() : null;
      // Select the first task to be ready
      var which = await Task.WhenAny(cancelled,
                                     front ?? cancelled,
                                     next);

      // Cancellation has been requested
      if (which.IsCanceled)
      {
        throw new TaskCanceledException();
      }

      // Move has completed ("next iteration")
      if (ReferenceEquals(which,
                          move))
      {
        // Iteration has reached an end
        if (!await move.ConfigureAwait(false))
        {
          break;
        }

        // Current task is now ready,
        // so now we can enqueue it as soon as the semaphore is acquired
        current = enumerator.Current;
        move = enumerator.MoveNextAsync()
                             .AsTask();
        next = semAcquire;
        continue;
      }

      // semaphore has been acquired so we can enqueue a new task
      if (ReferenceEquals(which,
                          semAcquire))
      {
        var x = current;
        // Async closure that waits for the task x and releases the semaphore
        async Task<T> TaskLambda()
        {
          var res = await x.ConfigureAwait(false);
          x.Dispose();
          sem.Release();
          return res;
        }
        // We can enqueue the task
        queue.Enqueue(TaskLambda());
        // and prepare the new semaphore acquisition
        semAcquire = sem.WaitAsync(cancellationToken);
        // The next thing to do would now be to move to the next iteration
        next   = move;
        continue;
      }

      // Front task was ready, so we can get its result and yield it
      if (ReferenceEquals(which,
                          front))
      {
        yield return await queue.Dequeue().ConfigureAwait(false);
      }
    }

    // We finished iterating over the inputs and
    // must now wait for all the tasks in the queue
    foreach (var task in queue)
    {
      // Dequeue a new task and wait for its result
      // Early exit if cancellation is requested
      yield return await Task.WhenAny(task,
                                      cancelled)
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
