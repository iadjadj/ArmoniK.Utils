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

using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using JetBrains.Annotations;

namespace ArmoniK.Utils.LowLevel;

[PublicAPI]
public static class ParallelWaitExt
{
  /// <summary>
  ///   Iterates over the input enumerable and spawn multiple parallel tasks.
  ///   The maximum number of tasks in flight at any given moment is given in the `parallelTaskOptions`.
  ///   All results are collected in-order.
  /// </summary>
  /// <param name="enumerable">Enumerable to iterate on</param>
  /// <param name="parallelTaskOptions">Options (eg: parallelismLimit, cancellationToken)</param>
  /// <typeparam name="T">Return type of the tasks</typeparam>
  /// <returns>Asynchronous results of tasks</returns>
  [PublicAPI]
  public static async IAsyncEnumerable<T> LowLevelParallelWait<T>(this IEnumerable<Task<T>> enumerable,
                                                                  ParallelTaskOptions       parallelTaskOptions = default)
  {
    var parallelism       = parallelTaskOptions.ParallelismLimit;
    var cancellationToken = parallelTaskOptions.CancellationToken;

    // CancellationTokenSource used to cancel all tasks inflight upon errors
    using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

    // Queue of tasks
    var queue = new Queue<Task<T>>();
    // Semaphore to limit the parallelism
    // Semaphore is created with one resource already acquired
    using var sem = new SemaphoreSlim(parallelism - 1,
                                      parallelism);

    // Iterate over the enumerable
    foreach (var x in enumerable)
    {
      // Prepare the new semaphore acquisition
      var semAcquire = sem.WaitAsync(cts.Token);

      // Async closure that waits for the task x and releases the semaphore
      async Task<T> TaskLambda()
      {
        var res = await x.ConfigureAwait(false);
        sem.Release();
        return res;
      }

      // We can enqueue the task
      queue.Enqueue(Task.Run(TaskLambda,
                             cts.Token));

      // Dequeue tasks as long as the semaphore is not acquired yet
      while (true)
      {
        var front = queue.Count != 0
                      ? queue.Peek()
                      : null;
        // Select the first task to be ready
        // There is no need to explicitly wait on cancellation: semAcquire natively supports cancellation
        using var which = await Task.WhenAny(front ?? semAcquire,
                                             semAcquire)
                                    .ConfigureAwait(false);

        // If there is an error or cancellation has been requested, we must throw the exception
        which.ThrowIfError();

        // Semaphore has been acquired so we can enqueue a new task
        if (ReferenceEquals(which,
                            semAcquire))
        {
          break;
        }

        // Front task was ready, so we can get its result and yield it
        Debug.Assert(ReferenceEquals(which,
                                     front));
        yield return await queue.Dequeue()
                                .ConfigureAwait(false);
      }
    }

    // We finished iterating over the inputs and
    // must now wait for all the tasks in the queue
    foreach (var task in queue)
    {
      // Dequeue a new task and wait for its result
      // As the task is wrapped in a Task.run with the cancellation token,
      // It will be cancelled as soon cancellation is triggered
      using (task)
      {
        yield return await task.ConfigureAwait(false);
      }
    }
  }


  /// <summary>
  ///   Iterates over the input enumerable and spawn multiple parallel tasks.
  ///   The maximum number of tasks in flight at any given moment is given in the `parallelTaskOptions`.
  ///   All results are collected in-order.
  /// </summary>
  /// <param name="enumerable">Enumerable to iterate on</param>
  /// <param name="parallelTaskOptions">Options (eg: parallelismLimit, cancellationToken)</param>
  /// <typeparam name="T">Return type of the tasks</typeparam>
  /// <returns>Asynchronous results of tasks</returns>
  [PublicAPI]
  public static async IAsyncEnumerable<T> LowLevelParallelWait<T>(this IAsyncEnumerable<Task<T>> enumerable,
                                                                  ParallelTaskOptions            parallelTaskOptions = default)
  {
    var parallelism       = parallelTaskOptions.ParallelismLimit;
    var cancellationToken = parallelTaskOptions.CancellationToken;

    // CancellationTokenSource used to cancel all tasks inflight upon errors
    using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

    // Queue of tasks
    var queue = new Queue<Task<T>>();
    // Semaphore to limit the parallelism
    // Semaphore is created with one resource already acquired
    using var sem = new SemaphoreSlim(parallelism - 1,
                                      parallelism);

    // Prepare acquire of the semaphore
    var semAcquire = sem.WaitAsync(cts.Token);

    // Manual enumeration allow for overlapping gets and yields
    await using var enumerator = enumerable.GetAsyncEnumerator(cts.Token);

    Task<bool> MoveNext()
      => enumerator.MoveNextAsync()
                   .AsTask();

    // Start first move
    var move = Task.Run(MoveNext,
                        cts.Token);

    // what to do next: either semAcquire or move
    Task next = move;

    while (true)
    {
      var front = queue.Count != 0
                    ? queue.Peek()
                    : null;
      // Select the first task to be ready
      // There is no need to explicitly wait on cancellation:
      //   next can be semAcquire which natively supports cancellation
      //   next can also be the moveNext which is ran in a Task.run with a cancellationToken
      using var which = await Task.WhenAny(front ?? next,
                                           next)
                                  .ConfigureAwait(false);

      // If there is an error or cancellation has been requested, we must throw the exception
      which.ThrowIfError(cts);

      // semaphore has been acquired so we can enqueue a new task
      if (ReferenceEquals(which,
                          semAcquire))
      {
        move = Task.Run(MoveNext,
                        cancellationToken);
        // prepare the new semaphore acquisition
        semAcquire = sem.WaitAsync(cancellationToken);
        // The next thing to do would now be to move to the next iteration
        next = move;
        continue;
      }

      // Front task was ready, so we can get its result and yield it
      if (ReferenceEquals(which,
                          front))
      {
        yield return await queue.Dequeue()
                                .ConfigureAwait(false);
        continue;
      }

      // Move has completed ("next iteration")
      Debug.Assert(ReferenceEquals(which,
                                   move));

      // Iteration has reached an end
      if (!await move.ConfigureAwait(false))
      {
        break;
      }

      // Current task is now ready,
      // so now we can enqueue it as soon as the semaphore is acquired
      var x = enumerator.Current;

      // Async closure that waits for the task x and releases the semaphore
      async Task<T> TaskLambda()
      {
        var res = await x.ConfigureAwait(false);
        sem.Release();
        return res;
      }

      // We can enqueue the task
      queue.Enqueue(Task.Run(TaskLambda,
                             cancellationToken));

      next = semAcquire;
    }

    // We finished iterating over the inputs and
    // must now wait for all the tasks in the queue
    foreach (var task in queue)
    {
      // Dequeue a new task and wait for its result
      // As the task is wrapped in a Task.run with the cancellation token,
      // It will be cancelled as soon cancellation is triggered
      using (task)
      {
        yield return await task.ConfigureAwait(false);
      }
    }
  }
}
