// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2022-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//   D. Brasseur       <dbrasseur@aneo.fr>
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


using JetBrains.Annotations;

namespace ArmoniK.Utils;

/// <summary>
///   Wraps an action that will be called when the object is disposed
/// </summary>
public readonly struct Defer : IDisposable
{
  private readonly Action deferred_;

  /// <summary>
  ///   Constructs a Disposable object that calls a specific action when disposed
  /// </summary>
  /// <param name="deferred">Action to be called at Dispose</param>
  [PublicAPI]
  public Defer(Action deferred)
    => deferred_ = deferred;

  /// <inheritdoc />
  public void Dispose()
    => deferred_();

  /// <summary>
  ///   Constructs a Disposable object that calls a specific action when disposed
  /// </summary>
  /// <param name="deferred">Action to be called at Dispose time</param>
  /// <returns>Disposable object that calls deferred action when disposed</returns>
  [PublicAPI]
  public static IDisposable Create(Action deferred)
    => new Defer(deferred);

  /// <summary>
  ///   A Disposable object that does nothing
  /// </summary>
  [PublicAPI]
  public static readonly IDisposable Empty = Create(() =>
                                                    {
                                                    });
}
