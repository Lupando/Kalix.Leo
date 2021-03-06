﻿/*
    Copyright (c) 2012, iD Commerce + Logistics
    All rights reserved.

    Redistribution and use in source and binary forms, with or without modification, are permitted
    provided that the following conditions are met:

    Redistributions of source code must retain the above copyright notice, this list of conditions
    and the following disclaimer. Redistributions in binary form must reproduce the above copyright
    notice, this list of conditions and the following disclaimer in the documentation and/or other
    materials provided with the distribution.
 
    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
    IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
    FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
    CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
    CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
    SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
    THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
    OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
    POSSIBILITY OF SUCH DAMAGE.
*/

using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace System.Collections.Generic
{
    public static class AsyncEnumerableEx
    {
        public static IAsyncEnumerable<T> Create<T>(Func<AsyncYielder<T>, Task> func)
        {
            Debug.Assert(func != null);
            return new YieldAsyncEnumerable<T>(func);
        }

        public static IAsyncEnumerable<TimeSpan> CreateTimer(TimeSpan waitFor)
        {
            var total = TimeSpan.Zero;
            return new YieldAsyncEnumerable<TimeSpan>(async y =>
            {
                while(true)
                {
                    y.ThrowIfCancellationRequested();
                    await Task.Delay(waitFor, y.CancellationToken).ConfigureAwait(false);
                    total += waitFor;
                    await y.YieldReturn(total).ConfigureAwait(false);
                }
            });
        }

        private sealed class YieldAsyncEnumerable<T> : IAsyncEnumerable<T>
        {
            readonly Func<AsyncYielder<T>, Task> func;

            public YieldAsyncEnumerable(Func<AsyncYielder<T>, Task> func)
            {
                Debug.Assert(func != null);
                this.func = func;
            }

            public IAsyncEnumerator<T> GetEnumerator()
            {
                return new YieldAsyncEnumerator<T>(func);
            }
        }

        private sealed class YieldAsyncEnumerator<T> : IAsyncEnumerator<T>
        {
            Func<AsyncYielder<T>, Task> _func;
            AsyncYielder<T> _yielder;
            Task _task;

            public YieldAsyncEnumerator(Func<AsyncYielder<T>, Task> func)
            {
                Debug.Assert(func != null);
                _func = func;
            }

            ~YieldAsyncEnumerator()
            {
                DisposeImpl();
            }

            public T Current { get; private set; }

            public async Task<bool> MoveNext(CancellationToken cancellationToken)
            {
                if (_task != null)
                {
                    // Second MoveNext() call. Tell Yielder to let the function continue.
                    _yielder.CancellationToken = cancellationToken;
                    _yielder.Continue();
                }
                else
                {
                    if (_func == null)
                    {
                        throw new AsyncYielderDisposedException();
                    }

                    // First MoveNext() call. Start the task.

                    _yielder = new AsyncYielder<T>();
                    _yielder.CancellationToken = cancellationToken;

                    _task = _func(_yielder);
                    _func = null;
                }

                // Wait for yield or return.

                Task finished = await Task.WhenAny(_task, _yielder.YieldTask).ConfigureAwait(false);

                var y = _yielder;
                if (finished != _task && y != null)
                {
                    // the function returned a result.
                    Current = y.YieldTask.Result;
                    return true;
                }

                // The operation is finished.

                Task t = _task;

                _yielder = null;
                _task = null;

                if (t != null && t.IsFaulted)
                {
                    throw t.Exception;
                }

                return false;
            }

            public void Dispose()
            {
                DisposeImpl();
                GC.SuppressFinalize(this);
            }

            void DisposeImpl()
            {
                var y = _yielder;
                if (y != null)
                {
                    y.Break();
                    _yielder = null;
                }
                
                // Let the task finish on its own
                _task = null;
            }
        }
    }

    public sealed class AsyncYielder<T>
    {
        TaskCompletionSource<T> setTcs = new TaskCompletionSource<T>();
        TaskCompletionSource<int> getTcs;

        internal Task<T> YieldTask { get { return setTcs.Task; } }
        public CancellationToken CancellationToken { get; internal set; }

        public Task YieldReturn(T value)
        {
            var gTcs = getTcs = new TaskCompletionSource<int>();
            Task t = gTcs.Task;
            var sTcs = setTcs;
            if (sTcs != null)
            {
                sTcs.SetResult(value);
            }
            return t;
        }

        public void ThrowIfCancellationRequested()
        {
            var ct = CancellationToken;
            if (ct != null)
            {
                ct.ThrowIfCancellationRequested();
            }
        }

        internal void Continue()
        {
            setTcs = new TaskCompletionSource<T>();
            var gTcs = getTcs;
            if (gTcs != null)
            {
                gTcs.SetResult(0);
            }
        }

        internal void Break()
        {
            var gTcs = getTcs;
            if (gTcs != null)
            {
                gTcs.TrySetCanceled();
            }
            var sTcs = setTcs;
            if (sTcs != null)
            {
                sTcs.TrySetCanceled();
            }
        }
    }

    public sealed class AsyncYielderDisposedException : ObjectDisposedException
    {
        internal AsyncYielderDisposedException()
            : base("AsyncYielder")
        {
        }
    }
}
