using System;
using System.Threading.Tasks;

namespace Kalix.Leo.Core
{
    /// <summary>
    /// https://stackoverflow.com/questions/28410046/revisiting-task-configureawaitcontinueoncapturedcontext-false
    /// </summary>
    public static class SafeTask
    {
        /// <summary>
        /// Run an async task in sync context in a moderately safe way, only use in extreme circumstances
        /// </summary>
        public static void SafeWait(Func<Task> func)
        {
            try
            {
                // Forces a thread switch, but not too bad if used sparingly
                Task.Run(() => func()).Wait();
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }

        /// <summary>
        /// Run an async task in sync context in a moderately safe way, only use in extreme circumstances
        /// </summary>
        public static T SafeResult<T>(Func<Task<T>> func)
        {
            try
            {
                // Forces a thread switch, but not too bad if used sparingly
                return Task.Run(() => func()).Result;
            }
            catch (AggregateException e)
            {
                throw e.InnerException;
            }
        }
    }
}
