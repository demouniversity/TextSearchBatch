//Copyright (c) Microsoft Corporation

namespace Microsoft.Azure.Batch.Samples.TextSearch
{
    using System;
    using Common;

    public class Program
    {
        public static void Main(string[] args)
        {
            try
            {
                //number of tasks and file directory
                ReducerTask reducerTask = new ReducerTask(Int32.Parse(args[0]), args[1]);
                reducerTask.Run();
            }
            catch (AggregateException e)
            {
                
                PrintAggregateException(e);

                throw;
            }
        }
        /// <summary>
        /// Processes all the exceptions inside an <see cref="AggregateException"/> and writes each inner exception to the console.
        /// </summary>
        /// <param name="aggregateException">The <see cref="AggregateException"/> to process.</param>
        public static void PrintAggregateException(AggregateException aggregateException)
        {
            // Go through all exceptions and dump useful information
            foreach (Exception exception in aggregateException.InnerExceptions)
            {
                Console.WriteLine(exception.ToString());
                Console.WriteLine();
            }
        }
    }
}
