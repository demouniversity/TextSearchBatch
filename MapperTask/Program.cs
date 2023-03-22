//Copyright (c) Microsoft Corporation

namespace Microsoft.Azure.Batch.Samples.TextSearch
{
    using System;
    using Common;

    public class Program
    {
        public const string MapperTaskExecutable = "MapperTask.exe";
        public static void Main(string[] args)
        {
            if (args != null && args.Length > 0)
            {
                if (args.Length != 1)
                {
                    DisplayUsage();
                    throw new ArgumentException("Incorrect number of arguments");
                }

                string blobSas = args[0];

                try
                {
                    MapperTask mapperTask = new MapperTask(blobSas);
                    mapperTask.RunAsync().Wait();
                }
                catch (AggregateException e)
                {
                    PrintAggregateException(e);

                    throw;
                }
            }
            else
            {
                DisplayUsage();
            }
        }

        /// <summary>
        /// Displays the usage of this executable.
        /// </summary>
        private static void DisplayUsage()
        {
            Console.WriteLine("{0} Usage:", MapperTaskExecutable);
            Console.WriteLine("{0} <blob SAS>       - Runs the mapper task, which downloads a file and performs a search on it", MapperTaskExecutable);
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
