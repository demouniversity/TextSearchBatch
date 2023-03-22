//Copyright (c) Microsoft Corporation

namespace Microsoft.Azure.Batch.Samples.TextSearch
{
    using System;
    using System.Collections;
    using System.IO;
    using System.Text.RegularExpressions;


    /// <summary>
    /// The reducer task.  This task aggregates the results from mapper tasks and prints the results.
    /// </summary>
    public class ReducerTask
    {

        private readonly string accountName;
        private readonly string jobId;

        private static int TASK_NUMBER { get; set; }
        private static string DIRECTORY { get; set; }
        /// <summary>

        public ReducerTask(int taskNumber, string dir)
        {
            TASK_NUMBER = taskNumber;
            DIRECTORY = dir;
            //Read some important data from preconfigured environment variables on the Batch compute node.
            this.accountName = Environment.GetEnvironmentVariable("AZ_BATCH_ACCOUNT_NAME");
            this.jobId = Environment.GetEnvironmentVariable("AZ_BATCH_JOB_ID");
        }

        /// <summary>
        /// Runs the reducer task.
        /// </summary>
        public void Run()
        {
            Hashtable files = new Hashtable();

            //Gather each Mapper tasks output and write it to standard out.
            for (int i = 0; i < TASK_NUMBER; i++)
            {

                string mapperTaskId = Helpers.GetMapperTaskId(i);
                FileStream newFileStream = null;
                StreamWriter streamWriter = null;
                string tableName = String.Empty;
                using (FileStream fileStream = new FileStream($"{DIRECTORY}/{mapperTaskId}", FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (StreamReader streamReader = new StreamReader(fileStream))
                {

                    int lineCount = 0;
                    string textLine = streamReader.ReadLine();
                    //Read the file content.
                    while (textLine != null)
                    {
                        ++lineCount;


                        //This starts the list of columns. The input file should be formated as 
                        //TABLE COLUMN:<column name>,<column name>
                        //This format is specified by the MapperTask
                        if (IsColumn(textLine))
                        {
                            string regex = @"TABLE COLUMN: ([A-Za-z_]+)";

                            Match m = Regex.Match(textLine, regex, RegexOptions.IgnoreCase);
                            if (m.Success)
                            {
                                string table = m.Groups[1].Value;
                                Console.WriteLine($"Create file for table {table}");
                                using (newFileStream = new FileStream($"{table}.csv", FileMode.OpenOrCreate, FileAccess.Write, FileShare.ReadWrite))
                                using (streamWriter = new StreamWriter(newFileStream))
                                {

                                    if (((textLine = streamReader.ReadLine()) != null) && (!String.IsNullOrEmpty(textLine)))
                                    {
                                        // Seek to the beginning of the file
                                        newFileStream.Seek(0, SeekOrigin.Begin);
                                        streamWriter.WriteLine(textLine);//just read the next line for the column names

                                    }
                                    textLine = streamReader.ReadLine();

                                }

                            }


                        }
                        //Process table VALUES
                        if ((!String.IsNullOrEmpty(textLine)) && (tableName = GetTableName(textLine)) != null)
                        {

                            using (newFileStream = new FileStream($"{tableName}.csv", FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
                            using (streamWriter = new StreamWriter(newFileStream))
                            {

                                while (((textLine = streamReader.ReadLine()) != null)
                                && (!(String.IsNullOrEmpty(textLine) && IsColumn(textLine)) //stop when we encounter columns and go back through loop to process the columns
                                && (tableName = GetTableName(textLine)) == null))
                                {
                                    if (IsColumn(textLine))
                                    {
                                        Console.WriteLine($"Found Column: {textLine}");
                                        break;
                                    }
                                    streamWriter.WriteLine(textLine);
                                }
                            }
                        }

                        //add to list of files that have been already processed and needs to be upload to blob. 
                        if ((!String.IsNullOrEmpty(tableName)) && !files.ContainsKey($"{tableName}.csv"))
                        {
                            files.Add($"{tableName}.csv", null);
                        }



                    }
                }

            }

            string GetTableName(string line)
            {
                //Regex will look for text before ".csv"
                //surrounding the table name. It extracts the value into the "ext" key. 
                string regex = @"^(?<ext>.*)\.csv";
                Match m = Regex.Match(line, regex, RegexOptions.IgnoreCase);
                if (!m.Success)
                {
                    //TODO Error handling
                    // Console.Error.WriteLine(@"Error parsing file: Expected file format is INSERT INTO `tablename VALUES (1)");
                    return null;
                }
                return m.Groups["ext"].Value;
            }

        }

        private bool IsColumn(string textLine)
        {
            return textLine.StartsWith("TABLE COLUMN:");

        }
    }
}
