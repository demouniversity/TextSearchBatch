//Copyright (c) Microsoft Corporation


namespace Microsoft.Azure.Batch.Samples.TextSearch
{
    using System;
    using System.IO;
    using System.Text.RegularExpressions;
    using System.Threading.Tasks;
    using System.Linq;
    using System.Collections.Generic;

    /// <summary>
    /// The mapper task - it processes a file by performing a regular expression match on each line.
    /// </summary>
    public class MapperTask
    {
        //private readonly Settings configurationSettings;
        private readonly string fileName;

        /// <summary>
        /// Constructs a mapper task object with the specified file name.
        /// </summary>
        /// <param name="blobSas">The file name to process.</param>
        public MapperTask(string fileName)
        {
            this.fileName = fileName;
        }

        /// <summary>
        /// Runs the mapper task.
        /// </summary>
        public async Task RunAsync()
        {

            int lineCount = 0;

            using (FileStream fileStream = new FileStream(this.fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (StreamReader streamReader = new StreamReader(fileStream))
            {

                string line = String.Empty;
                bool CreateIsFound = false;
                List<string> columnNames = new List<string>();
                string previousTableName = String.Empty;
                while ((line = streamReader.ReadLine()) != null)
                {


                    // Found Create TABLE and will process columns. 
                    if (CreateIsFound)
                    {
                        string regex = @"[""`]([^""`]+)[""`]\s+.*[^,\n]+";
                        //string regex = @"[""`]([A-Za-z_]+)[""`]\s+.*[^,\n]+";
                        MatchCollection m = Regex.Matches(line, regex, RegexOptions.IgnoreCase);

                        if (m.Count == 0)//we are done parsing columns
                        {
                            string result = String.Join(",", columnNames.ToArray<string>(), 1, columnNames.Count - 1);

                            Console.WriteLine($"TABLE COLUMN: {columnNames[0]}");//write only the column

                            Console.WriteLine(result);//write the column names
                            columnNames.Clear();
                            CreateIsFound = false;
                        }
                        
                    }
                    if (CreateIsFound || IsCreate(line))
                    {
                        //start parsing the column names for the CREATE Table info. 
                        CreateIsFound = true;
                        
                        // string regex = @"(?!PRIMARY KEY|KEY).*?\(*[""`](?<ext>[A-Za-z_]+)(?!PRIMARY KEY|KEY).*?\(*[""`][,\n]+";
                        string regex = @"([`""](\w+)[`""].*?[,\n])+";
                       
                        foreach (string str in line.Split(','))
                        {

                            if (!str.Contains("KEY"))
                            {
                                
                                string regex2 = @"[`""]([^""`]+)[`""]";
                                MatchCollection m2 = Regex.Matches(str, regex2, RegexOptions.IgnoreCase);
                                string[] array = m2?.Cast<Match>().Select(t => t.Groups[1].Value)?.ToArray<string>();
                                columnNames.AddRange(array);
                                
                            }
                            else
                                break;

                        }

                    }
                    //Process VALUES
                    if (IsInsert(line))
                    {
                        string tableName = GetTableName(line);
                        string[] columns = GetColumns(line);
                        string[] values = GetValues(line);

                        if (columns != null)
                        {

                            if (previousTableName != tableName)
                            {
                                Console.WriteLine($"{tableName}.csv");
                                previousTableName = tableName;
                            }
                            int count = columns.Length;
                            for (int j = 0; j < values.Length; j++)
                            {                              
                                string output = values[j].Replace("NULL", "");
                                Console.WriteLine(output);

                            }

                        }
                    }
                  
                }
            }
        
            #region
            /*
             * Line must start with "INSERT INTO" to return true.
             */
            bool IsInsert(string line)
            {
                return line.TrimStart().ToUpper().StartsWith("INSERT INTO");
            }
            bool IsCreate(string line)
            {
                return line.TrimStart().ToUpper().StartsWith("CREATE TABLE");
            }
            /*
             * Gets all values and returns as one array. 
             * Expects all values to be between parenthesis or single quote in front or after the word. 
             * No backticks
             */
            string[] GetValues(string line)
            {
                //match any word that has parenthesis or single quote in front or after the word. 
                //string regex = @"[(,''](?<ext>\w+)[)'',]";
                //The regex below finds anything between commas, starts with open paren and ends with comma, or starts with
                //comma then ends with closing paren
                //string regex = @"(?<=[,()])\s*(?<ext>([^,()])+)\s*?(?=[,)])";
                //The bottom will remove issues with the following scenario '<space> , If there is a space between
                //the paren/quote and comma. It will be read a new item as a space. This is taken out since there are 
                //String.Empty values, which this strips out. Using the regex above
                //string regex = @"(?<=[,(')])\s*(?![\s]+)(?<ext>([^,'()])+)\s*?(?=[,)'])";
               //// string regex = @"([(])\s*(?<ext>([^()])*)\s*([)])";
                string regex = @"(([(])\s*(?<ext>(.*?))\s*[)]\s*[;,])";
                MatchCollection m = Regex.Matches(line, regex, RegexOptions.IgnoreCase);


                string[] array = m?.Cast<Match>().Select(t => t.Groups["ext"].Value)?.ToArray<string>();
                return array;
            }
            /*
             *  Get the Table Name 
             *  Expects the input to be in the following format
             *  INSERT INTO `test_test_table_info` VALUES (
             */
            string GetTableName(string line)
            {
                //Regex will look for text between INSERT INTO and VALUES, with the back tick
                //surrounding the table name. It extracts the value into the "ext" key. 
                string regex = @"\s*INSERT\s*INTO\s*([""`'']{1}\s*(?<ext>\w+)[""`'']{1}?)\s*.*VALUES.*";
                Match m = Regex.Match(line, regex, RegexOptions.IgnoreCase);
                if (!m.Success)
                {
                    //TODO Error handling
                    Console.Error.WriteLine(@"Error parsing file: Expected file format is INSERT INTO `tablename VALUES (1)");
                    return null;
                }
                return m.Groups["ext"].Value;
            }
            /*
             * Only counting columns at this time. 
             * Need to verify if the columns need to be parsed from the other text. 
             * 
             */
            string[] GetColumns(string line)
            {
        
                string regex2 = @"[(](?<ext>.*?)[)]{1}?";
                Match m2 = Regex.Match(line, regex2, RegexOptions.IgnoreCase);
                if (m2.Success)
                {
                    string values2 = m2.Groups["ext"]?.Value;
                    int? count = values2?.Split(',')?.Length;
                    if (count.HasValue)
                    {
                        int length = count.Value;
                        string[] result = new string[length];
                        for (int i = 0; i < length; i++)
                        {
                            result[i] = $"{i}";
                        }
                        return result;
                    }
                }
                return null;
            }
        }
        #endregion
    }
}
