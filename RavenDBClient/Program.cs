using ChoETL;
using log4net.Config;
using Raven.Client.Documents;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RavenDBClient
{
    class Program
    {
        static void Main(string[] args)
        {
            var logger = log4net.LogManager.GetLogger("CrimesChicagoLoader");
            XmlConfigurator.Configure(new System.IO.FileInfo("log4net.config"));

            var store = new DocumentStore
            {
                Urls = new string[] { "http://127.0.01:8080"},
                Database = "CrimesChicago"
            };

            store.Initialize();
            var fileToLoad = @"740_1375_compressed_Chicago_Crimes_2012_to_2017.csv\Chicago_Crimes_2012_to_2017.csv";
            var defaultDate = new DateTime(2012, 1, 1);
            var defaultYear = 2012;
            logger.InfoFormat("Loading in RavenDB {0}", fileToLoad);

            var allRows = File.ReadAllLines(fileToLoad);
            int errors = 0;
            int parsed = 0;

           //// int maxRowToProcess = 200000;
            using (var session = store.BulkInsert())
            {
                foreach (var row in allRows)
                {
                    //** to put a limit in the number of row to precess **/
                    ////if(parsed > maxRowToProcess)
                    ////{
                    ////    break;
                    ////}

                    var rowToSplit = row;
                    if (row.StartsWith("#"))
                    {
                        continue;
                    }                   
                  

                    bool needToReplaceLocation = false;
                    var locationToReplace = string.Empty;
                    
                    
                    rowToSplit = GetValidString(row);
                    var splittedRow = rowToSplit.Split(',');
                    if (needToReplaceLocation)
                    {
                        splittedRow[22] = locationToReplace;
                    }

                    if(splittedRow.Length != 23)
                    {
                        logger.InfoFormat("Le nombre de colonnes n'est pas correct {0}", row);
                        errors++;
                        continue;
                    }

                    try
                    {
                        var crimeRecordRow = new CrimeRecord()
                        {
                            ID = splittedRow[1],
                            CaseNumber = splittedRow[2],                            
                            Date = string.IsNullOrEmpty(splittedRow[3]) ? defaultDate : DateTime.Parse(splittedRow[3], new DateTimeFormatInfo() { LongDatePattern = "dd/mm/yyyy hh:min:sec" }),
                            Block = splittedRow[4],
                            IUCR = splittedRow[5],
                            PrimaryType = splittedRow[6],
                            Description = splittedRow[7],
                            LocationDescription = splittedRow[8],                            
                            Beat = splittedRow[11],
                            District = splittedRow[12],
                            Ward = splittedRow[13],
                            CommunityArea = splittedRow[14],
                            FBICode = splittedRow[15],
                            XCoordinate = string.IsNullOrEmpty(splittedRow[16]) ? -1 : double.Parse(splittedRow[16], NumberFormatInfo.InvariantInfo),
                            YCoordinate = string.IsNullOrEmpty(splittedRow[17]) ? -1 : double.Parse(splittedRow[17], NumberFormatInfo.InvariantInfo),
                            Year = string.IsNullOrEmpty(splittedRow[18]) ? defaultYear : int.Parse(splittedRow[18], NumberFormatInfo.InvariantInfo),
                            UpdatedOn = string.IsNullOrEmpty(splittedRow[19]) ? defaultDate : DateTime.Parse(splittedRow[19], new DateTimeFormatInfo() { LongDatePattern = "dd/mm/yyyy hh:min:sec" }),
                            Latitude = string.IsNullOrEmpty(splittedRow[20]) ? -1 : double.Parse(splittedRow[20], NumberFormatInfo.InvariantInfo),
                            Longitude = string.IsNullOrEmpty(splittedRow[21]) ? -1 : double.Parse(splittedRow[21], NumberFormatInfo.InvariantInfo),
                            Location = splittedRow[22]
                        };
                     
                        bool boolToParse = false;
                        if(bool.TryParse(splittedRow[9], out boolToParse))
                        {
                            crimeRecordRow.Arrest = boolToParse;
                        }

                        if (bool.TryParse(splittedRow[10], out boolToParse))
                        {
                            crimeRecordRow.Domestic = boolToParse;
                        }

                        session.Store(crimeRecordRow);
                        parsed++;
                    }
                    catch (Exception ex)
                    {
                        logger.InfoFormat("Error while parsing row {0} exception {1}", row, ex.Message);
                        errors++;
                    }           

                }

                logger.Info($"Nombres de lignes parsée {parsed} en erreur {errors}");                
            }

            Console.WriteLine("Press any key to continue");
            Console.ReadLine();            
        }

        public static string GetValidString(string oldRow)
        {
            bool needtoReplace = false;
            var validString = new StringBuilder();
            foreach(var element in oldRow)
            {
                var elementToAdd = element;
                if(element == '\"' && needtoReplace == false)
                {
                    needtoReplace = true;
                    continue;
                }
                
                if (element == '\"' && needtoReplace == true)
                {
                    needtoReplace = false;
                    continue;
                }

                if (element == ',' && needtoReplace == true)
                {
                    elementToAdd = '/';
                }


                validString.Append(elementToAdd);
            }

            
            return validString.ToString();
        }
    }
}
