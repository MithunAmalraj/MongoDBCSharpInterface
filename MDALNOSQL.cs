using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Xml;

namespace MDAL
{
    public class MDALNOSQL
    {
        #region Declarations

        public MongoClient mongoClient = new MongoClient();
        private List<IDictionary<string, object>> parent = new List<IDictionary<string, object>>();
        private string VConfigPath = AppDomain.CurrentDomain.BaseDirectory + @"MConfig.xml";
        private string ConnectionString = "";
        private string DatabaseName = "";

        #endregion Declarations

        #region Constructor

        public MDALNOSQL()
        {
            try
            {
                XmlDocument doc = new XmlDocument();
                doc.Load(VConfigPath);
                XmlNodeList xNodeList = doc.SelectNodes("/MCONFIG/Item/VDBMongoConnection");
                XmlNode connection_string = xNodeList[0];
                if (connection_string != null)
                {
                    ConnectionString = connection_string.InnerText;
                    DatabaseName = MongoUrl.Create(ConnectionString).DatabaseName;
                    var settings = MongoClientSettings.FromUrl(MongoUrl.Create(ConnectionString));
                    mongoClient = new MongoClient(settings);
                }
            }
            catch (Exception ex)
            {
            }
        }

        public MDALNOSQL(string connenction)
        {
            try
            {
                XmlDocument doc = new XmlDocument();
                doc.Load(VConfigPath);
                XmlNodeList xNodeList = doc.SelectNodes("/MCONFIG/Item/" + connenction);
                XmlNode connection_string = xNodeList[0];
                if (connection_string != null)
                {
                    ConnectionString = connection_string.InnerText;
                    DatabaseName = MongoUrl.Create(ConnectionString).DatabaseName;
                    var settings = MongoClientSettings.FromUrl(MongoUrl.Create(ConnectionString));
                    mongoClient = new MongoClient(settings);
                }
            }
            catch (Exception ex)
            {
            }
        }

        #endregion Constructor

        #region Methods

        /// <summary>
        /// Get Server Stats
        /// </summary>
        public string GetServerStats()
        {
            try
            {
                var DB = mongoClient.GetDatabase("admin");
                var command = new BsonDocument { { "serverStatus", 1 } };
                var stats = DB.RunCommand<BsonDocument>(command);
                return stats.ToJson();
            }
            catch (Exception ex)
            {
            }
            return "";
        }

        /// <summary>
        /// Get Database Stats
        /// </summary>
        public string GetDatabaseStats()
        {
            try
            {
                var DB = mongoClient.GetDatabase(DatabaseName);
                var command = new BsonDocument { { "dbstats", 1 } };
                var stats = DB.RunCommand<BsonDocument>(command);
                return stats.ToJson();
            }
            catch (Exception ex)
            {
            }
            return "";
        }

        /// <summary>
        /// Select All Databases
        /// </summary>
        public string SelectAllDatabase()
        {
            List<BsonDocument> bsonDocList = new List<BsonDocument>();
            try
            {
                using (var cursor = mongoClient.ListDatabases())
                {
                    while (cursor.MoveNext())
                    {
                        foreach (var doc in cursor.Current)
                        {
                            bsonDocList.Add(doc);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return bsonDocList.ToJson();
        }

        /// <summary>
        /// Returns current DB Name
        /// </summary>
        public string GetCurrentDatabase()
        {
            try
            {
                return DatabaseName;
            }
            catch (Exception ex)
            {
            }
            return "";
        }

        /// <summary>
        /// Create a database
        /// </summary>
        public void CreateDatabase(string DatabaseName, string CollectionName, bool IsCappedCollection, long CappedCollectionSize)
        {
            try
            {
                var db = mongoClient.GetDatabase(DatabaseName);
                if (IsCappedCollection == true)
                {
                    var options = new CreateCollectionOptions { Capped = IsCappedCollection, MaxSize = CappedCollectionSize };
                    db.CreateCollection(CollectionName, options);
                }
                else
                {
                    db.CreateCollection(CollectionName);
                }
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// Drop a Database
        /// </summary>
        public void DropDatabase()
        {
            try
            {
                mongoClient.DropDatabase(DatabaseName);
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// Drop a Database Asynchronously
        /// </summary>
        public async void DropDatabaseAsync()
        {
            try
            {
                await mongoClient.DropDatabaseAsync(DatabaseName);
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// Select All Collection In a Database
        /// </summary>
        public string SelectAllCollection()
        {
            List<BsonDocument> bsonDocList = new List<BsonDocument>();
            try
            {
                var db = mongoClient.GetDatabase(DatabaseName);
                using (var cursor = db.ListCollections())
                {
                    while (cursor.MoveNext())
                    {
                        foreach (var doc in cursor.Current)
                        {
                            bsonDocList.Add(doc);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return bsonDocList.ToJson();
        }

        /// <summary>
        /// Get Collection Stats
        /// </summary>
        public string GetCollectionStats(string CollectionName)
        {
            try
            {
                var db = mongoClient.GetDatabase(DatabaseName);
                var command = new BsonDocumentCommand<BsonDocument>(new BsonDocument { { "collstats", CollectionName } });
                var stats = db.RunCommand(command);
                stats.ToJson();
            }
            catch (Exception ex)
            {
            }
            return "";
        }

        /// <summary>
        /// Create a Collection
        /// </summary>
        public void CreateCollection(string CollectionName, bool IsCappedCollection, long CappedCollectionSize)
        {
            try
            {
                var db = mongoClient.GetDatabase(DatabaseName);
                if (IsCappedCollection == true)
                {
                    var options = new CreateCollectionOptions { Capped = IsCappedCollection, MaxSize = CappedCollectionSize };
                    db.CreateCollection(CollectionName, options);
                }
                else
                {
                    db.CreateCollection(CollectionName);
                }
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// Drop a Collection
        /// </summary>
        public void DropCollection(string CollectionName)
        {
            try
            {
                var DB = mongoClient.GetDatabase(DatabaseName);
                DB.DropCollection(CollectionName);
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// Drop a Collection Asynchronously
        /// </summary>
        public async void DropCollectionAsync(string CollectionName)
        {
            try
            {
                var DB = mongoClient.GetDatabase(DatabaseName);
                await DB.DropCollectionAsync(CollectionName);
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        ///Backup and Drop a Collection
        /// </summary>
        public string BackupandDropCollection(string CollectionName, string BatchFilePath, string LogFilePath, string MongoBackupRARPath, string MongoServerPath, string MongoBackupPath, string WinRARPath)
        {
            Process proc = null;
            try
            {
                var db = mongoClient.GetDatabase(DatabaseName);

                string batFilePath = BatchFilePath;
                string batFileName = "BackupAndRAR_" + CollectionName + ".bat";

                if (File.Exists(batFilePath + batFileName))
                {
                    return "Backup Already In Progress. Please wait.";
                }

                using (FileStream fs = File.Create(batFilePath + batFileName))
                {
                    fs.Close();
                }

                string BackupFileName = "BACKUP_" + DatabaseName + "_" + CollectionName;

                using (StreamWriter sw = new StreamWriter(batFilePath + batFileName))
                {
                    sw.WriteLine("@echo off");
                    sw.WriteLine("set logfilename=backup_archive_log_%date%");
                    sw.WriteLine("set logfilename=%logfilename:/=-%");
                    sw.WriteLine("set logfilename=%logfilename:/=-%");
                    sw.WriteLine("set logfilename=%logfilename: =__%");
                    sw.WriteLine("set logfilename=%logfilename:.=_%");
                    sw.WriteLine("set logfilename=%logfilename::=-%");
                    sw.WriteLine("set currentdatefilename=backup_dump_log_%date%_%time%");
                    sw.WriteLine("set currentdatefilename=%currentdatefilename:/=-%");
                    sw.WriteLine("set currentdatefilename=%currentdatefilename:/=-%");
                    sw.WriteLine("set currentdatefilename=%currentdatefilename: =__%");
                    sw.WriteLine("set currentdatefilename=%currentdatefilename:.=_%");
                    sw.WriteLine("set currentdatefilename=%currentdatefilename::=-%");
                    sw.WriteLine("call :sub >> " + LogFilePath + "%logfilename%.txt");
                    sw.WriteLine("exit /b");
                    sw.WriteLine(":sub");
                    sw.WriteLine("echo ********************************************************************************************************************************");
                    sw.WriteLine("echo COLLECTION NAME : " + CollectionName + "");
                    sw.WriteLine("REM Create a file name for the database output which contains the date and time.Replace any characters which might cause an issue.");
                    sw.WriteLine("set filename=" + BackupFileName + "");
                    sw.WriteLine("REM Check If File Is already backed up");
                    sw.WriteLine("If Exist \"" + MongoBackupRARPath + "%filename%.zip\" (");
                    sw.WriteLine("    echo WINRAR backup file already exists \"" + MongoBackupRARPath + "%filename%.zip\" -  %date% - %time%");
                    sw.WriteLine("    exit 919");
                    sw.WriteLine(")");
                    sw.WriteLine("REM Check If Mongo dump exists in folder");
                    sw.WriteLine("If Not Exist \"" + MongoServerPath + "mongodump.exe\" (");
                    sw.WriteLine("    echo MONGODUMP does not exist \"" + MongoServerPath + "mongodump.exe\" -  %date% - %time%");
                    sw.WriteLine("    exit 919");
                    sw.WriteLine(")");
                    sw.WriteLine("echo MONGODUMP backup started -  %date% - %time%");
                    sw.WriteLine("echo.> " + LogFilePath + "%currentdatefilename%.txt");
                    sw.WriteLine("path=%path%;\"" + MongoServerPath + "\"");
                    sw.WriteLine("mongodump --uri " + ConnectionString + " --collection " + CollectionName + " --out " + MongoBackupPath + "%filename%\\ 2> " + LogFilePath + "%currentdatefilename%.txt");
                    sw.WriteLine("IF %ERRORLEVEL% NEQ 0 (");
                    sw.WriteLine("    echo MONGODUMP backup failed. mongodump --uri " + ConnectionString + " --collection " + CollectionName + " --out " + MongoBackupPath + "%filename%\\ -  %date% - %time%");
                    sw.WriteLine("    exit 919");
                    sw.WriteLine(")");
                    sw.WriteLine("echo MONGODUMP backup complete -  %date% - %time%");
                    sw.WriteLine("REM Check If Mongo dump backup file exists in folder");
                    sw.WriteLine("If Not Exist \"" + MongoBackupPath + "%filename%\" (");
                    sw.WriteLine("    echo MONGODUMP backup does not exist \"" + MongoBackupPath + "%filename%\" -  %date% - %time%");
                    sw.WriteLine("    exit 919");
                    sw.WriteLine(")");
                    sw.WriteLine("REM Check If WINRAR exists in folder");
                    sw.WriteLine("If Not Exist \"" + WinRARPath + "WINRAR.exe\" (");
                    sw.WriteLine("    echo WINRAR does not exist \"" + WinRARPath + "WINRAR.exe\" -  %date% - %time%");
                    sw.WriteLine("    exit 919");
                    sw.WriteLine(")");
                    sw.WriteLine("echo WINRAR started -  %date% - %time%");
                    sw.WriteLine("path=%path%;\"" + WinRARPath + "\"");
                    sw.WriteLine("WinRAR.exe  a -ep1 -ibck \"" + MongoBackupRARPath + "%filename%.zip\" \"" + MongoBackupPath + "" + "%filename%\"");
                    sw.WriteLine("IF %ERRORLEVEL% NEQ 0 (");
                    sw.WriteLine("    echo WINRAR failed %errorlevel%. WinRAR.exe  a -ep1 -ibck \"" + MongoBackupRARPath + "%filename%.zip\" \"" + MongoBackupPath + "" + "%filename%\" -  %date% - %time%");
                    sw.WriteLine("    exit 919");
                    sw.WriteLine(")");
                    sw.WriteLine("echo WINRAR complete -  %date% - %time%");
                    sw.WriteLine("REM Delete the backup directory (leave the ZIP file). The / q tag makes sure we don't get prompted for questions");
                    sw.WriteLine("echo BACKUP deleting original backup directory started %filename% -  %date% - %time%");
                    sw.WriteLine("rmdir \"" + MongoBackupPath + "%filename%\" /s /q");
                    sw.WriteLine("IF %ERRORLEVEL% NEQ 0 (");
                    sw.WriteLine("    echo BACKUP deleting original backup directory failed %errorlevel%. \"" + MongoBackupPath + "%filename%\" -  %date% - %time%");
                    sw.WriteLine("    exit 919");
                    sw.WriteLine(")");
                    sw.WriteLine("echo BACKUP deleting original backup directory complete -  %date% - %time%");
                    sw.WriteLine("echo COMPLETE -  %date% - %time%");
                    sw.WriteLine("exit 100");
                }

                ProcessStartInfo processInfo;
                Process process;
                processInfo = new ProcessStartInfo("cmd.exe", "/c " + batFilePath + batFileName);
                processInfo.CreateNoWindow = true;
                processInfo.UseShellExecute = false;
                // *** Redirect the output ***
                processInfo.RedirectStandardError = true;
                processInfo.RedirectStandardOutput = true;
                process = Process.Start(processInfo);
                process.WaitForExit();

                // *** Read the streams ***
                string output = process.StandardOutput.ReadToEnd();
                string error = process.StandardError.ReadToEnd();
                var exitCode = process.ExitCode;
                if (exitCode != 0 || error != "")
                {
                    error = exitCode > 0 ? output : error;
                }

                if (exitCode == 100)
                {
                    if (File.Exists(batFilePath + batFileName))
                    {
                        File.Delete(batFilePath + batFileName);
                    }

                    db.DropCollection(CollectionName);
                    //var command = new BsonDocument { { "repairDatabase", 1 } };
                    //var stats = db.RunCommand<BsonDocument>(command);
                    return "Backup Complete and Collection Dropped Successfully";
                }
            }
            catch (Exception ex)
            {
            }
            return "";
        }

        /// <summary>
        /// Restore a Collection
        /// </summary>
        public string RestoreCollection(string CollectionName, string BatchFilePath, string LogFilePath, string MongoBackupRARPath, string MongoServerPath, string MongoRestorePath, string WinRARPath)
        {
            try
            {
                Process proc = null;

                string batFileName = "UnzipAndRestore_" + CollectionName + ".bat";
                string batFilePath = BatchFilePath;

                if (File.Exists(batFilePath + batFileName))
                {
                    return "Backup Already In Progress. Please wait.";
                }

                using (FileStream fs = File.Create(batFilePath + batFileName))
                {
                    fs.Close();
                }

                string BackupFileName = "BACKUP_" + DatabaseName + "_" + CollectionName;

                if (!File.Exists(MongoBackupRARPath + BackupFileName + ".zip"))
                {
                    return "Backup File Not Found";
                }

                using (StreamWriter sw = new StreamWriter(batFilePath + batFileName))
                {
                    sw.WriteLine("@echo off");
                    sw.WriteLine("set logfilename=restore_archive_log_%date%");
                    sw.WriteLine("set logfilename=%logfilename:/=-%");
                    sw.WriteLine("set logfilename=%logfilename:/=-%");
                    sw.WriteLine("set logfilename=%logfilename: =__%");
                    sw.WriteLine("set logfilename=%logfilename:.=_%");
                    sw.WriteLine("set logfilename=%logfilename::=-%");
                    sw.WriteLine("call :sub >> " + LogFilePath + "%logfilename%.txt");
                    sw.WriteLine("exit /b");
                    sw.WriteLine(":sub");
                    sw.WriteLine("echo ********************************************************************************************************************************");
                    sw.WriteLine("echo COLLECTION NAME : " + CollectionName + "");
                    sw.WriteLine("set filename=" + BackupFileName + "");
                    sw.WriteLine("If Not Exist \"" + MongoBackupRARPath + "%filename%.zip\" (");
                    sw.WriteLine("    echo WINRAR backup does not exist. \"" + MongoBackupRARPath + "%filename%.zip\" -  %date% - %time%");
                    sw.WriteLine("    exit 919");
                    sw.WriteLine(")");
                    sw.WriteLine("echo WINRAR started -  %date% - %time%");
                    sw.WriteLine("path=%path%;\"" + WinRARPath + "\"");
                    sw.WriteLine("WinRAR.exe -ibck x \"" + MongoBackupRARPath + "%filename%.zip\" *.* \"" + MongoRestorePath + "\"");
                    sw.WriteLine("IF %ERRORLEVEL% NEQ 0 (");
                    sw.WriteLine("    echo WINRAR failed %errorlevel%. WinRAR.exe -ibck x \"" + MongoBackupRARPath + "%filename%.zip\" *.* \"" + MongoRestorePath + "\" -  %date% - %time%");
                    sw.WriteLine("    exit 919");
                    sw.WriteLine(")");
                    sw.WriteLine("echo WINRAR complete -  %date% - %time%");
                    sw.WriteLine("If Not Exist \"" + MongoRestorePath + "%filename%\" (");
                    sw.WriteLine("    echo MONGORESTORE backup does not exist. \"" + MongoRestorePath + "%filename%\" -  %date% - %time%");
                    sw.WriteLine("    exit 919");
                    sw.WriteLine(")");
                    sw.WriteLine("echo MONGORESTORE started -  %date% - %time%");
                    sw.WriteLine("path=%path%;\"" + MongoServerPath + "\"");
                    sw.WriteLine("mongorestore --uri " + ConnectionString + " --db " + DatabaseName + "  --collection " + CollectionName + "  " + MongoRestorePath + "%filename%\\" + DatabaseName + "\\" + CollectionName + ".bson");
                    sw.WriteLine("IF %ERRORLEVEL% NEQ 0 (");
                    sw.WriteLine("    echo MONGORESTORE failed %errorlevel%. mongorestore --uri " + ConnectionString + " --db " + DatabaseName + "  --collection " + CollectionName + "  " + MongoRestorePath + "%filename%\\" + DatabaseName + "\\" + CollectionName + ".bson -  %date% - %time%");
                    sw.WriteLine("    exit 919");
                    sw.WriteLine(")");
                    sw.WriteLine("echo MONGORESTORE complete -  %date% - %time%");
                    sw.WriteLine("echo BACKUP deleting original backup directory started %filename% -  %date% - %time%");
                    sw.WriteLine("rmdir \"" + MongoRestorePath + "%filename%\" /s /q");
                    sw.WriteLine("IF %ERRORLEVEL% NEQ 0 (");
                    sw.WriteLine("    echo BACKUP deleting original backup directory failed %errorlevel%. \"" + MongoRestorePath + "%filename%\"  -  %date% - %time%");
                    sw.WriteLine("    exit 919");
                    sw.WriteLine(")");
                    sw.WriteLine("echo BACKUP deleting original backup directory complete -  %date% - %time%");
                    sw.WriteLine("echo COMPLETE -  %date% - %time%");
                    sw.WriteLine("exit 100");
                }

                ProcessStartInfo processInfo;
                Process process;
                processInfo = new ProcessStartInfo("cmd.exe", "/c " + batFilePath + batFileName);
                processInfo.CreateNoWindow = true;
                processInfo.UseShellExecute = false;
                // *** Redirect the output ***
                processInfo.RedirectStandardError = true;
                processInfo.RedirectStandardOutput = true;
                process = Process.Start(processInfo);
                process.WaitForExit();

                // *** Read the streams ***
                string output = process.StandardOutput.ReadToEnd();
                string error = process.StandardError.ReadToEnd();
                var exitCode = process.ExitCode;
                if (exitCode != 0 || error != "")
                {
                    error = exitCode > 0 ? output : error;
                }

                if (exitCode == 100)
                {
                    if (File.Exists(batFilePath + batFileName))
                    {
                        File.Delete(batFilePath + batFileName);
                    }
                    return "Collection Restored Successfully";
                }
            }
            catch (Exception ex)
            {
            }
            return "";
        }

        /// <summary>
        /// Export a Collection
        /// </summary>
        public void ExportCollection(string CollectionName, string MongoServerPath, string MongoExportPath)
        {
            try
            {
                ProcessStartInfo startInfo = new ProcessStartInfo();
                startInfo.FileName = MongoServerPath + "mongoexport.exe";
                startInfo.Arguments = "-d " + DatabaseName + " -c " + CollectionName + " --type csv --out " + MongoExportPath + "\\output.csv";
                startInfo.UseShellExecute = false;

                Process exportProcess = new Process();
                exportProcess.StartInfo = startInfo;

                exportProcess.Start();
                exportProcess.WaitForExit();
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// Get All Indexes in a Collection
        /// </summary>
        public string GetAllCollectionIndexes(string CollectionName)
        {
            List<BsonDocument> bsonDocList = new List<BsonDocument>();
            try
            {
                var db = mongoClient.GetDatabase(DatabaseName);
                var collection = db.GetCollection<BsonDocument>(CollectionName);
                using (var cursor = collection.Indexes.List())
                {
                    foreach (var document in cursor.ToEnumerable())
                    {
                        bsonDocList.Add(document);
                        //string IndexName = document.GetElement("name").Value.ToString();
                        //var value = document.GetElement("key").Value;
                        //var valueAsDocument = value.AsBsonDocument;
                        //foreach (var elm in valueAsDocument.Elements)
                        //{
                        //    string FieldName = elm.Name;
                        //}
                    }
                }
            }
            catch (Exception ex)
            {
            }
            return bsonDocList.ToJson();
        }

        /// <summary>
        /// Create Index
        /// </summary>
        public void CreateIndex(string CollectionName, string FieldName, string IndexType)
        {
            try
            {
                var db = mongoClient.GetDatabase(DatabaseName);
                var collection = db.GetCollection<BsonDocument>(CollectionName);
                var indexDOC = new BsonDocument { { FieldName, 1 } };
                var indexModel = new CreateIndexModel<BsonDocument>(indexDOC);
                collection.Indexes.CreateOne(indexModel);
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// Drop Index
        /// </summary>
        public void DropIndex(string CollectionName, string IndexName)
        {
            try
            {
                var db = mongoClient.GetDatabase(DatabaseName);
                var collection = db.GetCollection<BsonDocument>(CollectionName);
                collection.Indexes.DropOne(IndexName);
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// Create Index Async
        /// </summary>
        public async void CreateIndexAsync(string CollectionName, string FieldName, string IndexType)
        {
            try
            {
                var db = mongoClient.GetDatabase(DatabaseName);
                var collection = db.GetCollection<BsonDocument>(CollectionName);
                var indexDOC = new BsonDocument { { FieldName, 1 } };
                var indexModel = new CreateIndexModel<BsonDocument>(indexDOC);
                await collection.Indexes.CreateOneAsync(indexModel).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// Drop Index Async
        /// </summary>
        public async void DropIndexAsync(string CollectionName, string IndexName)
        {
            try
            {
                var db = mongoClient.GetDatabase(DatabaseName);
                var collection = db.GetCollection<BsonDocument>(CollectionName);
                await collection.Indexes.DropOneAsync(IndexName);
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// Run a Command
        /// </summary>
        public string RunCommand(string Command)
        {
            try
            {
                var DB = mongoClient.GetDatabase(DatabaseName);
                var infoCommand = new BsonDocument(Command, 1);
                var stats = DB.RunCommand<BsonDocument>(infoCommand);
                return stats.ToJson();
            }
            catch (Exception ex)
            {
            }
            return "";
        }

        /// <summary>
        /// Get field names for a given collection name
        /// </summary>
        public List<string> GetFieldNames(string CollectionName)
        {
            List<string> documentFieldsList = new List<string>();
            try
            {
                var DB = mongoClient.GetDatabase(DatabaseName);
                var collection = DB.GetCollection<BsonDocument>(CollectionName);
                var a = collection.Find(x => true).Limit(1).FirstOrDefault();
                foreach (var elm in a.Elements)
                {
                    string docObj = "";
                    docObj = elm.Name;
                    documentFieldsList.Add(docObj);
                }
            }
            catch (Exception ex)
            {
            }
            return documentFieldsList;
        }

        /// <summary>
        /// Get documents from collection
        /// </summary>
        public DataSet GetDocuments(MongoCommand cmd, string CollectionName)
        {
            DataSet ds = new DataSet();
            try
            {
                var DB = mongoClient.GetDatabase(DatabaseName);
                var collection = DB.GetCollection<BsonDocument>(CollectionName);
                var filter = new BsonDocument();
                var sortFilter = "{";
                if (cmd != null && cmd.Parameters != null && cmd.Parameters.Count > 0)
                {
                    foreach (var item in cmd.Parameters)
                    {
                        if (item.value != null && item.value != "")
                        {
                            if (item.matchExact == true)
                            {
                                var multiplefilterObj = new BsonDocument(item.name, item.value);
                                filter.AddRange(multiplefilterObj);
                            }
                            else if (item.matchExact == false)
                            {
                                var multiplefilterObj = new BsonDocument { { item.name, new BsonDocument { { "$regex", item.value }, { "$options", "i" } } } };
                                filter.AddRange(multiplefilterObj);
                            }
                        }

                        if (item.isSorted == true)
                        {
                            string AscorDesc = "1";
                            if (item.isSortedAscorDesc == false)
                                AscorDesc = "-1";
                            if (sortFilter != "{")
                                sortFilter += "," + item.name + ":" + AscorDesc;
                            else
                                sortFilter += item.name + ":" + AscorDesc;
                        }
                    }
                }
                sortFilter += "}";
                using (var cursor = collection.Find(filter).Sort(sortFilter).ToCursor())
                {
                    while (cursor.MoveNext())
                    {
                        foreach (var doc in cursor.Current)
                        {
                            Dictionary<string, object> dict = doc.ToDictionary();
                            parent.Add(dict);
                        }
                    }
                }
                DataTable dt = ToDataTable(parent);
                ds.Tables.Add(dt);
            }
            catch (Exception ex)
            {
            }
            return ds;
        }

        /// <summary>
        /// Not Working
        /// </summary>
        public DataSet ExecFunction(MongoCommand cmd, string FunctionName)
        {
            DataSet ds = new DataSet();
            try
            {
                var DB = mongoClient.GetDatabase(DatabaseName);
                var bsonCommand = new BsonDocument();
                //if (cmd != null && cmd.Parameters != null && cmd.Parameters.Count > 0)
                //{
                //    foreach (var item in cmd.Parameters)
                //    {
                //        var multiplefilterObj = new BsonDocument(item.name, item.value.ToBson());
                //        bsonCommand.AddRange(multiplefilterObj);
                //    }
                //}
                var collection = DB.GetCollection<SystemJS>("system.js");
                var filter = new BsonDocument("_id", FunctionName);
                var document = collection.Find(filter).Limit(1).FirstOrDefault();
                var command = new BsonDocument("$where", document.value);
                var result = DB.RunCommand<BsonDocument>(command);
                Dictionary<string, object> dict = result.ToDictionary();
                parent.Add(dict);
                DataTable dt = ToDataTable(parent);
                ds.Tables.Add(dt);
            }
            catch (Exception ex)
            {
            }
            return ds;
        }

        /// <summary>
        /// Insert new document
        /// </summary>
        public void InsertDocument(MongoCommand cmd, string CollectionName)
        {
            try
            {
                var DB = mongoClient.GetDatabase(DatabaseName);
                var collection = DB.GetCollection<BsonDocument>(CollectionName);
                var documnt = new BsonDocument();
                if (cmd != null && cmd.Parameters != null && cmd.Parameters.Count > 0)
                {
                    foreach (var item in cmd.Parameters)
                    {
                        var documntObj = new BsonDocument { { item.name, item.value } };
                        documnt.AddRange(documntObj);
                    }
                    collection.InsertOne(documnt);
                }
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// Update existing documents based on condition
        /// </summary>
        public void UpdateDocument(MongoCommand cmd, string CollectionName)
        {
            try
            {
                bool IsAnyFilterConditionGiven = false;
                var DB = mongoClient.GetDatabase(DatabaseName);
                var collection = DB.GetCollection<BsonDocument>(CollectionName);
                var filter = new BsonDocument();
                var updateValues = new BsonDocument();
                if (cmd != null && cmd.Parameters != null && cmd.Parameters.Count > 0)
                {
                    foreach (var item in cmd.Parameters)
                    {
                        if (item.name != null && item.value != null && item.isFilterCondition == true)
                        {
                            IsAnyFilterConditionGiven = true;
                            if (item.matchExact == true)
                            {
                                var multiplefilterObj = new BsonDocument(item.name, item.value);
                                filter.AddRange(multiplefilterObj);
                            }
                            else if (item.matchExact == false)
                            {
                                var multiplefilterObj = new BsonDocument { { item.name, new BsonDocument { { "$regex", item.value }, { "$options", "i" } } } };
                                filter.AddRange(multiplefilterObj);
                            }
                        }
                        else if (item.name != null && item.value != null && item.isFilterCondition == false)
                        {
                            var multipleUpdateObj = new BsonDocument(item.name, item.value);
                            updateValues.AddRange(multipleUpdateObj);
                        }
                    }
                    if (IsAnyFilterConditionGiven == true)
                    {
                        var update = new BsonDocument("$set", updateValues);
                        var result = collection.UpdateMany(filter, update);
                    }
                }
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// Replace one existing document
        /// </summary>
        public void ReplaceOneDocument(MongoCommand cmd, string CollectionName)
        {
            try
            {
                bool IsAnyFilterConditionGiven = false;
                var DB = mongoClient.GetDatabase(DatabaseName);
                var collection = DB.GetCollection<BsonDocument>(CollectionName);
                var filter = new BsonDocument();
                var replacement = new BsonDocument();
                if (cmd != null && cmd.Parameters != null && cmd.Parameters.Count > 0)
                {
                    foreach (var item in cmd.Parameters)
                    {
                        if (item.name != null && item.value != null && item.isFilterCondition == true)
                        {
                            IsAnyFilterConditionGiven = true;
                            if (item.matchExact == true)
                            {
                                var multiplefilterObj = new BsonDocument(item.name, item.value);
                                filter.AddRange(multiplefilterObj);
                            }
                            else if (item.matchExact == false)
                            {
                                var multiplefilterObj = new BsonDocument { { item.name, new BsonDocument { { "$regex", item.value }, { "$options", "i" } } } };
                                filter.AddRange(multiplefilterObj);
                            }
                        }
                        else if (item.name != null && item.value != null && item.isFilterCondition == false)
                        {
                            var replaceObj = new BsonDocument(item.name, item.value);
                            replacement.AddRange(replaceObj);
                        }
                    }
                    if (IsAnyFilterConditionGiven == true)
                    {
                        var result = collection.ReplaceOne(filter, replacement);
                    }
                }
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// Update if document exists or insert document
        /// </summary>
        public void UpsertDocument(MongoCommand cmd, string CollectionName)
        {
            try
            {
                bool IsAnyFilterConditionGiven = false;
                var DB = mongoClient.GetDatabase(DatabaseName);
                var collection = DB.GetCollection<BsonDocument>(CollectionName);
                var filter = new BsonDocument();
                var updateValues = new BsonDocument();
                if (cmd != null && cmd.Parameters != null && cmd.Parameters.Count > 0)
                {
                    foreach (var item in cmd.Parameters)
                    {
                        if (item.name != null && item.value != null && item.isFilterCondition == true)
                        {
                            IsAnyFilterConditionGiven = true;
                            if (item.matchExact == true)
                            {
                                var multiplefilterObj = new BsonDocument(item.name, item.value);
                                filter.AddRange(multiplefilterObj);
                            }
                            else if (item.matchExact == false)
                            {
                                var multiplefilterObj = new BsonDocument { { item.name, new BsonDocument { { "$regex", item.value }, { "$options", "i" } } } };
                                filter.AddRange(multiplefilterObj);
                            }
                        }
                        else if (item.name != null && item.value != null && item.isFilterCondition == false)
                        {
                            var multipleUpdateObj = new BsonDocument(item.name, item.value);
                            updateValues.AddRange(multipleUpdateObj);
                        }
                    }
                    if (IsAnyFilterConditionGiven == true)
                    {
                        var update = new BsonDocument("$set", updateValues);
                        var options = new UpdateOptions { IsUpsert = true };
                        var result = collection.UpdateMany(filter, update, options);
                    }
                }
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// Delete existing documents based on condition
        /// </summary>
        public void DeleteDocument(MongoCommand cmd, string CollectionName)
        {
            try
            {
                bool IsAnyFilterConditionGiven = false;
                var DB = mongoClient.GetDatabase(DatabaseName);
                var collection = DB.GetCollection<BsonDocument>(CollectionName);
                var filter = new BsonDocument();
                if (cmd != null && cmd.Parameters != null && cmd.Parameters.Count > 0)
                {
                    foreach (var item in cmd.Parameters)
                    {
                        if (item.name != null && item.value != null && item.isFilterCondition == true)
                        {
                            IsAnyFilterConditionGiven = true;
                            if (item.matchExact == true)
                            {
                                var multiplefilterObj = new BsonDocument(item.name, item.value);
                                filter.AddRange(multiplefilterObj);
                            }
                            else if (item.matchExact == false)
                            {
                                var multiplefilterObj = new BsonDocument { { item.name, new BsonDocument { { "$regex", item.value }, { "$options", "i" } } } };
                                filter.AddRange(multiplefilterObj);
                            }
                        }
                    }
                    if (IsAnyFilterConditionGiven == true)
                    {
                        var result = collection.DeleteMany(filter);
                    }
                }
            }
            catch (Exception ex)
            {
            }
        }

        #endregion Methods

        #region CommonMethods

        public DataTable ToDataTable(List<IDictionary<string, object>> list)
        {
            DataTable result = new DataTable();
            try
            {
                if (list.Count == 0)
                    return result;

                var columnNames = list.SelectMany(dict => dict.Keys).Distinct();
                result.Columns.AddRange(columnNames.Select(c => new DataColumn(c)).ToArray());
                foreach (Dictionary<string, object> item in list)
                {
                    var row = result.NewRow();
                    foreach (var key in item.Keys)
                    {
                        row[key] = item[key];
                    }
                    result.Rows.Add(row);
                }
            }
            catch (Exception ex)
            {
            }
            return result;
        }

        #endregion CommonMethods
    }

    #region CommonClasses

    public class MongoCommand
    {
        public List<MongoParameter> Parameters { get; set; }

        public MongoCommand()
        {
            Parameters = new List<MongoParameter>();
        }
    }

    public class MongoParameter
    {
        public string name { get; set; }
        public dynamic value { get; set; }
        public bool matchExact { get; set; }
        public bool isFilterCondition { get; set; }
        public bool isSorted { get; set; }
        public bool isSortedAscorDesc { get; set; } //true -- Asc, false -- desc

        public MongoParameter(string parameterName, object parameterValue)
        {
            name = parameterName;
            value = parameterValue;
            matchExact = false;
            isFilterCondition = false;
            isSorted = false;
            isSortedAscorDesc = false;
        }

        public MongoParameter(string parameterName, dynamic parameterValue, bool parameterMatchExactValue = true)
        {
            name = parameterName;
            value = parameterValue;
            matchExact = parameterMatchExactValue;
            isFilterCondition = false;
            isSorted = false;
            isSortedAscorDesc = false;
        }

        public MongoParameter(string parameterName, bool parameterIsSorted = false, bool parameterIsSortedAscorDesc = true)
        {
            name = parameterName;
            value = "";
            matchExact = false;
            isFilterCondition = false;
            isSorted = parameterIsSorted;
            isSortedAscorDesc = parameterIsSortedAscorDesc;
        }

        public MongoParameter(string parameterName, dynamic parameterValue, bool parameterMatchExactValue = true, bool parameterIsFilterCondition = false)
        {
            name = parameterName;
            value = parameterValue;
            matchExact = parameterMatchExactValue;
            isFilterCondition = parameterIsFilterCondition;
            isSorted = false;
            isSortedAscorDesc = false;
        }

        public MongoParameter(string parameterName, dynamic parameterValue, bool parameterMatchExactValue = true, bool parameterIsSorted = false, bool parameterIsSortedAscorDesc = true)
        {
            name = parameterName;
            value = parameterValue;
            matchExact = parameterMatchExactValue;
            isFilterCondition = false;
            isSorted = parameterIsSorted;
            isSortedAscorDesc = parameterIsSortedAscorDesc;
        }
    }

    #endregion CommonClasses

    public class SystemJS
    {
        public string _id { get; set; }
        public BsonJavaScript value { get; set; }
    }
}