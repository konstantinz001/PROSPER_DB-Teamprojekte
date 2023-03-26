using ClassLibrary.Model;
using Microsoft.VisualBasic;
using Microsoft.VisualBasic.FileIO;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary.Processor
{
    public class SqlExecutor : ISqlExecutor
    {
        private readonly ISqlDataAccess _db;

        private readonly HttpClient httpClient = new HttpClient();

        public SqlExecutor(ISqlDataAccess db)
        {
            _db = db;
        }

        /// <summary>
        /// 
        /// Methoden zum Laden der Fakultäten, Fachrichtungen und Sachgruppen
        /// 
        /// </summary>
        public List<Fakultät> GetFakultäten()
        {
            string sql = "select DISTINCT Fakultät as FakultätenName From dbo.Sachgruppe";

            List<Fakultät> fakultäten = _db.LoadData<Fakultät, dynamic>(sql, new { });

            foreach (var fakultät in fakultäten)
            {
                sql = "select DISTINCT Fachrichtung as FachrichtungsName From dbo.Sachgruppe WHERE Fakultät = @FakultätenName";
                fakultät.FachrichtungList = _db.LoadData<Fachrichtung, dynamic>(sql, new { fakultät.FakultätenName });

                foreach (var fachrichtung in fakultät.FachrichtungList)
                {
                    sql = "select DISTINCT Sachgruppe as SachgruppenBezeichnung From dbo.Sachgruppe WHERE Fakultät = @FakultätenName AND Fachrichtung = @FachrichtungsName";
                    fachrichtung.SachgruppeList = _db.LoadData<Sachgruppe, dynamic>(sql, new { fakultät.FakultätenName, fachrichtung.FachrichtungsName });
                }
            }
            return fakultäten;
        }

        public List<Fachrichtung> getFachrichtungen()
        {
            string sql = "Select DISTINCT Fachrichtung as FachrichtungsName FROM Sachgruppe;";
            var fachrichtungsListe = _db.LoadData<Fachrichtung, dynamic>(sql, new { });

            foreach (var fachrichtung in fachrichtungsListe)
            {
                string sql1 = "Select DISTINCT Sachgruppe as SachgruppenBezeichnung FROM Sachgruppe WHERE Fachrichtung = @FachrichtungsName;";
                fachrichtung.SachgruppeList = _db.LoadData<Sachgruppe, dynamic>(sql1, new { fachrichtung.FachrichtungsName });
            }
            return fachrichtungsListe;
        }
        ///________________________________________________________________________________________________

        
        /// <summary>
        /// 
        /// Methoden zum Laden der DNB_Datensätze aus der Datenbank (Filterung über Überladung)
        /// 
        /// </summary>
        public List<DNB_Dataset> GetDNB_Dataset(int year, string sgt)
        {
            string sql = "select DNB_IDN, CREATOR, TITLE, ERSCHEINUNGSJAHR, ORT, SACHGRUPPE, PUBLIKATIONSART, LINK, CHECKED, GESCHLECHT From dbo.DNB_Dataset_" + year + " " +
                "WHERE (" +
                "SACHGRUPPE LIKE '" + sgt + "' OR SACHGRUPPE LIKE '%|" + sgt + "' or " +
                "SACHGRUPPE LIKE '%|" + sgt + "|%' or " +
                "SACHGRUPPE LIKE '" + sgt + "|%' or SACHGRUPPE LIKE '"+sgt+".%' OR SACHGRUPPE LIKE '%|"+sgt+".%' or SACHGRUPPE LIKE '%|"+sgt+".%|%' or SACHGRUPPE LIKE '"+sgt+".%|%')";
            return _db.LoadData<DNB_Dataset, dynamic>(sql, new { });
        }


        public List<UpdateHistory_Model> GetUpdateHistory()
        {
            string sql = @"SELECT name as TABLE_NAME, ISNULL(CAST(FORMAT(DATUM, 'd', 'de-DE' ) AS varchar(20)), '-') AS DATUM
                FROM Update_History Right JOIN sysobjects ON 
                Update_History.TABLE_NAME = sysobjects.name 
                where name LIKE 'DNB_Dataset_%' 
                AND xtype = 'U' 
                ORDER BY name DESC;";
            return _db.LoadData<UpdateHistory_Model, dynamic>(sql, new { });
        }


        public List<DNB_Dataset> GetDNB_Dataset(int year)
        {
            string sql = "select DNB_IDN, CREATOR, TITLE, ERSCHEINUNGSJAHR, ORT, SACHGRUPPE, PUBLIKATIONSART, LINK, CHECKED, GESCHLECHT From dbo.DNB_Dataset_" + year + ";";
            return _db.LoadData<DNB_Dataset, dynamic>(sql, new { });
        }


        public int GetDatasetCount(int year)
        {
            var db_Name = "DNB_Dataset_" + year;

            string sql = @"Select count(*) FROM " + db_Name;
            return _db.LoadData<int, dynamic>(sql, new { }).FirstOrDefault();
        }

        ///________________________________________________________________________________________________


        /// <summary>
        /// 
        /// Methoden zum bestimmen des Wertebereichs, also für welche Jahre (Min, Max) sind Daten vorhanden
        /// 
        /// </summary>
        public int GetMinJahr()
        {
            string sql = "SELECT trim('DNB_Dataset_' FROM TABLE_NAME) as JAHR FROM information_schema.tables WHERE TABLE_NAME LIKE 'DNB_Dataset_%' ORDER BY JAHR ASC;";
            return _db.LoadData<int, dynamic>(sql, new { }).FirstOrDefault();
        }

        public int GetMaxJahr()
        {
            string sql = "SELECT trim('DNB_Dataset_' FROM TABLE_NAME) as JAHR FROM information_schema.tables WHERE TABLE_NAME LIKE 'DNB_Dataset_%' ORDER BY JAHR DESC;";
            return _db.LoadData<int, dynamic>(sql, new { }).FirstOrDefault();
        }
        ///________________________________________________________________________________________________


        /// <summary>
        /// 
        /// Methode um die Favoriten beim Klick auf die Checkbox im Frontend zu ändern. Dies Triggert automatisch die Datenbank und speichert oder entfent Favoriten
        /// 
        /// </summary>
        public void UpdateFavoriten(string dnb_idx, string year, bool checkedValue)
        {
            string sql = "UPDATE dbo.DNB_Dataset_" + year + " SET CHECKED = " + (checkedValue ? 1 : 0).ToString() + " WHERE DNB_IDN = '" + dnb_idx + "';";
            _db.SaveData<dynamic>(sql, new { });
        }
        ///________________________________________________________________________________________________


        /// <summary>
        /// 
        /// Methoden um die Datenbank mit neuen Daten aus der DNB upzudaten
        /// 
        /// CreateDB erstellt, sofern nicht vorhanden eine neue Table für das jeweilige Jahr
        /// GetGender versucht das Geschlecht des jeweiligen Datensatzes (CREATOR/VORNAME) zu bestimmen
        /// InsertDNBData ruft GetGender auf. Sollte der Name nicht in der Datenbank vorhanden sein, werden die GenderAPIs abgefragt (Evtl. Key notwendig)
        /// Anschließend fügt InsertDNBData den Datensatz in die vorhandene und evtl. neu erstellte Table für das jeweilige Jahr ein
        /// 
        /// </summary>
        public void CreateDB(int year)
        {
            var db_Name = "DNB_Dataset_" + year;
            string sql = "if not exists (select * from sysobjects where name='" + db_Name + "' and xtype='U') " +
            "CREATE TABLE " + db_Name + " ( " +
                "[DNB_IDN][varchar](50) NOT NULL, " +
                "[CREATOR] [nvarchar] (100) NULL, " +
                "[TITLE][nvarchar] (1000) NULL, " +
                "[ERSCHEINUNGSJAHR][varchar] (25) NULL, " +
                "[SACHGRUPPE] [varchar](50) NULL, " +
                "[PUBLIKATIONSART][varchar] (25) NULL, " +
                "[LINK] [varchar](100) NULL, " +
                "[CHECKED][bit] NULL, " +
                "[VORNAME] [nvarchar](50) NULL, " +
                "[GESCHLECHT] [varchar](50) NULL, " +
                "[ORT][nvarchar] (50) NULL, " +
                "PRIMARY KEY(DNB_IDN) " +
            ")";
            _db.SaveData<dynamic>(sql, new { });
        }



        public async Task UpdateDNBData(string idn, string creator, string title, string ort, string releaseyear)
        {
            try
            {
                var db_Name = "DNB_Dataset_" + releaseyear;
                string sql = "UPDATE " + db_Name + " " +
                            "SET CREATOR = @creator " +
                            "WHERE DNB_IDN = @idn;";
                await _db.SaveDataAsync<dynamic>(sql, new { idn, creator });
            }
            catch { };


            try
            {
                var db_Name = "DNB_Dataset_" + releaseyear;
                string sql = "UPDATE " + db_Name + " " +
                            "SET TITLE = @title " +
                            "WHERE DNB_IDN = @idn;";
                await _db.SaveDataAsync<dynamic>(sql, new { idn, title });
            }
            catch { };

            string vorname;
            if (creator.Contains(", "))
            {
                vorname = creator.Split(", ")[1].Split(" ")[0].Trim().Replace("'", "");
            }
            else if (creator.Contains(','))
            {
                vorname = creator.Split(",")[1].Split(" ")[0].Trim().Replace("'", "");
            }
            else if (creator.Contains(" "))
            {
                vorname = creator.Split(" ")[1].Trim().Replace("'", "");
            }
            else
            {
                vorname = creator.Trim();
            }

            try
            {
                var db_Name = "DNB_Dataset_" + releaseyear;
                string sql = "UPDATE " + db_Name + " " +
                            "SET VORNAME = @vorname " +
                            "WHERE DNB_IDN = @idn;";
                await _db.SaveDataAsync<dynamic>(sql, new { idn, vorname });
            }
            catch { };


            try
            {
                var db_Name = "DNB_Dataset_" + releaseyear;
                string sql = "UPDATE " + db_Name + " " +
                            "SET ORT = @ort " +
                            "WHERE DNB_IDN = @idn;";
                await _db.SaveDataAsync<dynamic>(sql, new { idn, ort });
            }
            catch { };
        }


        public async Task InsertDNBData(string idn, string creator, string title, string releaseyear, string sachgruppe, string ort, string link, string genderapiKey, string genderizeKey)
        {
            string vorname;
            if (creator.Contains(", "))
            {
                vorname = creator.Split(", ")[1].Split(" ")[0].Trim().Replace("'", "");
            }
            else if (creator.Contains(','))
            {
                vorname = creator.Split(",")[1].Split(" ")[0].Trim().Replace("'", "");
            }
            else if (creator.Contains(" "))
            {
                vorname = creator.Split(" ")[1].Trim().Replace("'", "");
            }
            else
            {
                vorname = creator.Trim();
            }

            string? gender = GetGender(vorname);

            if (gender == null)
            {
                try
                {
                    string httpResponse = genderizeKey != null ? await httpClient.GetStringAsync(@"https://api.genderize.io/?name=" + 
                        vorname + "&apikey=" + genderizeKey) : await httpClient.GetStringAsync(@"https://api.genderize.io/?name=" + vorname);

                    dynamic json = JsonConvert.DeserializeObject(httpResponse)!;

                    gender = json["gender"];
                }
                catch
                {
                    gender = null;
                }
            }
            if (gender == null)
            {
                try
                {
                    string httpResponse = genderizeKey != null ? await httpClient.GetStringAsync(@"https://gender-api.com/get?name=" +
                        vorname + "&key=" + genderapiKey) : await httpClient.GetStringAsync(@"https://gender-api.com/get?name=" + vorname);

                    dynamic json = JsonConvert.DeserializeObject(httpResponse)!;

                    gender = json["gender"];
                }
                catch
                {
                    gender = null;
                }
            }

            if (gender == null)
                gender = "undefined";

            try
            {
                var db_Name = "DNB_Dataset_" + releaseyear;
                string sql = "INSERT INTO " + db_Name + " " +
                            "(DNB_IDN, CREATOR, TITLE, ERSCHEINUNGSJAHR, SACHGRUPPE, PUBLIKATIONSART, LINK, CHECKED, VORNAME, GESCHLECHT, ORT) " +
                            "values(@idn,@creator,@title,@releaseyear,@sachgruppe, 'Dissertation', @link, 0, @vorname, @gender, @ort)";
                await _db.SaveDataAsync<dynamic>(sql, new { idn, creator, title, releaseyear, sachgruppe, link, gender, vorname, ort });
            }
            catch { };
        }

        public string? GetGender(string vorname)
        {
            string dbPrefix = "DNB_Dataset_";
            int minYear = GetMinJahr();
            int maxYear = GetMaxJahr();

            for (int year = minYear; year <= maxYear; year++)
            {
                string sql = "SELECT GESCHLECHT FROM " + dbPrefix + year + " WHERE VORNAME = '" + vorname + "' ";
                string? genderResult = _db.LoadData<string, dynamic>(sql, new { }).FirstOrDefault();

                if (genderResult != null)
                {
                    return genderResult;
                }
            }
            return null;
        }


        public void SetUpdateHistory(int year)
        {
            try
            {
                var db_Name = "DNB_Dataset_" + year;
                string sql = "INSERT INTO Update_History (TABLE_NAME, Datum) " +
                            "VALUES(@db_Name, GETDATE());";
                _db.SaveData<dynamic>(sql, new { db_Name });
            }
            catch {
                var db_Name = "DNB_Dataset_" + year;
                string sql = "UPDATE Update_History " +
                            "SET Datum = GETDATE() " +
                            "WHERE TABLE_NAME = @db_Name;";
                _db.SaveData<dynamic>(sql, new { db_Name });
            };
        }

        ///________________________________________________________________________________________________
    }
}
