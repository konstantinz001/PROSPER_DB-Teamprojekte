using ClassLibrary.Model;
using Microsoft.Extensions.Configuration;
using Microsoft.VisualBasic;
using Microsoft.VisualBasic.FileIO;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary.Processor
{
    public class SqlExecutorAPI : ISqlExecutorAPI
    {
        /// <summary>
        /// 
        /// Wegen Sicherheitsaspekten werden für API nur Methoden für Lesezugriffe zur Verfügung gestellt
        /// 
        /// </summary>
        /// 

        private readonly ISqlDataAccessAPI _db;

        public SqlExecutorAPI(ISqlDataAccessAPI db)
        {
            _db = db;
        }



        public List<DNB_Dataset> GetDNB_Dataset(int year, string sgt)
        {
            string sql = "select DNB_IDN, CREATOR, TITLE, ERSCHEINUNGSJAHR, ORT, SACHGRUPPE, PUBLIKATIONSART, LINK, CHECKED, GESCHLECHT From dbo.DNB_Dataset_" + year + " " +
                "WHERE (" +
                "SACHGRUPPE LIKE '" + sgt + "' OR SACHGRUPPE LIKE '%|" + sgt + "' or " +
                "SACHGRUPPE LIKE '%|" + sgt + "|%' or " +
                "SACHGRUPPE LIKE '" + sgt + "|%')";

            return _db.LoadData<DNB_Dataset, dynamic>(sql, new { });
        }

        public List<DNB_Dataset> GetDNB_Dataset(int year)
        {
            string sql = "select DNB_IDN, CREATOR, TITLE, ERSCHEINUNGSJAHR, ORT, SACHGRUPPE, PUBLIKATIONSART, LINK, CHECKED, GESCHLECHT From dbo.DNB_Dataset_" + year;
            return _db.LoadData<DNB_Dataset, dynamic>(sql, new { });
        }

        public List<Fakultät> GetFakultäten()
        {
            string sql = "select DISTINCT Fakultät as FakultätenName From dbo.Sachgruppe";

            List<Fakultät> fakultäten = _db.LoadData<Fakultät, dynamic>(sql, new { });

            foreach (var fakultät in fakultäten)
            {
                sql = "select DISTINCT Fachrichtung as FachrichtungsName From dbo.Sachgruppe WHERE Fakultät = '" + fakultät.FakultätenName + "'";
                fakultät.FachrichtungList = _db.LoadData<Fachrichtung, dynamic>(sql, new { });

                foreach (var fachrichtung in fakultät.FachrichtungList)
                {
                    sql = "select DISTINCT Sachgruppe as SachgruppenBezeichnung From dbo.Sachgruppe WHERE Fakultät = '" + fakultät.FakultätenName + "' AND Fachrichtung = '" + fachrichtung.FachrichtungsName + "'";
                    fachrichtung.SachgruppeList = _db.LoadData<Sachgruppe, dynamic>(sql, new { });
                }
            }
            return fakultäten;
        }
    }
}
