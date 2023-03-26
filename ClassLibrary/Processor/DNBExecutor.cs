using ClassLibrary.Model;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using static ClassLibrary.Processor.SqlExecutor;
using static ClassLibrary.SqlDataAccess;


namespace ClassLibrary.Processor
{
    public class DNBExecutor : IDNBExecutor
    {
        private string pub = "Dissertation";

        private readonly ISqlExecutor _db;

        public DNBExecutor(ISqlExecutor db)
        {
            _db = db;
        }



        /// <summary>
        /// 
        /// Methode zum Ausführen der Aktualisierung. Beinhaltet Abfragen der DNB-SRU-Schnittstelle, Extrahieren der Daten aus XML, Weiterreichen an SqlExecutor, welcher Daten in Datenbank einfügt
        /// 
        /// </summary>
        public int processingRequest(int numberOfRecords, int position, int year, bool stepByStep, string genderapiKey, string genderizeKey)
        {
            try
            {
                XDocument response;

                if (!stepByStep)
                {
                    response = ((numberOfRecords - position) >= 100) ? requestDNB(position, year, pub, 100) : requestDNB(position, year, pub, numberOfRecords - position);
                }
                else
                {
                    response = requestDNB(position, year, pub, 1);
                }

                var records = response.Descendants(XNamespace.Get("http://www.loc.gov/MARC21/slim") + "record").ToList();


                foreach (var record in records)
                {
                    try
                    {
                        _db.InsertDNBData(getIDN_FromRecords(record), getCreator_FromRecords(record).Replace("'", ""), getTitle_FromRecords(record).Replace("'", ""), year.ToString(), getSachgruppe_FromRecords(record), getOrt_FromRecords(record).Replace("'", ""), getLink_FromRecords(record), genderapiKey, genderizeKey);
                    }
                    catch
                    {
                        Console.Write("Primary Key Error");
                    }
                }
                if (!stepByStep)
                {
                    return position += 100;
                }
                else
                {
                    return position += 1;
                }
            }
            catch
            {
                processingRequest(numberOfRecords, position++, year, true, genderapiKey, genderizeKey);
                return position;
            }
        }
        ///________________________________________________________________________________________________


        /// <summary>
        /// 
        /// Methoden zum bestimmen der Gesamtergebnisse
        /// 
        /// </summary>
        public int get_numberOfRecords(int year)
        {
            try
            {
                while (true)
                {
                    string query;
                    string url;

                    string yearFilter = "jhr=(" + year + ")";
                    string catalogFilter = "catalog=dnb.hss";
                    query = pub == "Dissertation" ? catalogFilter + " and " + yearFilter + " and hss=diss*" : catalogFilter + " and " + yearFilter + " and hss=habil*";
                    url = @"https://services.dnb.de/sru/dnb?version=1.1&operation=searchRetrieve&query=" + query + "&startRecord=" + 0 + "&recordSchema=MARC21-xml&maximumRecords=" + 1;
                    var request = WebRequest.Create(url);
                    request.Method = "GET";
                    try
                    {
                        var webResponse = request.GetResponse();
                        var webStream = webResponse.GetResponseStream();
                        var reader = new StreamReader(webStream);
                        var data = reader.ReadToEnd();

                        var response = XDocument.Load(url);
                        return int.Parse(response.Descendants(XNamespace.Get("http://www.loc.gov/zing/srw/") + "numberOfRecords").ToList().FirstOrDefault()!.Value);
                    }
                    catch
                    {
                        Console.Write("ERROR: TimeOut");
                    }
                }
            }
            catch
            {
                return 0;
            }
        }
        ///________________________________________________________________________________________________


        /// <summary>
        /// 
        /// Methoden zum Ausführen der Anfrage an die DNB
        /// 
        /// </summary>
        private XDocument requestDNB(int position, int year, string publikationsart, int maxRecords)
        {
            while (true)
            {
                string query;
                string url;

                string yearFilter = "jhr=(" + year + ")";
                string catalogFilter = "catalog=dnb.hss";
                query = publikationsart == "Dissertation" ? catalogFilter + " and " + yearFilter + " and hss=diss*" : catalogFilter + " and " + yearFilter + " and hss=habil*";



                url = @"https://services.dnb.de/sru/dnb?version=1.1&operation=searchRetrieve&query=" + query + "&startRecord=" + position + "&recordSchema=MARC21-xml&maximumRecords=" + maxRecords;

                var request = WebRequest.Create(url);
                request.Method = "GET";

                try
                {
                    var webResponse = request.GetResponse();
                    var webStream = webResponse.GetResponseStream();
                    var reader = new StreamReader(webStream);
                    var data = reader.ReadToEnd();

                    return XDocument.Load(url);
                }
                catch
                {
                    Console.Write("ERROR: TimeOut");
                }
            }
        }
        ///________________________________________________________________________________________________


        /// <summary>
        /// 
        /// Methoden zum Filtern/Extrahieren der jeweiligen Daten aus dem von dem DNB-SRU-Schnittstelle zurückgelieferten XML-File
        /// 
        /// </summary>
        private string getIDN_FromRecords(XElement record)
        {
            try
            {
                return record.Descendants(XNamespace.Get("http://www.loc.gov/MARC21/slim") + "controlfield").First(child => child.Attribute("tag").Value == "001").Value;
            }
            catch
            {
                return "";
            }
        }

        private string getOrt_FromRecords(XElement record)
        {
            List<string> sgtList = new List<string>();
            string ort = "";
            try
            {
                ort = record.Descendants(XNamespace.Get("http://www.loc.gov/MARC21/slim") + "datafield").ToList().FindAll(child => child.Attribute("tag").Value == "264").
                    Elements().First(child => child.Attribute("code").Value == "a").Value.Split(',')[0];
            }
            catch
            {
                ort = "";
            }

            return ort;
        }

        private string getSachgruppe_FromRecords(XElement record)
        {
            List<string> sgtList = new List<string>();
            string sgt1;
            string sgt2;
            string sgt3;
            try
            {
                sgt1 = record.Descendants(XNamespace.Get("http://www.loc.gov/MARC21/slim") + "datafield").ToList().FindAll(child => child.Attribute("tag").Value == "082").
                    Elements().First(child => child.Attribute("code").Value == "a").Value.Split('.')[0];
            }
            catch
            {
                sgt1 = "";
            }
            try
            {
                sgt2 = record.Descendants(XNamespace.Get("http://www.loc.gov/MARC21/slim") + "datafield").ToList().FindAll(child => child.Attribute("tag").Value == "084").
                    Elements().First(child => child.Attribute("code").Value == "a").Value.Split('.')[0];
            }
            catch
            {
                sgt2 = "";
            }
            try
            {
                sgt3 = record.Descendants(XNamespace.Get("http://www.loc.gov/MARC21/slim") + "datafield").ToList().FindAll(child => child.Attribute("tag").Value == "083").
                    Elements().First(child => child.Attribute("code").Value == "a").Value.Split('.')[0];
            }
            catch
            {
                sgt3 = "";
            }

            if (!string.IsNullOrEmpty(sgt1) && !string.IsNullOrEmpty(sgt2) && !string.IsNullOrEmpty(sgt3))
                return sgt1 + "|" + sgt2 + "|" + sgt3;
            else if (!string.IsNullOrEmpty(sgt1) && !string.IsNullOrEmpty(sgt2))
                return sgt1 + "|" + sgt2;
            else if (!string.IsNullOrEmpty(sgt1) && !string.IsNullOrEmpty(sgt3))
                return sgt1 + "|" + sgt3;
            else if (!string.IsNullOrEmpty(sgt2) && !string.IsNullOrEmpty(sgt3))
                return sgt2 + "|" + sgt3;
            else if (!string.IsNullOrEmpty(sgt1))
                return sgt1;
            else if (!string.IsNullOrEmpty(sgt2))
                return sgt2;
            else if (!string.IsNullOrEmpty(sgt3))
                return sgt3;
            else
                return "";
        }

        private string getCreator_FromRecords(XElement record)
        {
            try
            {
                return record.Descendants(XNamespace.Get("http://www.loc.gov/MARC21/slim") + "datafield").ToList().FindAll(child => child.Attribute("tag").Value == "100").
                    Elements().First(child => child.Attribute("code").Value == "a").Value.Replace("'","");
            }
            catch
            {
                try
                {
                    return record.Descendants(XNamespace.Get("http://www.loc.gov/MARC21/slim") + "datafield").ToList().FindAll(child => child.Attribute("tag").Value == "110").
                        Elements().First(child => child.Attribute("code").Value == "a").Value.Replace("'", "");
                }
                catch
                {
                    try
                    {
                        return record.Descendants(XNamespace.Get("http://www.loc.gov/MARC21/slim") + "datafield").ToList().FindAll(child => child.Attribute("tag").Value == "700").
                            Elements().First(child => child.Attribute("code").Value == "a").Value.Replace("'", "");
                    }
                    catch
                    {
                        return "";
                    }
                }
            }
        }

        private string getTitle_FromRecords(XElement record)
        {
            string title1;
            string title2;
            try
            {
                title1 = record.Descendants(XNamespace.Get("http://www.loc.gov/MARC21/slim") + "datafield").ToList().FindAll(child => child.Attribute("tag").Value == "245").
                    Elements().First(child => child.Attribute("code").Value == "a").Value.Split('.')[0];
            }
            catch
            {
                title1 = "";
            }
            try
            {
                title2 = record.Descendants(XNamespace.Get("http://www.loc.gov/MARC21/slim") + "datafield").ToList().FindAll(child => child.Attribute("tag").Value == "245").
                    Elements().First(child => child.Attribute("code").Value == "b").Value.Split('.')[0];
            }
            catch
            {
                title2 = "";
            }

            if (!string.IsNullOrEmpty(title1) && !string.IsNullOrEmpty(title2))
                return title1 + ": " + title2;
            else if (!string.IsNullOrEmpty(title1))
                return title1;
            else
                return title2;
        }


        private string getLink_FromRecords(XElement record)
        {
            string link;
            try
            {
                link = record.Descendants(XNamespace.Get("http://www.loc.gov/MARC21/slim") + "datafield").ToList().FindAll(child => child.Attribute("tag").Value == "856").
                    Elements().First(child => child.Attribute("code").Value == "u").Value;
            }
            catch
            {
                link = "";
            }

            return link;
        }

        ///________________________________________________________________________________________________
    }

}

