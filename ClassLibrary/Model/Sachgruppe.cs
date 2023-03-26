using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary.Model
{
    public class Sachgruppe
    {
        public string SachgruppenBezeichnung { get; set; }
    }
    public class Fakultät
    {
        public string FakultätenName { get; set; }  
        public List<Fachrichtung> FachrichtungList { get; set; }
    }
    public class Fachrichtung
    {
        public string FachrichtungsName { get; set; }
        public List<Sachgruppe> SachgruppeList { get; set; }

    }
}

