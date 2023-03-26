using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary.Model
{
    public class DNB_Dataset
    {
        public string? DNB_IDN { get; set; }
        public string? CREATOR { get; set; }    
        public string? TITLE { get; set; }
        public string? ERSCHEINUNGSJAHR { get; set; }
        public string? ORT { get; set; }
        public string? PUBLIKATIONSART { get; set; }
        public string? SACHGRUPPE { get; set; }
        public string? LINK { get; set; }
        public bool CHECKED { get; set; }
        public string GESCHLECHT { get; set; }
    }


    public class DNB_DataSetList
    {
        public string? Bezeichnung { get; set; }
        public List<DNB_Dataset>? DNB_Datasets { get; set; }
    }
}
