using ClassLibrary.Model;

namespace ClassLibrary.Processor
{
    public interface ISqlExecutor
    {
        void CreateDB(int year);
        List<DNB_Dataset> GetDNB_Dataset(int year);
        List<DNB_Dataset> GetDNB_Dataset(int year, string sgt);
        List<Fachrichtung> getFachrichtungen();
        List<Fakultät> GetFakultäten();
        string? GetGender(string vorname);
        int GetMaxJahr();
        int GetMinJahr();
        Task InsertDNBData(string idn, string creator, string title, string releaseyear, string sachgruppe, string ort, string link, string genderapiKey, string genderizeKey);
        void UpdateFavoriten(string dnb_idx, string year, bool checkedValue);
        void SetUpdateHistory(int year);
        List<UpdateHistory_Model> GetUpdateHistory();
        int GetDatasetCount(int year);


        Task UpdateDNBData(string idn, string creator, string title, string ort, string releaseyear);
    }
}