using ClassLibrary.Model;

namespace ClassLibrary.Processor
{
    public interface ISqlExecutorAPI
    {
        List<DNB_Dataset> GetDNB_Dataset(int year);
        List<DNB_Dataset> GetDNB_Dataset(int year, string sgt);
        List<Fakultät> GetFakultäten();
    }
}