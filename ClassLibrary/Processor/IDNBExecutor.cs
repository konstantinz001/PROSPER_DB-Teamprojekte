namespace ClassLibrary.Processor
{
    public interface IDNBExecutor
    {
        int get_numberOfRecords(int year);
        int processingRequest(int numberOfRecords, int position, int year, bool stepByStep, string genderapiKey, string genderizeKey);
    }
}