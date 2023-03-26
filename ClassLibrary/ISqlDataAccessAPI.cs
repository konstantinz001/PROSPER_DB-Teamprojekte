namespace ClassLibrary
{
    public interface ISqlDataAccessAPI
    {
        string ConnectionStringName { get; set; }

        Task<List<T>> LoadDataAsync<T, U>(string sql, U parameters);

        List<T> LoadData<T, U>(string sql, U parameters);
    }
}