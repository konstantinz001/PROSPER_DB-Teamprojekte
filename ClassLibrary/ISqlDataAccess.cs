namespace ClassLibrary
{
    public interface ISqlDataAccess
    {
        string ConnectionStringName { get; set; }

        Task<List<T>> LoadDataAsync<T, U>(string sql, U parameters);
        Task SaveDataAsync<T>(string sql, T parameters);

        List<T> LoadData<T, U>(string sql, U parameters);
        void SaveData<T>(string sql, T parameters);
    }
}