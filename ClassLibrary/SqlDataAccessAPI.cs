using Microsoft.Extensions.Configuration;
using System;
using System.Data;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;


namespace ClassLibrary
{

    /// <summary>
    /// 
    /// Erstellung des DB-Mappers und Bereitstellung von Methoden zum Lesen und Schreiben auf der Datenbank
    /// 
    /// </summary>
    public class SqlDataAccessAPI : ISqlDataAccessAPI
    {
        private readonly IConfiguration _config;

        public string ConnectionStringName { get; set; } = "Default";

        public SqlDataAccessAPI(IConfiguration config)
        {
            _config = config;
        }

        public async Task<List<T>> LoadDataAsync<T, U>(string sql, U parameters)
        {
            string connectionString = _config.GetConnectionString(ConnectionStringName);

            using (IDbConnection connection = new SqlConnection(connectionString))
            {
                var data = await connection.QueryAsync<T>(sql, parameters);

                return data.ToList();
            }
        }

        public List<T> LoadData<T, U>(string sql, U parameters)
        {
            string connectionString = _config.GetConnectionString(ConnectionStringName);

            using (IDbConnection connection = new SqlConnection(connectionString))
            {
                try
                {
                    var data = connection.Query<T>(sql, parameters);

                    return data.ToList();
                }
                catch
                {
                    return new List<T>();
                }
            }
        }
    }
}
