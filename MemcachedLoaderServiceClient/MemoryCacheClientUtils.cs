using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Text;
using System.Threading.Tasks;

namespace MemcachedLoaderServiceClient
{
    public class MemoryCacheClientUtils
    {
        /// <summary>
        /// Gets a type data table object for a list of dictionaries whose key is a string
        /// </summary>
        /// <typeparam name="T">Kvp Value Generic Type</typeparam>
        /// <param name="list">List/Collection of dictionary objects to transform to a datatable</param>
        /// <returns></returns>
        public static DataTable GetDataTableFromDictionaries<T>(List<Dictionary<string, T>> list)
        {
            DataTable dataTable = new DataTable();

            if (list == null || !list.Any()) return dataTable;

            foreach (var column in list.First().Select(c => new DataColumn(c.Key, typeof(T))))
            {
                dataTable.Columns.Add(column);
            }

            foreach (var row in list.Select(
                r =>
                {
                    var dataRow = dataTable.NewRow();
                    r.ToList().ForEach(c => dataRow.SetField(c.Key, c.Value));
                    return dataRow;
                }))
            {
                dataTable.Rows.Add(row);
            }

            return dataTable;
        }
    }
}
