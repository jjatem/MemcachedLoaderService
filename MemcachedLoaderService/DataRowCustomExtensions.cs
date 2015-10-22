using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MemcachedLoaderService
{
    public static class DataRowCustomExtensions
    {
        public static string GetFormatedMemCachedKey(this DataRow myRow, List<string> PKColumnsList, CachedQuery QuerySpecs)
        {
            string FormattedKey = string.Empty;

            if (myRow != null && myRow.Table.Columns != null && myRow.Table.Columns.Count > 0)
            {
                if (PKColumnsList != null && PKColumnsList.Count > 0)
                {
                    FormattedKey = string.Format("{0}.key={1}", PKColumnsList.Aggregate((bulk, obj) => bulk + myRow[obj].ToString() + "+"));
                }
                else
                {
                    FormattedKey = string.Format("{0}.key={1}", myRow[0].ToString());
                }
            }

            return FormattedKey;
        }
    }
}
