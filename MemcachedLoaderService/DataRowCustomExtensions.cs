﻿using System;
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
                if (PKColumnsList != null && PKColumnsList.Count > 1)
                {
                    FormattedKey = string.Format("{0}.key=", QuerySpecs.KeyPrefix);

                    foreach(string ColumnName in PKColumnsList)
                    {
                        FormattedKey += myRow[ColumnName].ToString().ToUpper().Trim().Replace(" ", string.Empty) + "+";
                    }

                    int LastSeparatorPos = FormattedKey.LastIndexOf("+");
                    if (LastSeparatorPos > 0)
                    {
                        FormattedKey = FormattedKey.Remove(LastSeparatorPos);
                    }
                            

                }
                else if (PKColumnsList != null && PKColumnsList.Count == 1)
                {
                    FormattedKey = string.Format("{0}.key={1}", QuerySpecs.KeyPrefix, myRow[PKColumnsList.ElementAt(0)].ToString());
                }
                else
                {
                    FormattedKey = string.Format("{0}.key={1}", QuerySpecs.KeyPrefix, myRow[0].ToString());
                }
            }

            return FormattedKey;
        }
    }
}
