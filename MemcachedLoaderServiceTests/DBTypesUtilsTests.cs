using Microsoft.VisualStudio.TestTools.UnitTesting;
using MemcachedLoaderService;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MemcachedLoaderService.Tests
{
    [TestClass()]
    public class DBTypesUtilsTests
    {
        [TestMethod()]
        public void GetDBTypeTest()
        {
            DBType Test = DBTypesUtils.GetDBType("sqlserver");

            bool IsValid = DBTypesUtils.ValidDBType("postgresql");
        }
    }
}