import xmlFactory
import DataSource
import xmlBulider

dataSource = DataSource.DataSource()

tableName = 'org_partymember'

tableInfo = dataSource.getTableDescInfo(tableName)

bulider = xmlBulider.XMLBulider(tableInfo,tableName)

bulider.execute(bulider.insert)
