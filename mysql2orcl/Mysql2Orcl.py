import pymysql as mysql

import traceback
import time
import datetime
import re


class Mysql2Orcl:

    def __init__(self):
        self.host = 'host'
        self.port = 3306
        self.user = 'user'
        self.password = 'passwd'
        self.dataBase = 'dsfa0611'
        self.db = self.__getConnection()
        self.dataSet = self.__initDataSet()

    def __initDataSet(self):
        dataSet = {}
        dataSet['varchar'] = 'NVARCHAR2'
        dataSet['datetime'] = 'TIMESTAMP'
        dataSet['decimal'] = 'NUMBER'
        dataSet['text'] = 'CLOB'
        dataSet['int'] = 'NUMBER'
        return dataSet

    def __getConnection(self):
        db = mysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.dataBase,
            charset="utf8"
        )
        return db

    def getCharSetType(self, info):
        typeSize = info[2]
        typeInfo = info[1]
        dataSet = self.dataSet[info[1]]
        if typeSize is None and typeInfo == 'int':
            return dataSet + '(10)'

        if dataSet == 'CLOB':
            return dataSet

        return typeInfo + '(' + str(typeSize) + ')'

    def queryTables(self):
        sql = '''
              SELECT
                  TABLE_NAME
              FROM
                  information_schema.tables
              WHERE
                  TABLE_SCHEMA = '{dbName}'
                  AND TABLE_NAME like concat('dsfa_','%')
              '''
        return self.queryData(sql.format(dbName=self.dataBase))

    def queryTableColumns(self, tableName):
        sql = '''
               SELECT
                  COLUMN_NAME,       DATA_TYPE,    CHARACTER_MAXIMUM_LENGTH,
                  COLUMN_COMMENT,    TABLE_NAME
              FROM
                  information_schema.columns
              WHERE
                    TABLE_SCHEMA =  '{dbName}'
                    AND TABLE_NAME = '{tableName}'
              '''

        return self.queryData(sql.format(dbName=self.dataBase, tableName=tableName))

    def constructCteateStatement(self, tableName):
        tableInfos = mysql2Orcl.queryTableColumns(tableName)
        sql = '''CREATE TABLE {tableName}
                (
                  {columns}
                );
              '''

        columns = ''

        try:
            for info in tableInfos:
                columns += info[0].upper() + '\t' + \
                    self.getCharSetType(info) + ', \r\n\t\t'

            columns = columns[:-6]
            return sql.format(tableName=tableName, columns=columns)
        except:
            traceback.print_exc()
            print(tableName)

    def queryData(self, sql):
        cursor = self.db.cursor()
        cursor.execute(sql)
        return cursor.fetchall()


mysql2Orcl = Mysql2Orcl()
# dsfa_syslog_operate

# statement = mysql2Orcl.constructCteateStatement('dsfa_syslog_operate')
# print(statement)


tables = mysql2Orcl.queryTables()
for table in tables:
    tableName = table[0]
    statement = mysql2Orcl.constructCteateStatement(tableName)
    print(statement)
