# coding=utf-8
import pymysql as mysql

import traceback
import time
import datetime
import re
import os


class Mysql2Orcl:

    def __init__(self):
        self.host = '192.168.0.6'
        self.port = 3307
        self.user = 'dsfa'
        self.password = 'dsfa@123'
        self.dataBase = 'dsfa'
        self.db = self.__getConnection()
        self.dataSet = self.__initDataSet()
        self.insertSQLModel = 'mysqldump_{tableName}_Insert.sql'

    def __initDataSet(self):
        dataSet = {}
        dataSet['varchar'] = 'NVARCHAR2'
        dataSet['datetime'] = 'TIMESTAMP'
        dataSet['decimal'] = 'NUMBER'
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

    def __getCharSetType(self, info):
        typeSize = info[2]
        mysqlTypeInfo = info[1]

        if 'text' in mysqlTypeInfo:
            return 'CLOB'

        dataSet = self.dataSet[mysqlTypeInfo]
        if typeSize is None:
            if mysqlTypeInfo == 'int':
                return dataSet + '(10)'
            else:
                return dataSet

        if typeSize > 3000:
            return 'CLOB'

        return dataSet + '(' + str(typeSize) + ')'

    def __queryData(self, sql):
        '''
        文档注释

        Description
            查询执行SQL

            Args
                sql : 需要执行得SQL

            Return
                执行结果
        '''
        cursor = self.db.cursor()
        cursor.execute(sql)
        return cursor.fetchall()

    def __deleteSpecialSimple(self, tableName):
        '''
          文档注释

          Description
              删除导出文件中的 '`' 符号

              Args
                  tableName: 表名
        '''
        rRegex = r'INSERT .*? VALUES'
        file = self.insertSQLModel.format(tableName=tableName)
        with open(file, "r", encoding="utf-8") as f1, open("%s.bak" % file, "w", encoding="utf-8") as f2:
            for line in f1:
                matchObj = re.search(rRegex, line)
                if matchObj:
                    replaceStr = matchObj.group().replace('`', '')
                    line = line.replace(matchObj.group(), replaceStr.upper())
                    f2.write(line)

        os.remove(file)
        os.rename("%s.bak" % file, file)

    def __exportMysqlDump(self, tableName):
        '''
        文档注释

        Description
            调用 mysqldump 工具生成包含 insert 语句的SQL
            Args
                tableName : 表名
        '''
        cmd = 'mysqldump  --user={user} -p{password}  --host={host} --port={port} --protocol=tcp  --skip-comments --complete-insert --allow-keywords --default-character-set=utf8 --skip-add-locks  --extended-insert=FALSE  --single-transaction=TRUE --no-create-info --skip-triggers {dbname} {tableName} > mysqldump_{tableName}_Insert.sql'
        os.system(cmd.format(user=self.user,
                             password=self.password,
                             host=self.host,
                             port=self.port,
                             dbname=self.dataBase,
                             tableName=tableName))

    def __queryTableColumns(self, tableName):
        '''
          文档注释

          Description
              查询 mysql 表 的对应列信息

              Return
                  所查询表对应列的 tuble

        '''

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

        return self.__queryData(sql.format(dbName=self.dataBase, tableName=tableName))

    def __dealWithTimeStampLine(self, line, tableName):

        columnInfo = self.__queryTableColumns(tableName)

        rRegex = r'.*?\((.*?)\).*?'
        rets = re.findall(rRegex, line)
        for ret in rets:
            print(ret)

        return line

    def __dealWithTimeStamp(self, tableName):
        file = self.insertSQLModel.format(tableName=tableName)
        with open(file, "r", encoding="utf-8") as f1, open("%s.bak" % file, "w", encoding="utf-8") as f2:
            for line in f1:
                line = self.__dealWithTimeStampLine(line, tableName)
                f2.write(line)
        os.remove(file)
        os.rename("%s.bak" % file, file)

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
        return self.__queryData(sql.format(dbName=self.dataBase))

    def constructCteateStatement(self, tableName):
        '''
        文档注释

        Description
            生成对应表 oracle 下的 create 语句

            Return
                string : 生成的 create 语句字符串

        '''
        tableInfos = mysql2Orcl.__queryTableColumns(tableName)
        sql = '''
CREATE TABLE {tableName}
(
    {columns}
);
              '''

        columns = ''

        try:
            for info in tableInfos:
                columns += '"{columnName}"'.format(columnName=info[0].upper()) + '\t' + \
                    self.__getCharSetType(info) + ', \n\t\t'

            columns = columns[:-5]
            return sql.format(tableName=tableName, columns=columns)
        except:
            traceback.print_exc()
            print(tableName)

    def constructInsertStatement(self, tableName):
        '''
        文档注释

        Description
            构造 insert 语句,并生成对应文件。文件调用 mysqldump 工具生成。
            Args
                tableName : 表名
        '''
        self.__exportMysqlDump(tableName)
        self.__deleteSpecialSimple(tableName)

    def constructDropTableStatement(self, tableName):
        '''
        文档注释

        Description
            构造 drop 语句

            Args
                tableName : 表名

            Return 
                drop 语句
        '''
        dropSQL = 'drop table {tableName}; \n'.format(tableName=tableName)
        return dropSQL.upper()


mysql2Orcl = Mysql2Orcl()

t = time.time()


tables = mysql2Orcl.queryTables()


tableName = 'dsfa_cms_auth'
mysql2Orcl.constructInsertStatement(tableName)

# dropFile = open('Mysql2Orcl_{dbname}_Drop_'.format(dbname=mysql2Orcl.dataBase) + str(int(t)) +
#                   '.sql', 'a', encoding='utf-8')
# statement = mysql2Orcl.constructDropTableStatement(tableName)
# dropFile.write(statement)
# dropFile.close

# createFile = open('Mysql2Orcl_{dbname}_Create_'.format(dbname=mysql2Orcl.dataBase) + str(int(t)) +
#                   '.sql', 'a', encoding='utf-8')
# statement = mysql2Orcl.constructCteateStatement(tableName)
# createFile.write(statement)
# createFile.close


# for table in tables:
#     mysql2Orcl.constructInsertStatement(table[0])

# dropFile = open('Mysql2Orcl_{dbname}_Drop_'.format(dbname=mysql2Orcl.dataBase) + str(int(t)) +
#                 '.sql', 'a', encoding='utf-8')
# for table in tables:
#     statement = mysql2Orcl.constructDropTableStatement(table[0])
#     dropFile.write(statement)
# dropFile.close


# createFile = open('Mysql2Orcl_{dbname}_Create_'.format(dbname=mysql2Orcl.dataBase) + str(int(t)) +
#                   '.sql', 'a', encoding='utf-8')
# for table in tables:
#     statement = mysql2Orcl.constructCteateStatement(table[0])
#     createFile.write(statement)
# createFile.close
