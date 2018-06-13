import xmlFactory


class XMLBulider:

    """Description for XMLBulider
    
    初始化函数的两个参数:
        colums: 对应每个字段的描述
        tableName: 表名称

        代码中没有检查异常

    函数:
        execute(self,executeType)  # 执行生成对应 xml
    """
    def __init__(self, colums, tableName):
        '''
        文档注释
        
        Args
            colums:  对应每个字段的描述
            tableName: 表名称
      
        '''
        self.__factory = xmlFactory.XMLFactory()
        self.__cloums = colums
        self.__tableName = tableName
        self.insert = 'INSERT'
        self.select = 'SELECT'
        self.__from = 'FROM'
        self.__into = 'INTO'
        self.__values = 'VALUES'
        self.__where = 'WHERE'

        self.__tab1 = '\t'
        self.__tab2 = '\t\t'
        self.__tab3 = '\t\t\t'
        self.__tab5 = '\t\t\t\t\t'
        self.__tab6 = '\t\t\t\t\t\t'


    def __insertSatement(self):
        '''
        文档注释
        Description
            构造插入语句并添加到 xml 节点中

        Returns
            None
        '''

        factory = self.__factory
        sql = self.__constructInsertSql()

        factory.addSqlStatement(sql, 'name', 'SQL_EXECUTE')
        self.__addParameter()

    def __constructInsertSql(self):

        '''
        文档注释

        Description
            生成插入语句的 SQL 并格式化

        Returns 
            sql：生成好的 SQL 语句

        '''

        tableInfo = self.__cloums
        sql = '\n' + self.__tab5 + self.insert + '\n'
        sql += self.__tab5 + self.__tab2 + self.__tableName + '\n'
        sql += self.__tab5 + self.__values + '\n'
        sql += self.__tab6 + '('

        for table in tableInfo:
            sql += table[0] + ',\n' + self.__tab6
        sql = sql[:-9] + ')'

        param = self.__tab6 + '(' 
        for i in range(0, len(tableInfo)):
            param += '?,' + '\n' + self.__tab6
        param = param[:-8] + ')'
        sql = sql + '\n' + self.__tab2 + self.__tab5 + self.__into + '\n'
        sql += param + '\n' + self.__tab5

        return sql

    def __selectStatement(self):
        ''' 
        文档注释

        Description
            构造 select 语句 并插入 xml 节点中

        Returns 
            None
        '''
        factory = self.__factory

        sql = self.__constructSelectSql()

        factory.addSqlStatement(sql, 'name', 'SQL_QUERY')
        self.__addParameter()

    def __constructSelectSql(self):
        '''
        文档注释

        Description
            生成 select  语句并格式化
        
        Returns 
            sql:构造好的  SQL 语句

        '''
        tableInfo = self.__cloums
        sql = '\n' + self.__tab5 + self.select + '\n' + self.__tab6

        for table in tableInfo:
            sql += table[0] + ',\n' + self.__tab6

        sql = sql[:-9]
        sql += '\n' + self.__tab5 + self.__from + '\n' + self.__tab6
        sql += self.__tableName + '\n' + self.__tab5
        sql += self.__where + '\n' + self.__tab6 + tableInfo[0][0]
        sql += ' = ?' + self.__tab1

        return sql

    def __addParameter(self):
        '''
        文档注释
        
        Description
            动态添加 Parameter 参数节点

        Returns
            None
        '''
        factory = self.__factory
        colums = self.__cloums
        for colum in colums:
            factory.createParameterNode(colum[0], colum[1])

    def execute(self, executeType):

        '''
        文档注释
        
        Description
            根据参数生成 对应类型 xml 
        
        Args 
            executeType: 执行类型

        Returns 
            None
        '''

        factory = self.__factory
        if self.select == executeType:
            self.__selectStatement()
        elif self.insert == executeType:
            self.__insertSatement()
        else:
            self.__selectStatement()
            self.__insertSatement()

        factory.writeXmlToFile(self.__tableName + '.xml')