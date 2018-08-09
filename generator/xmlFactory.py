from lxml import etree


class XMLFactory:

    """Description for XMLFactory
    
        代码中没有检查异常

    函数:
        addSqlStatement(self, sqlStatement, sqlName, sqlType)  # 往 Xml 中添加 SQL 语句 调用此方法后，内部的节点会被自动关联创建
        createParameterNode(self, attrName, attrType)          # 往 Xml 中添加参数列表
    """

    def __init__(self):
        self.__root = etree.Element('BL')

    # 创建 Action 节点
    def __createActionNode(self, attrName, attrType):
        '''
        文档注释
        
        Description

            构造 Action 节点

            Args
               attrName : Action 节点中 name 属性的名称
               arrtType : Action 节点中 tyoe 属性的名称 

        
            Returns
                None

        '''

        root = self.__root
        action = etree.SubElement(root, 'Action')
        action.set('name', attrName)
        action.set('type', attrType)
        action.set('for', '')
        action.set('desc', '')
        action.set('next', '')

        self.__action = action
        self.__createExpressionNode()
        self.__createSqlNode(attrName, attrType)
        self.__createParametersNode()
        self.__createDataSetNode()
        self.__createDataNode()

    def __createExpressionNode(self):
        '''
        文档注释

        Description
            创建 Expression 节点

            Returns
                None
        '''
        action = self.__action
        self.__exprtession = etree.SubElement(action, 'Expression')

    def __createSqlNode(self, sqlName, sqlType):
        '''
        文档注释

        Description
            创建 Sql 节点

            Returns
                None
        '''
        expression = self.__exprtession
        sql = etree.SubElement(expression, 'Sql')
        self.__sql = sql

    def __createParametersNode(self):
        '''
        文档注释

        Description
            创建 Parameters 节点

            Returns
                None
        '''
        expression = self.__exprtession
        parameters = etree.SubElement(expression, 'Parameters')
        self.__parameters = parameters

    def __createDataSetNode(self):
        '''
        文档注释

        Description
            创建 DataSet 节点

            Returns
                None
        '''
        action = self.__action
        dataSet = etree.SubElement(action, 'DataSet')
        dataSet.set('name', '')
        dataSet.set('key', '{%GUID:N}')
        dataSet.set('parentkey', '')
        self.__dataSet = dataSet

    def __createDataNode(self):
        '''
        文档注释

        Description
            创建 Data 节点

            Returns
                None
        '''
        dataSet = self.__dataSet
        data = etree.SubElement(dataSet, 'Data')
        data.set('format', '')
        data.set('name', '')
        data.text = '*'
        return data

    # 创建 Sql 节点
    def addSqlStatement(self, sqlStatement, sqlName, sqlType):
        '''
        文档注释

        Description

            把 SQL 语句添加到对应的 XML 节点中
            
            Args
                sqlStatement：SQL 语句
                sqlName：Action 节点中 name 属性的名称
                sqlType：Action 节点中 tyoe 属性的名称 
            
            Returns
                None
        '''
        self.__createActionNode(sqlName, sqlType)
        sql = self.__sql
        sql.text = etree.CDATA(sqlStatement)

    def createParameterNode(self, attrName, attrType):
        '''
        文档注释

        Description
            添加 Parameter 到 XMl 中
        
        Args
            attrName : SQL 参数名称
            attrType ：SQL 参数类型

        Returns
            None

        '''
        parameters = self.__parameters
        if attrType.find('int') > -1:
            attrType = 'INT'
        elif attrType.find('date') > -1:
            attrType = 'DATE'
        else:
            attrType = 'STRING'
        parameter = etree.SubElement(parameters, 'parameter')
        parameter.set('name', attrName)
        parameter.set('type', attrType)
        parameter.text = '@{' + attrName + '}'

    def writeXmlToFile(self, fileName):
        '''
        文档注释

        Description
            把构造好的 xml 写入 文件
        
            Args
                fileName ： 文件名称
            
            Returns
                None
        '''
        root = self.__root
        tree = etree.ElementTree(root)

        tree.write(
            fileName,
            pretty_print=True,
            xml_declaration=True,
            encoding='utf-8')