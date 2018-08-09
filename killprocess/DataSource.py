import pymysql as mysql

class DataSource:
    """ Description for DataSource
     
    初始化是要时需要配置数据库信息，先采用硬编码的方式，也没有检查异常
        host：地址
        port: 端口号
        user: 用户名
        password: 密码
        dataBase: 数据库

    函数:
        getConnection(self)  # 获得数据库连接对象
        getCursor(self)      # 获得 Cursor 操作对象
        getConnectedProcess(self) # 获取表的 Desc 描述信息
        closeDataSource(self) # 关闭数据连接


    """

    def __init__(self):
        '''
        文档注释
        
        Description
            初始化数据库对象


            Returns
                None

        '''
        self.host = '192.168.0.49'
        self.port=3306
        self.user="root"
        self.password="dreamsoft"
        self.dataBase='ipb'

    
    def getConnection(self):
        '''


        '''
        db = mysql.connect(
            host = self.host,
            port = self.port,
            user = self.user,
            password = self.password,
            db = self.dataBase
        )

        self.db = db

        return db


    def getCursor(self):

        db = self.getConnection()

        return db.cursor()

    def getConnectedProcess(self):

        cursor = self.getCursor()

        processInfo = cursor.execute('show processlist')

        return processInfo