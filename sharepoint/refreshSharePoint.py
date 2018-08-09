import pymysql as mysql

import traceback
import time
import datetime
import re

class SharePoint:

    def __init__(self):
        self.host = '192.168.0.49'
        self.port = 3306
        self.user = "root"
        self.password = "dreamsoft"
        self.dataBase = 'ipb_yy'
        self.db = self.getConnection()
    
    def getConnection(self):
        db = mysql.connect(
                host = self.host,
                port = self.port,
                user = self.user,
                password = self.password,
                db = self.dataBase,
                charset="utf8"
        )
        return db
    
    # 领导直通车审核通过的数据 SQL
    def getleaderSQL(self):
        sql = '''
                SELECT 
                    ADVISOR_ID as accountId,CONTENT
                FROM
                    ltc_suggest suggest
                WHERE
                    (STATUS = '3' OR status = '4')
                        AND ADVISOR_ID <> ''
                '''
        return sql

    # 新闻评论审核通过 SQL 
    def getNewsCommentsSQL(self):
        sql  =  '''
                    SELECT 
                        ACCOUNTID AS accountId, CONTENT
                    FROM
                        ST_CLASS_FAQ
                    WHERE
                        REVIEWTYPE = '0'
                '''
        return sql
    
    def getPariseSQL(self):
        sql  = '''
                SELECT 
                    STUDENTID AS accountId,
                    FLOOR(COUNT(*) / 10) AS shareNumber
                FROM
                    ST_ATTENTION
                GROUP BY STUDENTID , EVENTTYPE
                HAVING COUNT(*) > 10 AND EVENTTYPE = '0'
              '''
        return sql

    def getRuleSql(self):
        sql = '''   
                SELECT 
                    POINT, ID
                FROM
                    SP_RULE
                WHERE
                    NAME = '{name}' AND ISDEFAULT = 1
              '''
        return sql

    def getInsertSubsidarySQL(self):
        sql = '''
                INSERT INTO 
                SP_PERSON_SUBSIDIARY
                    (ID,GOAL,ORIGIN,ACCOUNT_ID,REMARK,CREATTIME,DELETED,RULE_ID,YEAR)
                VALUE
                    (md5('{ID}'),'{GOAL}','追加','{ACCOUNT_ID}','规则变更，补全的数据({name})',NOW(),0,'{RULE_ID}',YEAR(NOW()));
              '''
        return sql

    def getUpdateAccount(self):
        sql = '''
                UPDATE ORG_ACCOUNT 
                    SET 
                        INTEG_TOTAL = INTEG_TOTAL + '{GOAL}',
                        INTEG_REST = INTEG_REST + '{GOAL}'
                    WHERE
                        ID = '{ACCOUNT_ID}';
              '''
        return sql

    def queryDate(self,sql):
        db = self.db
        cursor = db.cursor()
        cursor.execute(sql)
        return cursor.fetchall()




    def doRefreshParise(self,name,beFixRecords,file):
        try:
            cursur = self.db.cursor()
            ruleSql = self.getRuleSql()
            formatRuleSql = ruleSql.format(name=name)
            ruleRecod = self.queryDate(formatRuleSql)
            # 分数
            rulePoint  = ruleRecod[0][0]
            # 规则ID
            ruleId = ruleRecod[0][1]

            for record in beFixRecords:
                accountId = record[0]
                fixCycles = record[1]
                for i in range(0,fixCycles):
                    print(i)
                    t = time.time()
                    time.sleep(0.01)
                    tempId = int(round(t * 1000))
                    insertSubsidiarySQL = self.getInsertSubsidarySQL()
                    formatSubsidiarySQL = insertSubsidiarySQL.format(ID = tempId, GOAL = rulePoint, ACCOUNT_ID = accountId, RULE_ID = ruleId, name = name)
                #    cursur.execute(formatSubsidiarySQL)
                #    self.db.commit()
                #    print(formatSubsidiarySQL)
                    file.write('\n'+ formatSubsidiarySQL +'\n' )

                updateAccountSQL = self.getUpdateAccount()
                formateAccountSQL = updateAccountSQL.format(GOAL = rulePoint*fixCycles, ACCOUNT_ID = accountId)
            #    cursur.execute(formateAccountSQL)
            #    self.db.commit()
            #    print(formateAccountSQL)
                file.write('\n'+ formateAccountSQL +'\n' )
        except:
                traceback.print_exc()


    def judegContentImg(self,content):
        replaceImg = re.sub('<img .*?>','',content)
        replaceImg = re.sub('&ltimg .*&gt','',replaceImg)
        trimStr = re.sub(r'\s+','',replaceImg)
        return len(trimStr)

    def doRefreshRegexContent(self,name,beFixRecords,file):
        try:
            cursur = self.db.cursor()
            ruleSql = self.getRuleSql()
            formatRuleSql = ruleSql.format(name=name)
            ruleRecod = self.queryDate(formatRuleSql)
            # 分数
            rulePoint  = ruleRecod[0][0]
            # 规则ID
            ruleId = ruleRecod[0][1]
           
            for record in beFixRecords:
                accountId = record[0]
                fixContent = record[1]

                if not self.judegContentImg(fixContent):
                    print("content can`t be full img or writespace character" ,'\t' ,fixContent)
                    continue
                t = time.time()
                time.sleep(0.01)
                tempId = int(round(t * 1000))
                insertSubsidiarySQL = self.getInsertSubsidarySQL()
                formatSubsidiarySQL = insertSubsidiarySQL.format(ID = tempId, GOAL = rulePoint, ACCOUNT_ID = accountId, RULE_ID = ruleId, name = name)
            #    cursur.execute(formatSubsidiarySQL)
            #    self.db.commit()
            #    print(formatSubsidiarySQL)
                file.write('\n'+ formatSubsidiarySQL +'\n' )
                updateAccountSQL = self.getUpdateAccount()
                formateAccountSQL = updateAccountSQL.format(GOAL = rulePoint, ACCOUNT_ID = accountId)
            #    cursur.execute(formateAccountSQL)
            #    self.db.commit()
            #    print(formateAccountSQL,'\t',fixContent)
                file.write('\n'+ formateAccountSQL +'\n' )
        except:
            traceback.print_exc()

    def launch(self):        
        t = time.time()
        fielName = './refreshSharePoint_K_' + str(int(t))+ '.sql'
        file = open(fielName, 'a', encoding='utf-8')

        pariseSql = self.getPariseSQL()
        print('---------------------点赞----------------------')
        self.doRefreshParise("点赞",self.queryDate(pariseSql),file)

        print('-----------------领导直通车----------------------')
        leaderSql = self.getleaderSQL()
        self.doRefreshRegexContent('领导直通车',self.queryDate(leaderSql),file)

        print('----------------新闻审核-----------------------')
        newsCommentsSql = self.getNewsCommentsSQL()
        self.doRefreshRegexContent('新闻审核',self.queryDate(newsCommentsSql),file)

        file.close()

sharePoint = SharePoint()

sharePoint.launch()






