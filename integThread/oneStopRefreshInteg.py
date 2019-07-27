import pymysql as mysql

import traceback
import time
import datetime
import threading
import math


class oneStopRefreshInteg:
    def __init__(self):
        self.host = 'host'
        self.port = 3306
        self.user = 'root'
        self.password = 'root'
        self.dataBase = 'test'

    def getConnection(self):
        db = mysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.dataBase,
            charset="utf8")
        return db

    def actionThread(self, threadName, startIndex, eachTheardParseCounts):
        members = self.getRefreshMemberId(startIndex, eachTheardParseCounts)
        db = self.getConnection()
        cursor = db.cursor()
        rules = self.queryRuleLevel3()
        for member in members:
            memberId = member[0]
            # 清空个人积分
            self.clearBaseAndPoint(cursor, memberId)
            db.commit()
            # print(threadName, ":", memberId)
            # print(":")
            for rule in rules:
                sTotal = 0.0
                ruleId = rule[0]
                ruleTotal = float(rule[1])
                ruleFid = rule[2]
                subsidarys = self.querySubsidary(cursor, memberId, ruleId)
                for subsidary in subsidarys:
                    sId = subsidary[0]
                    sScore = float(subsidary[1])
                    #如果是第一条记录
                    if sTotal == 0:
                        if sScore > ruleTotal:
                            self.updateSubSidary(cursor, ruleTotal, sId, 1)
                            sTotal = ruleTotal
                        else:
                            sTotal = sScore
                    else:
                        if sTotal < ruleTotal:
                            if sTotal + sScore > ruleTotal:
                                self.updateSubSidary(
                                    cursor, ruleTotal - sTotal, sId, 1)
                                sTotal = ruleTotal
                            else:
                                sTotal = sTotal + sScore
                        else:
                            self.updateSubSidary(cursor, 0, sId, 0)

                self.updateIntegBase(cursor, memberId, ruleFid, sTotal)
                self.updateIntegPoint(cursor, memberId, sTotal)
                db.commit()

        print(startIndex, eachTheardParseCounts)

    def _main(self, threadCounts):
        beRefreshMeberCounts = self.getRefreshMemberCcount()[0]
        eachTheardParseCounts = math.ceil(beRefreshMeberCounts / threadCounts)
        # print(beRefreshMeberCounts)
        for i in range(threadCounts):
            threadName = 'thread_' + str(i)
            startIndex = eachTheardParseCounts * i
            t = threading.Thread(
                target=self.actionThread,
                args=(threadName, startIndex, eachTheardParseCounts))
            t.start()

    def sumIntegBase(self, cursor, memberId):
        sumIntegBaseSQL = """
                            SELECT 
                                SUM(BASCPOINT)
                            FROM
                                integ_base
                            WHERE
                                MEMBERID = '{memberId}'
                                AND YEAR =  date_format(now(),"%Y");
         
                         """
        cursor.execute(sumIntegBaseSQL.format(memberId=memberId))
        ret = cursor.fetchone()
        return ret[0]

    # 清除个人积分
    def clearBaseAndPoint(self, cursor, memberId):
        baseSQL = """
              			update integ_base 
                    set BASCPOINT = 0 
                    where 
                      MEMBERID = '{memberId}' 
                      and 
                      year = date_format(now(),"%Y");
                  """
        pointSQL = """  
                    UPDATE integ_person_point 
                              SET 
                                  TOTAL = 0
                              WHERE
                                  MEMBERID = '{memberId}'
                                  AND 
                                  year = DATE_FORMAT(NOW(), '%Y');
                   """
        ebaseSQL = baseSQL.format(memberId=memberId)
        epointSQL = pointSQL.format(memberId=memberId)
        # print(sql)
        cursor.execute(ebaseSQL)
        cursor.execute(epointSQL)
        # 插入一条默认数据
        self.insertAnDefaultPointData(cursor, memberId)

    # 更新个人总分
    def updateIntegPoint(self, cursor, memberId, score):
        updateIntegPointSQL = """      
                              UPDATE integ_person_point 
                              SET 
                                  TOTAL = TOTAL + {score}
                              WHERE
                                  MEMBERID = '{memberId}'
                                      AND year = DATE_FORMAT(NOW(), '%Y');
       
                              """
        sql = updateIntegPointSQL.format(score=score, memberId=memberId)
        #  print(sql)
        cursor.execute(sql)

    # 更新Base分数
    def updateIntegBase(self, cursor, memberId, fId, socre):
        updateIntegBaseSQL = """
                                UPDATE integ_base 
                                SET 
                                    BASCPOINT = BASCPOINT + {score}
                                WHERE
                                    MEMBERID = '{memberId}'
                                        AND BASCRULEID = '{fId}'
                                        AND year = DATE_FORMAT(NOW(), '%Y');    
                             """

        updateRefreshStatusSQL = """
                                UPDATE refreshmember 
                                SET 
                                    estatus = 1
                                WHERE
                                    memberId = '{memberId}'
                                """
        baseSql = updateIntegBaseSQL.format(
            score=socre, memberId=memberId, fId=fId)

        statusSql = updateRefreshStatusSQL.format(memberId=memberId)

        self.insertAnDefalutBaseData(cursor, memberId, fId)

        cursor.execute(baseSql)
        cursor.execute(statusSql)

    def insertAnDefaultPointData(self, cursor, memberId):
        queryIntegPointSql = """
                              SELECT 
                                  MEMBERID
                              FROM
                                  integ_person_point
                              WHERE
                                  memberId = '{memberId}'
                                  AND year = DATE_FORMAT(NOW(), '%Y')
                             """
        formatSQL = queryIntegPointSql.format(memberId=memberId)

        cursor.execute(formatSQL)
        eachItem = cursor.fetchone()
        if eachItem is None:
            try:
                # 插入一条默认数据
                insertIntegBaseSql = """ 
                                        insert into  integ_person_point
                                            (ID,TOTAL,YEAR,MEMBERID) 
                                        values 
                                            (md5(uuid()), 0, DATE_FORMAT(NOW(), '%Y'), '{memberId}')
                                     """
                formatSQL = insertIntegBaseSql.format(memberId=memberId)
                cursor.execute(formatSQL)
            except:
                traceback.print_exc()

    #插入一条默认Base数据
    def insertAnDefalutBaseData(self, cursor, memberId, fid):

        queryIntegBaseSql = """
                              SELECT 
                                    BASCPOINT
                                FROM
                                    integ_base
                                WHERE
                                    year = DATE_FORMAT(NOW(), '%Y')
                                        AND MEMBERID = '{memberId}'
                                        AND BASCRULEID = '{fid}'
                            """

        formatSQL = queryIntegBaseSql.format(memberId=memberId, fid=fid)

        cursor.execute(formatSQL)
        eachItem = cursor.fetchone()
        if eachItem is None:
            try:
                # 插入一条默认数据
                insertIntegBaseSql = """
                                        insert into integ_base 
                                            (ID,BASCRULEID,BASCPOINT,YEAR,MEMBERID)
                                         values
                                            (md5(uuid()), '{fId}', 0, date_format(now(),'%Y'),'{memberId}')
                                    """

                formatSQL = insertIntegBaseSql.format(
                    fId=fid, memberId=memberId)

                cursor.execute(formatSQL)
            except:
                traceback.print_exc()

    # 更新明细
    def updateSubSidary(self, cursor, cscore, sId, state):
        updateSubSidarySQL = """
                                UPDATE integ_person_subsidiary 
                                SET 
                                    point = {score},
                                    ISOVERFLOW = 1,
                                    state ={state}
                                WHERE
                                    id = '{sId}'
                             """
        sql = updateSubSidarySQL.format(score=cscore, sId=sId, state=state)
        #print(sql)
        cursor.execute(sql)

    # 查询明细记录
    def querySubsidary(self, cursor, memberId, ruleId):
        subSidarySQL = """
                        SELECT 
                          ID, POINT
                        FROM
                          integ_person_subsidiary
                        WHERE
                          PARTY_MEMBER_ID = '{cmemberId}'
                            AND rule_id = '{ruleId}'
                            AND year = DATE_FORMAT(NOW(), '%Y')
                       """
        sql = subSidarySQL.format(cmemberId=memberId, ruleId=ruleId)
        cursor.execute(sql)
        return cursor.fetchall()

    def queryRuleLevel3(self):
        level3RuleSQL = """
                         SELECT 
                              id, total, fid
                          FROM
                              integ_rule
                          WHERE
                              level = '3'
                        """
        return self.queryDBData(level3RuleSQL)

    # 分批查询
    def getRefreshMemberId(self, startIndex, eachTheardParseCounts):
        refreshMemberIdSQL = """  
                                SELECT 
                                    memberId
                                FROM
                                    refreshmember  where estatus = 0
                                ORDER BY memberId DESC
                                LIMIT {index} , {size}
                             """

        ret = self.queryDBData(
            refreshMemberIdSQL.format(
                index=startIndex, size=eachTheardParseCounts))

        return ret

    # 查询要刷新的党员总数
    def getRefreshMemberCcount(self):
        refreshCountSql = """
                          SELECT 
                              COUNT(*)
                          FROM
                              refreshmember where estatus = 0
                          """
        ret = self.queryDBData(refreshCountSql)

        return ret[0]

    def queryDBData(self, sql):
        db = self.getConnection()
        cursor = db.cursor()
        cursor.execute(sql)
        ret = cursor.fetchall()
        db.close()
        return ret


a = oneStopRefreshInteg()
t = time.time()
#fielName = 'oneStopRefreshInteg' + str(int(t)) + '.sql'
#file = open(fielName, 'a', encoding='utf-8')
threadCounts = 30
a._main(threadCounts)
#file.close()
