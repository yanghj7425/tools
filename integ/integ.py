import pymysql as mysql
import integ

import traceback
import time
import datetime

class Integ:
    def __init__(self):

        self.host = '192.168.0.28'
        self.port = 3306
        self.user = "root"
        self.password = "123456"
        self.dataBase = 'ipb_pudong'



    def getConnection(self):
        
        db = mysql.connect(
                host = self.host,
                port = self.port,
                user = self.user,
                password = self.password,
                db = self.dataBase
            )
        return db


    def show(self, file, mem):

        db =self.getConnection()
        cursor = db.cursor()
    
        # 查询个人总分
        if mem == 1:
            cursor.execute("select TOTAL, MEMBERID from INTEG_PERSON_POINT where year = 2018")
        else:
            cursor.execute("select TOTAL, MEMBERID from INTEG_PERSON_POINT where year = 2018 and memberId = %s", mem)

        pointScore = cursor.fetchall()

        loop = 0
        for eachPoint in pointScore:
            loop = loop + 1
            print(loop)
            memberScore = eachPoint[0]
            memberId = eachPoint[1]

            # 根据党员ID 计算各积分项总分
            querySubsidiarySql = ''' SELECT 
                                        rul.fid as level2Id, SUM(sub.POINT) point
                                    FROM
                                        INTEG_PERSON_SUBSIDIARY sub
                                            LEFT JOIN
                                        integ_rule rul ON rul.id = sub.RULE_ID
                                    WHERE
                                        PARTY_MEMBER_ID = '{memberId}'
                                        AND sub.year = '2018'
                                    GROUP BY rul.fid
                                '''
            formatSQL = querySubsidiarySql.format(memberId=memberId)
            cursor.execute(formatSQL)
            subsidiary = cursor.fetchall()
            
            # 保存总分
            sumLevle2Score = 0

            for each in subsidiary:
                level2Id = each[0]

                #从积分明细中获取各项总分
                level2Score = each[1]

                sumLevle2Score += level2Score

                #查询单项总分
                queryIntegBaseSql = "select BASCPOINT from integ_base where year='2018'  and MEMBERID = '{memberId}' and BASCRULEID = '{level2Id}'; "

                formatSQL = queryIntegBaseSql.format(memberId=memberId, level2Id=level2Id)

                cursor.execute(formatSQL)
                eachItem = cursor.fetchone()

                #从积分表中查询该项总分
                eachItemSumScore = 0
                if eachItem is None:
                    try:
                        t = time.time()
                        tempId = int(round(t * 1000))
                        # 插入一条默认数据
                        insertIntegBaseSql = "insert into integ_base (ID,BASCRULEID,BASCPOINT,YEAR,MEMBERID) select md5('{tempId}'),'{level2Id}',0,date_format(now(),'%Y'),'{memberId}' from dual where not exists (select BASCPOINT from integ_base where year='2018'  and MEMBERID = '{memberId}' and BASCRULEID = '{level2Id}');"
                        formatSQL = insertIntegBaseSql.format(tempId=tempId, level2Id=level2Id, memberId=memberId)
                        print(formatSQL)
                        cursor.execute(formatSQL)
                        file.write('\n'+formatSQL + ';' + '\n' )
                        db.commit()
                    except:
                        traceback.print_exc()
                        print(formatSQL)
                        db.rollback()
    
                else:
                    eachItemSumScore = eachItem[0]   

                #如果计算出来的 单项总分和数据库中不一致
                if level2Score != eachItemSumScore:
                    # 单项总分
                    updateIntegBaseSql ='''   
                                            UPDATE integ_base 
                                            SET 
                                                BASCPOINT = '{level2Score}'
                                            WHERE
                                                year = '2018'
                                                    AND MEMBERID = '{memberId}'
                                                    AND BASCRULEID = '{level2Id}' '''        
                    try:
                        formatSQL = updateIntegBaseSql.format(level2Score=level2Score, memberId=memberId, level2Id=level2Id)

                        file.write('\n'+formatSQL + ';#'+ str(eachItemSumScore) + '\n' )
                        cursor.execute(formatSQL)
                        db.commit()
                    except:
                        traceback.print_exc()

                        db.rollback()
            
            # 个人总分
            updatePersonPointSql = '''
                                    UPDATE INTEG_PERSON_POINT 
                                        SET 
                                            TOTAL = '{total}'
                                        WHERE
                                        MEMBERID = '{memberId}' '''

            if memberScore != sumLevle2Score:
                try:
                    formatSQL = updatePersonPointSql.format(total = sumLevle2Score,memberId=memberId)
                    cursor.execute(formatSQL)
                    file.write('\n'+formatSQL + ';#' + str(memberScore) + '\n' )
                    db.commit()
                except:
                    traceback.print_exc()
                    db.rollback()
     

# a = Integ()

# t = time.time()
# fielName = 'integConifg_' + str(int(t))+ '.sql'
# file = open(fielName, 'a', encoding='utf-8')
# mem = 1
# a.show(file, mem)
# file.close()
