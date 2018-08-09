import re 

str = '涂鸦 &ltimg src="../images/comment/arclist/15.gif" border="0" style="-webkit-touch-callout: none; -webkit-user-select: none;"&gthaha&ltimg src="../images/comment/arclist/5.gif" border="0" style="-webkit-touch-callout: none; -webkit-user-select: none;"&gt的改变'

replacedStr = re.sub('&ltimg .*?&gt','',str)
print('replaced:', replacedStr,len(replacedStr))
trimedStr = re.sub(r'\s+','',replacedStr)
print(len(trimedStr))
print(trimedStr)

if not 0:
    print('-------------')