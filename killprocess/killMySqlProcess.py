import DataSource 
import subprocess


dataSource = DataSource.DataSource()

url="mysql  -e 'show processlist' -uroot -proot"
subprocess.Popen(url)


processInfo =dataSource.getConnectedProcess()

print (processInfo)
print (type(processInfo))
    
