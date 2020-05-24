from datetime import datetime as dt
import datetime
import sqlite3



def dbwrite(csv_file_name):
    #ファイルを一行ずつ読みこむ
    d = dt.now().strftime('%Y%m%d')
    path = d+'-alldata.txt'
    with open(path) as f:
        for s_line in f:
            print(s_line)
            row=s_line.split(",")
            code=row[0]  #code
            d=row[1]     #date
            o=row[2]     #open
            h=row[3]     #high
            l=row[4]     #low
            close=row[5] #close
            v=row[6]     #volume
            #DBに登録
            dbinsert(code,d,o,l,h,close,v)
def dbinsert(code,d,open,high,low,close,volume):
        dbname='kabuka2.db'
        conn=sqlite3.connect(dbname)
        c = conn.cursor()
        sql = 'INSERT INTO kabuka3(code,hizuke,open,high,low,close,volume) VALUES(?,?,?,?,?,?,?)'
        user = (code,d,open,high,low,close,volume)
        c.execute(sql, user)
        conn.commit()
        conn.close()
#DB書き込み   
#d = dt.now().strftime('%Y%m%d')
path_w = '20200519-alldata.txt'
    
dbwrite(csv_file_name=path_w)
print("end")