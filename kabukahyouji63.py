#!/usr/bin/env python
# -*- coding: utf-8 -*-
#-----------------------------------------
#Note 
#   株価をYAHOOからスクレイピングするプログラム
#   beautiful soupを使わないプログラム
#   日のついてくるバグを修正
#   sqlite3に入れる機能を付ける
#   タスクスケジュラーでない
#------------------------------------------

import codecs, os
import datetime
import urllib.request, urllib.error
import re
import csv
import sys
import time
from datetime import datetime as dt
import datetime
import sqlite3
from contextlib import closing
#import schedule


# Yahoo!ファイナンスから株価データをダウンロードするクラス
class StockDataGetter:
    def __init__(self, frm, to, market):
        self.from_date = datetime.datetime.strptime(frm, '%Y/%m/%d')
        self.to_date   = datetime.datetime.strptime(to, '%Y/%m/%d')
        self.market    = market
        self.alldata   =""
        #以下ディレクトリ作成
        d = dt.now().strftime('%Y-%m-%d')
        if not os.path.exists(d):
            os.mkdir(d)
        self.data_dir  = d
        
    #-----------------------
    # 1,株価データの新規取得
    #    get_price_data
    #------------------------
    def get_price_data(self, code):
        print(code)
        print("----1------")
        self.code = code
        self.save_to_file(self.prices_text())  #3,10
    #-----------------------
    # 2,株価データの新規取得
    #    update_price_data
    #------------------------
    def update_price_data(self, code):
        print("----2-----")
        self.code = code
        if os.path.exists(self.data_file_name()):
            self.get_from_date()
            self.append_to_file(self.prices_text())
        else:
            self.save_to_file(self.prices_text())
    #-------------------------------
    # 3,ファイルに記録される文字列
    #   prices_text
    #-------------------------------
    def prices_text(self):
        print("----3 ファイルに記録される文字列------")
        prices = self.get_price_data_from_historical_data_pages()#4
        if len(prices) == 0:
            return []
        else:
            return self.prices_to_text(prices)   #7
    #------------------------------------
    # 4,時系列データのページを読み込む
    #   get_price_data_from_historical_data_pages
    #--------------------------------------
    def get_price_data_from_historical_data_pages(self):
        print("----4 時系列データのページを読み込む------")
        page_num = 1
        prices = []
        reg_data = re.compile(self.reg_prices())        #6
        reg_next = re.compile(r'次へ</a></ul>')
        while True:
            url = self.url_for_historical_data(page_num) #5
            try:
                request = urllib.request.Request(url)
                text = urllib.request.urlopen(request).read().decode('utf-8')
            except urllib.error.HTTPError:
                return []
            except urllib.error.URLError:
                return []
            prices.extend(reg_data.findall(text))
            page_num += 1
            if reg_next.search(text) == None:
                break
        return prices
    #------------------------
    #5, 時系列データのURL
    #url_for_historical_data
    #---------------------------
    def url_for_historical_data(self, page_num):
        print("----5 時系列データのURL------")
        return 'http://info.finance.yahoo.co.jp/history/?code={0}.{1}&sy={2}&sm={3}&sd={4}&ey={5}&em={6}&ed={7}&tm=d&p={8}'.format(self.code, self.market, self.from_date.year, self.from_date.month, self.from_date.day, self.to_date.year, self.to_date.month, self.to_date.day, page_num)
    #------------------------------------------------
    # 6,株価データ表から株価データを取り出すための
    #   正規表現パターン
    # reg_prices
    #------------------------------------------------
    def reg_prices(self):
        print("----6 株価データ表から株価データを取り出すための正規表現パターン------")
        return r'<td>(\d{4}年\d{1,2}月\d{1,2}日)</td><td>((?:\d|,)+)</td><td>((?:\d|,)+)</td><td>((?:\d|,)+)</td><td>((?:\d|,)+)</td><td>((?:\d|,)+)</td><td>((?:\d|,)+)</td>'
    #-----------------------------------------
    # 7,株価データの配列を文字列に変換
    #   prices_to_text
    #------------------------------------
    def prices_to_text(self, prices):
        print("----7 株価データの配列を文字列に変換------")
        new_prices = []
        reg_month = re.compile('/(\d)/')
        reg_day   = re.compile('/(\d)$')
        count=1
        for price in reversed(prices):
            p = []
            if count>1:
                p.append(self.code)    
            # price[0]は日付
            # 年月日の文字を取り除き"/"で区切る
            print("price[0]"+price[0])
            p.append(re.sub('[年月]', '/', price[0]))
            #print('p1 before'+p[0])
            if count>1:
                p[1] = re.sub('日', '', p[1])
                #print('p2 after'+p[0])
            else:
                p[0] = re.sub('日', '', p[0])
            # 1桁の月や日を0で始まる二桁の数字に            
            if reg_month.search(p[0]):
                p[0] = reg_month.sub('/0' + reg_month.search(p[0]).group(1) +'/', p[0])
            if reg_day.search(p[0]) != None:
                p[0] = reg_day.sub('/0' + reg_day.search(p[0]).group(1), p[0])
            # price[1..6]は値段と出来高
            # 数字の間にあるカンマを取り除く
            for i in range(1, 7):
                p.append(re.sub(',', '', price[i]))
            new_prices.append(','.join(p))#ここに改行コードを入れる？i=1のときcodeの番号も
            count=count+1
        return '\n'.join(new_prices)
    #----------------------------------------------
    #8, 
    #   data_file_name
    #----------------------------------------------
    def data_file_name(self):
        print("----8 data_file_name-------")
        return '{0}/{1}.txt'.format(self.data_dir, self.code)
    #----------------------------------------------
    #9, ファイル中の最終日の翌日を新しい開始日とする
    #   get_from_date
    #----------------------------------------------
    def get_from_date(self):
        print("----9------")
        last_date = codecs.open(self.data_file_name(), 'r', 'utf-8').readlines()[-1][:10]
        self.from_date = datetime.datetime.strptime(last_date, '%Y/%m/%d') + datetime.timedelta(days=+1)
    #-------------------------------
    # 10,データをテキストに保存
    #  save_to_file
    #--------------------------------
    def save_to_file(self, prices_text):
        print("----10 データをテキストに保存-----")
        self.save(prices_text, "w")
    #------------------------------------
    # 11,既存のファイルにデータを追加
    #   append_to_file
    #-----------------------------------
    def append_to_file(self, prices_text):
        print("----11-------")
        self.save(prices_text, "a")
    #-------------------------------------
    #12,save
    #--------------------------------------
    def save(self, prices_text, open_mode):
        print("------12 save    -------")
        if len(prices_text) != 0:
            f = codecs.open(self.data_file_name(), open_mode)
            prices_text=self.code+","+prices_text     #codeを追加
            print("prices_text="+prices_text)
            f.write(prices_text)
            f.close()
            self.alldata=self.alldata+prices_text+"\n"
    #----------------
    #alldatawrite
    #----------------
    def alldatawrite(self):
        d = dt.now().strftime('%Y%m%d')
        path_w = d+'-alldata.txt'
        with open(path_w, mode='w') as f:
           f.write(self.alldata)

#-----------------------------Classs end---------------------

#-----------------
#共通定数
#------------------
csvnm='225list.csv'
csvnm='core30_2.csv'
dbname='kabuka.db'
#---------------------------------------------------
# data225
#---------------------------------------------------
def data225():
    kabucode=[]
    with open(csvnm, 'r') as f:
#    with open('./data/Test5.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            kabucode.append(str(row[0]))
    return(kabucode)
#---------------------------------------------------
#dbwrite
#  DBに書き込む
#  サンプルレイアウト
#  1332,2019/11/01,616,617,608,615,2432600,615
#-----------------------------------------------------
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


#----------------------------
# dbinsert
# 参考
# d = datetime.datetime.strptime(row[0], '%Y/%m/%d').date() #日付
# o = float(row[1])  # 初値
# h = float(row[2])  # 高値
# l = float(row[3])  # 安値
# c = float(row[4])  # 終値
# v = int(row[5])    # 出来高
# yield code, d, o, h, l, c, v
# create table kabuka(code,date,open REAL,high REAL,low REAL,close REAL,volume INTEGER ,close2 REAL) 
#----------------------------
def dbinsert(code,d,open,high,low,close,volume):
        dbname='kabuka.db'
        conn=sqlite3.connect(dbname)
        c = conn.cursor()
        sql = 'INSERT INTO kabuka3(code,hizuke,open,high,low,close,volume) VALUES(?,?,?,?,?,?,?)'
        user = (code,d,open,high,low,close,volume)
        c.execute(sql, user)
        conn.commit()
        conn.close()
#    select_sql = 'select * from users'
#    for row in c.execute(select_sql):
#        print(row)



#-----------------
#job(メイン処理)
#------------------
def job():
    print("Start Hello")
    args = sys.argv
    print(args)
    if len(args)==3:
        frm =args[1]
        to  = args[2]
        print("開始日")
        print(frm)
        print("終了日")
        print(to)
 
    else:
        n1 = datetime.datetime.now()
        n2 =n1 + datetime.timedelta(weeks=-1)
        to=n1.strftime('%Y/%m/%d')
        frm=n2.strftime('%Y/%m/%d')
        print("開始日")
        print(frm)
        print("終了日")
        print(to)
    count=1
    #-----------
    #曜日Check
    #    0は月曜日
    #------------
    """
    weekday = datetime.date.today().weekday()
    if weekday==5:    
       print("土曜日ですの処理はしません")
       break
       #sys.exit()
    if weekday==6:    
       print("日曜日ですの処理はしません")
       #sys.exit()
       break
    """
    
    
    market = 't'#Note このtは任意かも
    kabucode=data225()
    sdg = StockDataGetter(frm, to, market)
    for num in range(len(kabucode)):
        print("for start")
        time.sleep(1)
        sdg.get_price_data(kabucode[num-1])
        time.sleep(1)
        
        print("for end")
    #すべてのデータ一覧出力
    sdg.alldatawrite()
    count=count+1
    

    #------------Loop End----------------------------------- 
    #DB書き込み   
    d = dt.now().strftime('%Y%m%d')
    path_w = d+'-alldata.txt'
    
    dbwrite(csv_file_name=path_w)
    print("end")
#-------------
#Main
#--------------
def main():
    """
    # タスク実行」を出力
    schedule.every().day.at("20:00").do(
        lambda:
        job()
    )
    # タスク監視ループ
    while True:
        # 当該時間にタスクがあれば実行
        schedule.run_pending()
        # 1秒スリープ
        time.sleep(1)
    """
    job()







if __name__ == "__main__":
    main()

