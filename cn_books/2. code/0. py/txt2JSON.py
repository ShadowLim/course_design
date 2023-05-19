import os, json


with open("E:\Store\Desktop\\tb_book.txt", encoding='utf-8') as file :
    lines = file.readlines()
    tolist = list()
    for data in lines :
        res = {}
        data = data.strip("\n")
        str = data.split("\t")
        res['id'] = str[0]
        res['type'] = str[1]
        res['name'] = str[2]
        res['author'] = str[3]
        res['price'] = str[4]
        res['discount'] = str[5]
        res['pub_time'] = str[6]
        res['pricing'] = str[7]
        res['publisher'] = str[8]
        res['crawler_time'] = str[9]
        tolist.append(res)

with open("E:\\Store\\Desktop\\tb_book_out.txt", "w", encoding='utf-8') as outFile :
    jsonData = json.dumps(tolist)
    for row in jsonData :
        outFile.write(row)