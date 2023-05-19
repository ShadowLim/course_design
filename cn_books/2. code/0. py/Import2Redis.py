# import redis
# import csv
# pool = redis.ConnectionPool(host = 'localhost', port = 6379, decode_responses = True)
# r = redis.Redis(connection_pool = pool)
# with open('/home/lbj/windowsUpload/data/tb_book.csv', 'r', encoding='utf-8') as file :
#     count = 0
#     reader = csv.reader(file)
#     for item in reader:
#         if count >= 15000:
#             break
#         key = item[0]
#         r.hmset(key, {
#             "id": item[0],
#             "type": item[1],
#             "name":str(item[2]),
#             "author": item[3],
#             "price": item[4],
#             "discount": item[5],
#             "pub_time": item[6],
#             "pricing": item[7],
#             "publisher": item[8],
#             "crawler_time": item[9]
#         })
#         print(r.hget(key, "id"))
#         count = count + 1
#     print('finsh!')


import redis
import csv
pool = redis.ConnectionPool(host = 'localhost', port = 6379, decode_responses = True)
r = redis.Redis(connection_pool = pool)
with open('/home/lbj/windowsUpload/data/tb_book.csv', 'r', encoding='utf-8') as file :
    count = 0
    reader = csv.reader(file)
    for item in reader:
        if count >= 15000:
            break
        r.hset(item[0], "id", item[0])
        r.hset(item[0], "type", item[1])
        r.hset(item[0], "name", str(item[2]))
        r.hset(item[0], "author", item[3])
        r.hset(item[0], "price", item[4])
        r.hset(item[0], "discount", item[5])
        r.hset(item[0], "pub_time", item[6])
        r.hset(item[0], "pricing", item[7])
        r.hset(item[0], "publisher", item[8])
        r.hset(item[0], "crawler_time", item[9])
        print(r.hget(item[0], "id"))
        count = count + 1
    print('finsh!')
