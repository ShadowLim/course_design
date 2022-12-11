-------------------------------------------------------
with t1 as (
select
		id,
		city,
		locate("层", houseInfo) idx1,
		locate("建", houseInfo) idx2,
		substring(houseInfo, locate("层", houseInfo) + 1, length(houseInfo)) as tmpStr,
		locate("层", substring(houseInfo, locate("层", houseInfo) + 1, length(houseInfo))) idx3,
		locate("建", houseInfo) - 2 - (locate("层", houseInfo) + 2) + 1 len,
		substring(houseInfo, locate("层", substring(houseInfo, locate("层", houseInfo) + 1, length(houseInfo))) + 2 + locate("层", houseInfo),
		locate("建", houseInfo) - 2 - (locate("层", substring(houseInfo, locate("层", houseInfo) + 1, length(houseInfo))) + 2) - 1) as createTime
from tb_house
),
t2 as (
select id, city, 2022 - createTime as years, count(createTime), createTime
from t1
where createTime not like '%透露%'
group by id, city, createTime
),
t3 as (
select city, years, count(years) size
from t2
group by city, years
order by city, years desc
)

-- 上海	2862
-- 北京	1950
-- 广州	2015
-- 深圳	2863
select city, sum(size)
from t3
group by city

-- 上海	小于10年	195
-- 北京	小于10年	196
-- 广州	小于10年	405
-- 深圳	小于10年	758
select city, '小于10年' as yearrange, sum(size)
from t3
where years < 10
group by city


-- 上海	10~15年	398
-- 北京	10~15年	302
-- 广州	10~15年	371
-- 深圳	10~15年	534
select city,  '10~15年' as yearrange, sum(size)
from t3
where years >= 10 and years < 15
group by city


-- 上海	15~20年	430
-- 北京	15~20年	465
-- 广州	15~20年	344
-- 深圳	15~20年	782
select city,  '15~20年' as yearrange, sum(size)
from t3
where years >= 15 and years < 20 
group by city

-- 上海	20~30年	1147
-- 北京	20~30年	551
-- 广州	20~30年	792
-- 深圳	20~30年	658
select city,  '20~30年' as yearrange, sum(size)
from t3
where years >= 20 and years < 30 
group by city

-- 上海	30~40年	551
-- 北京	30~40年	329
-- 广州	30~40年	94
-- 深圳	30~40年	129
select city,  '30~40年' as yearrange, sum(size)
from t3
where years >= 30 and years < 40 
group by city


-- 上海	40~50年	119
-- 北京	40~50年	99
-- 广州	40~50年	7
-- 深圳	40~50年	2
select city,  '40~50年' as yearrange, sum(size)
from t3
where years >= 40 and years <= 50 
group by city

-- 上海	超过50年	22
-- 北京	超过50年	8
-- 广州	超过50年	2
select city, '超过50年' as yearrange, sum(size)
from t3
where years > 50
group by city

--------------------------------------------------------------------------
-- 关注数的最值
with t1 as (
select city, locate("人", followInfo) as idx,
SUBSTRING(followInfo, 1, locate("人", followInfo) - 1)as FollowSize
from tb_house
)

select city,
min(FollowSize), max(FollowSize)
from t1
group by city;

