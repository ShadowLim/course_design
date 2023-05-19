create database if not exists db_ke_house;
use db_ke_house;

create table if not exists tb_ke_house (
id int,
city string,
title string,
houseInfo array<string>,
followInfo array<string>,
positionInfo string,
total int,
unitPrice double,
tag string,
crawler_time timestamp
)
row format delimited fields terminated by '\t'
collection items terminated by '|';

load data inpath '/Ke_House/tb_house.txt' into table tb_ke_house;
------------------------------------------------------------------------------
with t1 as (
select city, count(city) totalcnt
from tb_ke_house
where array_contains(houseinfo, "未透露年份建") = false
group by city
),
t2 as (
select city, 2022 - cast(substring(houseinfo[2],0,length(houseinfo[2])-2) as int) as ycnt 
from tb_ke_house 
where houseinfo[2] != "未透露年份建"
),
t3 as (
select city, count(ycnt) as cnt
from t2
where ycnt >= 20
group by city
)
select t1.city, cnt / totalcnt rate
from t1 join t3
on t1.city = t3.city
group by t1.city, cnt, totalcnt;

上海    0.6425576519916143
北京    0.5061538461538462
广州    0.4441687344913151
深圳    0.27558505064617533


------------------------------------------------------------------------------
select city,layer, avg(total/size) from 
(
select city,houseinfo[0] layer,cast(substring(houseinfo[4],0,length(houseinfo[4])-2) as int) as size, 
total from tb_ke_house 
) t
group by city,layer;

上海    中楼层  6.558067143940795
上海    低楼层  6.526130548560414
上海    高楼层  6.413051004426704
北京    中楼层  6.793251767738759
北京    低楼层  6.834817416267729
北京    地下室  7.8998778998779
北京    底层    6.896333047472122
北京    顶层    7.077389474267064
北京    高楼层  6.962124638725149
广州    中楼层  3.7183773994723497
广州    低楼层  3.8123485536451747
广州    地下室  3.2927350427350426
广州    高楼层  3.800820253577766
深圳    中楼层  6.039007961759094
深圳    低楼层  6.085307049389125
深圳    高楼层  6.167436216151663
Time taken: 15.819 seconds, Fetched: 16 row(s)
------------------------------------------------------------------------------
select city, cnt1 / totalcnt 采光比率, 
cnt2 / totalcnt 近地铁比率,
cnt3 / totalcnt 电梯比率,
cnt4 / totalcnt 精装比率,
cnt5 / totalcnt 景观比率,
cnt6 / totalcnt 近公园比率
from 
(
select city, sum(if (title regexp('.*采光.*') = true, 1, 0)) as cnt1, 
sum(if (title regexp('.*地铁.*') = true, 1, 0) + if (title regexp('.*号线.*') = true, 1, 0) + if (title regexp('.*公交.*') = true, 1, 0)) as cnt2, 
sum(if (title regexp('.*电梯.*') = true, 1, 0)) as cnt3, 
sum(if (title regexp('.*精装.*') = true, 1, 0)) as cnt4, 
sum(if (title regexp('.*景观.*') = true, 1, 0)) as cnt5, 
sum(if (title regexp('.*公园.*') = true, 1, 0)) as cnt6, 
count(city) as totalcnt
from tb_ke_house
group by city
) t
group by city;


select city, cnt1 / totalcnt '采光比例', 
cnt2 / totalcnt '交通便利',
cnt3 / totalcnt '电梯覆盖率',
cnt4 / totalcnt '精装房',
cnt5 / totalcnt '景观好',
cnt6 / totalcnt '近公园/社区'
from 
(
select city, sum(if (title regexp('.*采光.*') = true, 1, 0)) cnt1, 
sum(if (title regexp('.*地铁.*') = true, 1, 0) + if (title regexp('.*号线.*') = true, 1, 0) + if (title regexp('.*公交.*') = true, 1, 0)) cnt2, 
sum(if (title regexp('.*电梯.*') = true, 1, 0)) cnt3, 
sum(if (title regexp('.*精装.*') = true, 1, 0)) cnt4, 
sum(if (title regexp('.*景观.*') = true, 1, 0)) cnt5, 
sum(if (title regexp('.*公园.*') = true, 1, 0) + if (title regexp('.*社区.*') = true, 1, 0)) cnt6, 
count(city) totalcnt
from tb_house
group by city
) t
group by city;


HQL:

with t1 as
(
select city, sum(if (title regexp('.*采光.*') = true, 1, 0)) cnt1
from tb_ke_house
group by city
),
t2 as (
select city, count(city) totalcnt
from tb_ke_house
group by city
)
select t1.city, cnt1, totalcnt, cnt1/totalcnt rate1
from t1 join t2
on t1.city = t2.city
group by t1.city, cnt1, totalcnt;


上海    0.21335559265442405
北京    0.17799461641991923
广州    0.18006669136717302
深圳    0.13246143527833668

或者：（太多Job了（51个））
with t1 as
(
select city, sum(if (title regexp('.*采光.*') = true, 1, 0)) cnt1, 
sum(if (title regexp('.*地铁.*') = true, 1, 0) + if (title regexp('.*号线.*') = true, 1, 0) + if (title regexp('.*公交.*') = true, 1, 0)) cnt2, 
sum(if (title regexp('.*电梯.*') = true, 1, 0)) cnt3, 
sum(if (title regexp('.*精装.*') = true, 1, 0)) cnt4, 
sum(if (title regexp('.*景观.*') = true, 1, 0)) cnt5, 
sum(if (title regexp('.*公园.*') = true, 1, 0)) cnt6
from tb_ke_house
group by city
),
t2 as (
select city, count(city) totalcnt
from tb_ke_house
group by city
),
t3 as (
select t1.city, cnt1,totalcnt,cnt1/totalcnt rate1
from t1 join t2
on t1.city = t2.city
group by t1.city, cnt1,totalcnt
),
t4 as (
select t1.city, cnt2,totalcnt,cnt2/totalcnt rate2
from t1 join t2
on t1.city = t2.city
group by t1.city, cnt2,totalcnt
),
t5 as (
select t1.city, cnt3,totalcnt,cnt3/totalcnt rate3
from t1 join t2
on t1.city = t2.city
group by t1.city, cnt3,totalcnt
),
t6 as (
select t1.city, cnt4,totalcnt,cnt4/totalcnt rate4
from t1 join t2
on t1.city = t2.city
group by t1.city, cnt4,totalcnt
),
t7 as (
select t1.city, cnt5/totalcnt rate5
from t1 join t2
on t1.city = t2.city
group by t1.city, cnt5,totalcnt
),
t8 as (
select t1.city, cnt6/totalcnt rate6
from t1 join t2
on t1.city = t2.city
group by t1.city, cnt6,totalcnt
)
select t3.city, rate1, rate2, rate3, rate4, rate5, rate6 from t3, t4, t5, t6, t7, t8;
-------------------------------------------------------------------------------

select city, sizerange, count(sizerange) cnt
from 
(
select city, size
case
	when size < 100 then '<100'
	when size < 200 then '100~200'
	when size < 300 then '200~300'
	when size < 500 then '300~500'
	else 'other' 
end sizerange
from 
(
	select
		id,
		city,
		locate("厅", houseInfo) idx1,
		locate("平", houseInfo) idx2,
		locate("平", houseInfo)- 1 - locate("厅", houseInfo) - 2 + 1 len,
		substring(houseInfo, locate("厅", houseInfo) + 2,
		locate("平", houseInfo) - 1 - locate("厅", houseInfo) - 2 + 1) as size
	from tb_ke_house
	) t1
) t2
group by city, sizerange


HQL:

with t2 as
(
select city,size,
case when size < 100 then '<100'
when size < 200 then '100~200'
when size < 300 then '200~300'
when size < 500 then '300~500'
else 'other' 
end sizerange
from 
(
select
id,
city,
substring(houseInfo[4], 0, length(houseinfo[4]) - 2) as size
from tb_ke_house
) t1
),
t3 as (
select city, count(city) totalcnt
from tb_ke_house
group by city
)
select t2.city, sizerange, count(sizerange)/totalcnt as rate
from t2 join t3 on
t2.city = t3.city
group by t2.city, sizerange,totalcnt


上海    100~200 0.15158597662771287
上海    200~300 0.002671118530884808
上海    300~500 6.67779632721202E-4
上海    <100    0.8450751252086811
北京    100~200 0.1756393001345895
北京    200~300 0.0026917900403768506
北京    300~500 3.3647375504710633E-4
北京    <100    0.8213324360699865
广州    100~200 0.2578732864023712
广州    200~300 0.00889218228973694
广州    300~500 0.0014820303816228233
广州    <100    0.731752500926269
深圳    100~200 0.17069081153588195
深圳    200~300 0.003688799463447351
深圳    300~500 3.3534540576794097E-4
深圳    <100    0.8252850435949027

------------------------------------------------------------------------------
select 
	city, newTag, cnt / totalcnt as rate
from 
(
	select th.city, if(tag != '', tag, "未知标签") as newTag, count(tag) cnt, totalcnt
	from tb_house th join 
	(
		select city, count(city) totalcnt
		from tb_house
		group by city
	) t1 
	on th.city = t1.city
	group by th.city, newTag
) t2
group by city, newTag


HQL:

with t1 as
(
select city, count(city) totalcnt
from tb_ke_house
group by city
),
t2 as (
select city, count(tag) cnt, if(tag != '', tag, "未知标签") as newTag
from tb_ke_house
group by city, tag
)
select t2.city, newTag, cnt / totalcnt as rate
from t2 join t1 
on t2.city = t1.city
group by t2.city, newTag,cnt, totalcnt

上海    必看好房        0.16928213689482471
上海    房主自荐        0.16060100166944907
上海    新上    0.10350584307178631
上海    未知标签        0.5666110183639399
北京    必看好房        0.7614401076716016
北京    房主自荐        0.04744279946164199
北京    新上    0.014468371467025572
北京    未知标签        0.1766487213997308
广州    必看好房        0.13004816598740274
广州    房主自荐        0.03260466839570211
广州    新上    0.17562060022230455
广州    未知标签        0.6617265653945906
深圳    必看好房        0.09221998658618377
深圳    房主自荐        0.021797451374916163
深圳    新上    0.04627766599597585
深圳    未知标签        0.8397048960429242


---------------------------------------------------------------------------
select city, sum(size)
from 
(
select city, 
cast(substring(followinfo[0], 0, length(followinfo[0]) - 3) as int) as size
from tb_ke_house
)t
group by city



上海    15472
北京    38988
广州    8687
深圳    9723
------------------------------------------------------------------------------
x室x厅
with t1 as (
select city, count(city) totalcnt
from tb_ke_house
group by city
),
t2 as (
select city,
houseinfo[3] as scale,
count(houseinfo[3]) as size
from tb_ke_house
group by city, houseinfo[3]
)
select 
t2.city, scale, size / totalcnt as rate
from t1 join t2 on
t1.city = t2.city
group by t2.city, scale, size, totalcnt

上海    1室0厅  0.0330550918196995
上海    1室1厅  0.2036727879799666
上海    1室2厅  0.009348914858096828
上海    2室0厅  0.021368948247078464
上海    2室1厅  0.3662771285475793
上海    2室2厅  0.1666110183639399
上海    3室0厅  0.001001669449081803
上海    3室1厅  0.05342237061769616
上海    3室2厅  0.12954924874791318
上海    3室3厅  3.33889816360601E-4
上海    4室2厅  0.013689482470784642
上海    4室3厅  3.33889816360601E-4
上海    5室1厅  3.33889816360601E-4
上海    5室2厅  6.67779632721202E-4
上海    6室2厅  3.33889816360601E-4
北京    1室0厅  0.019515477792732168
北京    1室1厅  0.16150740242261102
北京    1室2厅  0.0023553162853297443
北京    2室0厅  0.002018842530282638
北京    2室1厅  0.5114401076716016
北京    2室2厅  0.06022880215343203
北京    3室0厅  0.002018842530282638
北京    3室1厅  0.15814266487213996
北京    3室2厅  0.06796769851951548
北京    3室3厅  3.3647375504710633E-4
北京    4室1厅  0.004037685060565276
北京    4室2厅  0.009421265141318977
北京    5室1厅  3.3647375504710633E-4
北京    5室2厅  6.729475100942127E-4
广州    1室0厅  0.016672841793256763
广州    1室1厅  0.058169692478695814
广州    1室2厅  0.0011115227862171174
广州    2室0厅  7.410151908114116E-4
广州    2室1厅  0.25861430159318266
广州    2室2厅  0.1344942571322712
广州    3室1厅  0.13004816598740274
广州    3室2厅  0.30344572063727304
广州    3室3厅  3.705075954057058E-4
广州    4室1厅  0.010744720266765468
广州    4室2厅  0.07187847350870692
广州    4室3厅  3.705075954057058E-4
广州    5室1厅  0.0011115227862171174
广州    5室2厅  0.010003705075954057
广州    6室2厅  0.001852537977028529
广州    7室3厅  3.705075954057058E-4
深圳    1室0厅  0.047283702213279676
深圳    1室1厅  0.10596914822266935
深圳    1室2厅  0.001676727028839705
深圳    2室1厅  0.1891348088531187
深圳    2室2厅  0.1334674714956405
深圳    3室0厅  3.3534540576794097E-4
深圳    3室1厅  0.09825620389000671
深圳    3室2厅  0.27699530516431925
深圳    4室1厅  0.018443997317236754
深圳    4室2厅  0.10663983903420524
深圳    4室3厅  6.706908115358819E-4
深圳    4室4厅  3.3534540576794097E-4
深圳    5室1厅  0.0026827632461435278
深圳    5室2厅  0.015761234071093227
深圳    5室3厅  6.706908115358819E-4
深圳    6室1厅  3.3534540576794097E-4
深圳    6室2厅  0.001006036217303823
深圳    9室1厅  3.3534540576794097E-4


------------------------------------------------------------------------------
东 西 南 北
东南 西南 
东北 西北 

--------------------------------------------------------------------