<%@ page contentType="text/html;charset=UTF-8" language="java" %>

<head>
    <meta charset="utf-8">
    <title>NowCoder招聘岗位可视化统计分析平台</title>
    <script src="static/echarts/echarts.min.js"></script>
    <style>
        * {
            box-sizing: border-box;
        }

        body {
            font-family: Arial;
            padding: 10px;
            background: #f1f1f1;
        }

        /* 头部标题 */
        .header {
            padding: 30px;
            text-align: center;
            background: white;
        }

        .header h1 {
            font-size: 50px;
        }

        /* 导航条 */
        .topnav {
            overflow: hidden;
            background-color: #96bef0;
            text-indent: 175px;    /* 导航栏选项之间的间距  */
            display: inline-block;
            text-align: center;
        }

        /* 导航条链接 */
        .topnav a {
            float: left;
            display: inline-block;
            color: #f2f2f2;
            text-align: center;
            padding: 14px 16px;
            text-decoration: none;
            text-align: center;
        }

        /* 链接颜色修改 */
        .topnav a:hover {
            background-color: #ddd;
            color: black;
        }

        /* 创建两列 */
        /* Left column */
        .leftcolumn {
            float: left;
            width: 75%;
        }

        /* 右侧栏 */
        .rightcolumn {
            float: left;
            width: 25%;
            background-color: #f1f1f1;
            padding-left: 0px;
        }

        /* 左栏图像部分 */
        .leftfakeimg {
            background-color: #aaa;
            width: 100%;
            padding: 0px;
        }

        /* 右栏图像部分 */
        .rightfakeimg {
            background-color: #aaa;
            width: 20%;
            padding: 0px;
            float: left;
            height: 80%;
        }

        /*右栏文章介绍 */
        .intro {
            float:right;
            width:60%;
        }

        /* 文章卡片效果 */
        .card {
            background-color: white;
            padding: 20px;
            margin-top: 20px;
        }

        /* 热门文章卡片效果 */
        .cpcard {
            background-color: white;
            padding: 20px;
            margin-top: 20px;
            height: 80px;
        }

        /* 列后面清除浮动 */
        .row:after {
            content: "";
            display: table;
            clear: both;
        }

        /* 底部 */
        .footer {
            padding: 20px;
            text-align: center;
            background: #ddd;
            margin-top: 20px;
        }

        /* 响应式布局 - 屏幕尺寸小于 800px 时，两列布局改为上下布局 */
        @media screen and (max-width: 800px) {
            .leftcolumn, .rightcolumn {
                width: 100%;
                padding: 0;
            }
        }

        /* 响应式布局 -屏幕尺寸小于 400px 时，导航等布局改为上下布局 */
        @media screen and (max-width: 400px) {
            .topnav a {
                float: none;
                width: 100%;
            }
        }
    </style>
</head>

<body>
<div class="header">
    <h1>NowCoder招聘岗位可视化统计分析平台</h1>
    <p><b>实习一线大厂岗位统计分析</b></p>
</div>

<div class="topnav">
    <a href="http://localhost:8080/NCJob/index.jsp" style="float:left">首页</a>
    <a href="http://localhost:8080/NCJob/CityPosCount.html">岗位数量统计分析</a>
    <a href="http://localhost:8080/NCJob/edu.html">学历要求统计分析</a>
    <a href="http://localhost:8080/NCJob/salary.html">薪资统计分析</a>
    <a href="http://localhost:8080/NCJob/feedback.html" style="float:right">反馈评估</a>
</div>

<p><b><c>实习一线大厂的岗位情况</c></b></p>
<div class="row">
    <!-- 为ECharts准备一个具备大小（宽高）的Dom -->
    <div id="main" style="width: 1200px;height:600px; margin: 0px auto;"></div>
    <script type="text/javascript">
        // 基于准备好的dom，初始化echarts实例
        var myChart = echarts.init(document.getElementById('main'));
        // 指定图表的配置项和数据
        var option;
        setTimeout(function () {
            option = {
                legend: {},
                tooltip: {
                    trigger: 'axis',
                    showContent: false
                },
                dataset: {
                    source: [
                        ['一线大厂','开发岗', '算法岗', '数据岗', '工程师', '策划岗', '设计岗', '运营岗', '培训岗', '其他'],
                        ['字节跳动', 2345,631,858,105,15,0,1215,0,1034],
                        ['华为', 88, 0, 15, 0, 0, 0, 0, 0, 15],
                        ['阿里巴巴', 0, 15, 0, 15, 0, 0, 15, 0, 64],
                        ['百度', 15, 15, 0, 31, 0, 0, 0, 0, 45],
                        ['美团', 626, 225, 183, 285, 0, 210, 272, 75, 701]
                    ]
                },
                xAxis: { type: 'category' },
                yAxis: { gridIndex: 0 },
                grid: { top: '55%' },
                series: [
                    {
                        type: 'line',
                        smooth: true,
                        seriesLayoutBy: 'row',
                        emphasis: { focus: 'series' }
                    },
                    {
                        type: 'line',
                        smooth: true,
                        seriesLayoutBy: 'row',
                        emphasis: { focus: 'series' }
                    },
                    {
                        type: 'line',
                        smooth: true,
                        seriesLayoutBy: 'row',
                        emphasis: { focus: 'series' }
                    },
                    {
                        type: 'line',
                        smooth: true,
                        seriesLayoutBy: 'row',
                        emphasis: { focus: 'series' }
                    },
                    {
                        type: 'line',
                        smooth: true,
                        seriesLayoutBy: 'row',
                        emphasis: { focus: 'series' }
                    },
                    {
                        type: 'line',
                        smooth: true,
                        seriesLayoutBy: 'row',
                        emphasis: { focus: 'series' }
                    },
                    {
                        type: 'line',
                        smooth: true,
                        seriesLayoutBy: 'row',
                        emphasis: { focus: 'series' }
                    },
                    {
                        type: 'line',
                        smooth: true,
                        seriesLayoutBy: 'row',
                        emphasis: { focus: 'series' }
                    },
                    {
                        type: 'line',
                        smooth: true,
                        seriesLayoutBy: 'row',
                        emphasis: { focus: 'series' }
                    },
                    {
                        type: 'pie',
                        id: 'pie',
                        radius: '20%',
                        center: ['50%', '25%'],
                        emphasis: {
                            focus: 'self'
                        },
                        label: {
                            formatter: '{b}: {@开发岗} ({d}%)'
                        },
                        encode: {
                            itemName: '一线大厂',
                            value: '开发岗',
                            tooltip: '开发岗'
                        }
                    }
                ]
            };
            myChart.on('updateAxisPointer', function (event) {
                const xAxisInfo = event.axesInfo[0];
                if (xAxisInfo) {
                    const dimension = xAxisInfo.value + 1;
                    myChart.setOption({
                        series: {
                            id: 'pie',
                            label: {
                                formatter: '{b}: {@[' + dimension + ']} ({d}%)'
                            },
                            encode: {
                                value: dimension,
                                tooltip: dimension
                            }
                        }
                    });
                }
            });
            // 使用刚指定的配置项和数据显示图表。
            myChart.setOption(option);
        });
    </script>
</div>

<div class="footer">
    <p>
        Copyright@lbj_fosu | 粤ICP备********号
    </p>
</div>

</body>
</html>
