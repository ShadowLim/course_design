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
  <p><b>Talk is cheap, show me the code!</b></p>
</div>

<div class="topnav">
  <a href="http://localhost:8080/NCJob/index.jsp" style="float:left">首页</a>
  <a href="http://localhost:8080/NCJob/CityPosCount.html">岗位数量统计分析</a>
  <a href="http://localhost:8080/NCJob/edu.html">学历要求统计分析</a>
  <a href="http://localhost:8080/NCJob/salary.html">薪资统计分析</a>
  <a href="http://localhost:8080/NCJob/feedback.html" style="float:right">反馈评估</a>
</div>

<h3 align="center">一线大厂的岗位情况</h3>
<h4 align="center">此处统计的是字节跳动、阿里巴巴、华为、百度、美团</h4>

<div style="width: 200px;height:80px; margin: 0px auto;">
  <a href="idx1.jsp">
    <input type="submit" value="实习一线大厂的岗位情况">
  </a><br><br>
  <a href="idx2.jsp">
    <input type="submit" value="非实习一线大厂的岗位情况">
  </a><br><br>
</div>



<div class="footer">
  <p>
    Copyright@lbj_fosu | 粤ICP备********号
  </p>
</div>

</body>
</html>




