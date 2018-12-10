# Spark ETL
## 数据
就以简单的用户信息作为测试，见data/user.csv
## 开发注意
1. 数据库连接配置写在resources/db.properties文件中
2. readWriters包中写每一种数据源的读取和写入操作， 每一种数据源写出加载时需要的参数 和 输出时需要的参数
3. 看每种数据源是否能包装成DatasourceOptions的形式，参考JDBC实现JDBCDatasource.scala，如果不行，尝试一下修改接口或者告诉我