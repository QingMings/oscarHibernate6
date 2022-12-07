# oscar 神通数据库 hibernate6.1.5 方言适配
## 神通数据库版本7.1
## hibernate 版本 6.1.5Final

## 使用方式

配置 hibernate 方言即可


```yaml
##  application.yml
spring:
  jpa:
    hibernate:
      ddl-auto: create-drop
    show-sql: false
    properties:
      hibernate:
        dialect: cn.qingmings.oscar.OscarDialect
        format_sql: true
```