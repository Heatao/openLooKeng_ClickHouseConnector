### openLooKeng_ClickHouseConnector

openLooKeng的ClickHouse插件。

openLooKeng项目地址：https://gitee.com/openlookeng/hetu-core

### 在olk项目代码中添加clickhouse模块

1. 确定openLooKeng工程版本与clickhouse模块的版本一致

```
    <parent>
        <groupId>io.hetu.core</groupId>
        <artifactId>presto-root</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </parent>
```

```
    <groupId>io.hetu.core</groupId>
    <artifactId>presto-root</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <packaging>pom</packaging>
```

2. 在openLooKeng根目录下的pom.xml中添加clickhouse模块

```
<module>hetu-clickhouse</module>
```

3. 在hetu-server/src/main/provisio/hetu.xml配置文件中注册clickhouse的connector

```
    <artifactSet to="plugin/clickhouse">
        <artifact id="${project.groupId}:hetu-clickhouse:zip:${project.version}">
            <unpack />
        </artifact>
    </artifactSet>
```

有问题和建议，欢迎联系我heatao_zj@yeah.net
