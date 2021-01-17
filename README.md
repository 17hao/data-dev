### DNS解析的问题

- 阿里云主机
  
    - 没有配置域名
    - 有一个公网IP
    - 主机名aliyun
- Zookeeper、Kafka、Hbase在同一台主机上

    - Kafka和Hbase配置的zk地址为aliyun
    
客户端通过zk的地址连接Kafka和Hbase时，需要在本地hosts文件中添加阿里云主机和ip地址的映射