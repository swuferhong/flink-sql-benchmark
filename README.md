# flink-sql-benchmark

## Flink TPC-DS benchmark 标准流程

### Step 1: 环境准备

- 集群 配置推荐
  - 阿里云 EMR 集群
    - 主节点 *1: ecs.g7.8xlarge / vCPU 32 核 128 GiB / 系统盘: 120GB*1
      数据盘: 80GB*1
    - 核心实例组 *15: ecs.d2s.20xlarge / vCPU 80 核 352 GiB / 系统盘: 120GB*1
      数据盘: 7300GB*30
    - 服务选配时: 勾选Hadoop、Hive. 本文档测试的 Hadoop 版本为2.7.5, Hive 版本为 3.1.2
    - 配置:
      - Yarn 配置:
        - 修改 yarn-site 中 classpath 配置, 增加 mapreduce 相关类到 classpath 中
          - `$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*,$HADOOP_YARN_HOME/share/hadoop/mapreduce/*`
    - 打开端口
      - 首先集群购买时需要配置公网 IP
      - 安全组开通 8433、22、8888 端口访问, 注意配置网段, 不要开通 0.0.0.0/32 的访问权限
      - Web 访问需要在阿里云 EMR 的控制台 用户管理 中添加一个新用户, 并配置密码, 在 访问链接与端口 中可访问 Yarn Web 界面
  - 自建集群
    - 推荐配置: 参考上述阿里云 EMR 集群配置
    - Hadoop 环境准备
      - 安装 Hadoop(2.7.5), 并配置好 HDFS
      - 配置 Hadoop 环境变量: `${HADOOP_HOME}` 和 `${HADOOP_CLASSPATH}`
      - 启动 Hadoop, 验证 HDFS 是否可以访问
      - 配置 Yarn， 并启动 ResourceManager 和 NodeManager:
        - 修改 yarn-site 中 classpath 配置, 增加 mapreduce 相关类到 classpath 中
          - `$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*,$HADOOP_YARN_HOME/share/hadoop/mapreduce/*`
    - Hive 环境
      - 开启 Hive 的 metastore 和 Hive 的 HiveServer2 服务
        - 开启命令 `nohup hive --service metastore &` 和 `nohup hive --service hiveservice2 &`
        - 配置环境变量 `${HIVE_CONF_DIR}` 指向 Hive conf 安装目录
      - 集群需安装 `gcc`. Hive 原始数据生成时会使用
- TPC-DS Benchmark 工程准备
  - 下载 TPC-DS flink-sql-benchmark 工程
    - 方法1: download jar 包
      - 暂不支持
    - 方法2: clone github 工程，并自己打包
      - 工程 clone:
        - 内部: `git clone git@gitlab.alibaba-inc.com:ververica/flink-sql-benchmark.git`
        - 开源: `git clone https://github.com/ververica/flink-sql-benchmark.git`
        - 开源版本较久未更新，现在建议使用内部版本
      - 工程打包
        - 工程目录下执行： `mvn clean package -DskipTests`
          - 如果在集群上打包，需要集群安装 Maven
      - 工程上传
        - 如果不在集群上打包，需使用 scp 命令将 TPC-DS Benchmark 工程拷贝到集群的指定目录中

- Flink 环境
  - 下载对应版本 Flink 到 TPC-DS Benchmark 工程的 `packages` 目录下
    - 下载地址: `https://flink.apache.org/downloads.html`, 下载对应版本的 Flink， 本文档测试的 Flink 版本为 Flink1.16
    - 这里将 Flink 安装在 `packages` 目录下， 可以避免修改环境变量，与其他冲突，推荐放到 `packages` 目录下
  - 替换推荐配置文件
    - 使用TPC-DS工程目录下 `tools/flink/flink-conf.yaml` 替换 `${FLINK_HOME}/conf/flink-conf.yaml`
  - 上传 `flink-sql-connector-hive包`
    - 下载对应版本的 `flink-sql-connector-hive` 放到 `${FLINK_HOME}/lib/` 下
      - 例如： `flink-1.15.0` 对应的 `connector` 包下载地址: `https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.2_2.12/1.15.0/flink-sql-connector-hive-3.1.2_2.12-1.15.0.jar`

### Step 2: TPC-DS 原始数据生成

- 设置环境信息
  - cd 到 TPC-DS 工程下的 `tools/common` 目录下， 修改 env.sh, 修改其中的各项信息
    - 非 Partition 表
      - `SCALE` 对应生成数据集大小，单位 GB. 推荐设置为 10000 (10TB)
      - `FLINK_TEST_DB` 对应生成的 Hive 外表的 Database name. 该 Database 提供给 Flink 使用.
        - 使用默认的即可: `export FLINK_TEST_DB=tpcds_bin_orc_$SCALE`
    - Partition 表
      - `SCALE` 对应生成数据集大小，单位 GB. 推荐设置为 10000 (10TB)
      - `FLINK_TEST_DB`
      - 修改为: `export FLINK_TEST_DB=tpcds_bin_partitioned_orc_$SCALE`
- 数据生成
  - 非 Partition 表数据生成
    - cd `tools/datagen`; 运行 `./init_hive_db.sh`， 生成原始数据和对应的 Hive 外表
      - 生成的原始数据会存在 HDFS 的 `/tmp/tpcds-generate/${SCALE}` 目录下
      - Hive 中会生成指向原始数据的数据库 `tpcds_text_${SCALE}`
      - Hive 中会生成 Flink 使用的 orc 格式外表数据库 `tpcds_bin_orc_${SCALE}`
  - Partition 表数据生成
    - cd `tools/datagen`; 运行 `./init_partition_db.sh`， 生成原始数据和对应的 Hive 外表
    - 生成的原始数据会存在 HDFS 的 `/tmp/tpcds-generate/${SCALE}` 目录下
    - Hive 中会生成指向原始数据的数据库 `tpcds_text_${SCALE}`
    - Hive 中会生成 Flink 使用的 orc 格式的多 partition 外表数据库 `tpcds_bin_partitioned_orc_${SCALE}`
      - 这一步因为涉及到 hive 动态分区的数据写入，时间较长，请耐心等待

### Step 3: TPC-DS 统计信息生成
- 统计信息生成:
  - 生成各表的统计信息： cd `tools/stats`; 运行 `./init_stats.sh`

### Step 4: Flink 执行 TPC-DS Queries

- 单 query 验证: cd `tools/flink`; 运行 `./run_query.sh q1.sql 1`
  - 其中， `q1.sql` 代表的是执行的 query 序号， 取值为1-99; `1` 为运行的 iterator 轮数，建议取值为 2
  - 当 iterator 轮数取值为 `1` 时， 由于 Flink 集群的预热时间，会导致运行时间大于实际 query 执行时间
- 所有 query 执行: cd `tools/flink`; 运行 `./run_all_queries.sh 2`
  - 由于 run all query 时间较长。 所以可以采用后台运行的方式，命令为: `nohup ./run_all_queries.sh 2 > partition_result.log 2>&1 &`
- 可选参数： `--warmup`; 加快执行时间
  - 例: `nohup ./run_all_queries.sh 2 warmup > partition_result.log 2>&1 &`


## 其他系统运行 TPC-DS

### Spark TPC-DS benchmark 流程

#### Step 1: 环境准备

- 集群配置、TPC-DS 工程准备、Hadoop 环境准备、Hive 环境 与 `### Step 1: 环境准备` 相同
- 下载 Spark 到 TPC-DS Benchmark 工程的 `packages` 目录下
  - 下载地址 `https://spark.apache.org/downloads.html`
  - 本文档测试的 Spark 版本为 3.2.1
- Spark 环境准备
  - copy 集群 Hive 的 `hive-site.xml` 到 `${SPARK_HOME}/lib` 下

#### Step 2: TPC-DS 原始数据生成

- 设置环境信息
  - cd 到 TPC-DS 工程下的 `tools/common` 目录下， 修改 env.sh, 修改其中 spark 的信息
    - 修改通用的 spark 属性信息
      - `export SPARK_HOME=${SPARK_INSTALL_PATH}/spark-3.2.1`
      - `export SPARK_CONF_DIR=$SPARK_HOME/conf`
      - `export SPARK_BEELINE_SERVER="jdbc:hive2://xxxx:10001"`
        - spark 统计信息生成时需要该属性
    - 非 Partition 表
      - `SCALE` 对应生成数据集大小，单位 GB. 推荐设置为 10000 (10TB)
      - `SPARK_TEST_DB` 对应生成的 Hive 外表的 Database name. 该 Database 提供给 Spark 使用.
        - 使用默认的即可: `export SPARK_TEST_DB=spark_tpcds_bin_orc_$SCALE`
    - Partition 表
      - `SCALE` 对应生成数据集大小
      - `SPARK_TEST_DB`
        - 修改为: `export FLINK_TEST_DB=spark_tpcds_bin_partitioned_orc_$SCALE`
- 数据生成
  - 非 Partition 表数据生成
    - cd `tools/datagen`; 运行 `./spark_init_hive_db.sh`， 生成原始数据和对应的 Hive 外表
  - Partition 表数据生成
    - cd `tools/datagen`; 运行 `./spark_init_partition_db.sh`， 生成原始数据和对应的 Hive 外表

#### Step 3: TPC-DS 统计信息生成

- 统计信息生成:
  - 非 Partition 表
    - 生成各表的统计信息： cd `tools/stats`; 运行 `./collect_spark_stats.sh`
  - Partition 表
    - 生成各表的统计信息： cd `tools/stats`; 运行 `./collect_spark_stats.sh`
      - 暂定: 后面要增加参数，让spark analyze table 加入 partition 属性

#### Step 4: Spark 执行 TPC-DS Queries

- 环境准备好后， 运行单query: cd `tools/spark`; 执行 `./run_query.sh q1.sql 1`
- 运行所有query: cd`tools/spark`; 执行 `./run_all_query.sh 2`
  - spark结果打印在am的stdout，需要手动到yarn界面复制相关信息来分析结果