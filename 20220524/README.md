## Spark SQL

### 기본 세팅

```powershell
# hadoop(hdfs, yarn) 실행
$ /kikang/hadoop3/sbin/start-all.sh (hdfs 실행: start-dfs.sh / yarn 실행: start-yarn.sh)
(hdfs web ui) http://spark-master-01:50170/
(yarn web ui) http://spark-master-01:8188/

# spark(standalone, history server) 실행
$ /kikang/spark3/sbin/start-all.sh
(standalone web ui) http://spark-master-01:8180/
$ /kikang/spark3/sbin/start-history-server.sh
(history server web ui) http://spark-master-01:18180/

# zeppelin(web notebook) 실행
$ /kikang/zeppelin-0.10.0-bin-all/bin/zeppelin-daemon.sh restart
(zeppelin notebook web ui) http://spark-master-01:9090/
"								
```



### 기본 구조

1. 정형/반정형file (csv, json, avro, )
2. 정형 noSQL(MongoDB, Redis, HBase)
3. 정형 DW(Hive, Spark DW)
4. 비정형 RDD: 텍스트 파일을 읽기 위한 용도
5. RDB를 읽기 위한 Dataframe:

### 실행방법

1. 순수 Jupyter Notebook 기준

- [참고 URL](https://spark.apache.org/docs/latest/sql-getting-started.html) (Spark 공식문서-Getting Started)

```shell
spark@spark-master-01:~$ jupyter notebook --ip 0.0.0.0 --port 9998
```

```python
! sudo pip install pyspark
sc #SparkContext
from pyspark.sql import SparkSession #SpakSession
spark = SparkSession \
    .builder \
    .getOrCreate()
```

<img width="203" alt="image" src="https://user-images.githubusercontent.com/64065318/169928381-3784bd20-a293-48fa-ad69-71981259a5c8.png">

2. Command line에서 PySpark

```shell
$ PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS='notebook --ip 0.0.0.0 --port 9999' YARN_CONF_DIR=/kikang/spark3/conf2 /kikang/spark3/bin/pyspark --master yarn --executor-memory 6G --executor-cores 2 --num-executors 3 --name PySparkShell_YARN
```

- 1과 차이점: SparkSession이나 SparkContext를 new로 만들 필요가 없음

  3. Zeppeline

     ```python
     %pyspark
     
     #로우데이터
     df_raw = spark \
         .read \
         .text("/kikang/data/airline_on_time")
     
     df_raw.show(5, truncate=False)
     
     #데이터 로딩
     df = spark \
         .read \
         .csv("/kikang/data/airline_on_time",
              header = True,
              inferSchema = True)
     
     #데이터 스키마 확인
     df.printSchema()
     
     #데이터 컬럼 개수 확인
     print(df.columns)
     print(len(df.columns))
     
     #데이터 건수 확인
     df.count()
     
     #데이터 출력
     df.show(100)
     
     #도표로 데이터 출력
     z.show(df)
     
     #데이터 분포 확인 - 전체
     df.describe().show()
     
     #데이터 분포 확인 - 컬럼 지정
     df.select(
         "ActualElapsedTime",
         "AirTime", 
         "DepDelay", 
         "ArrDelay"
     ).describe().show()
     
     #데이터 분포 확인 - 컬럼 지정, 타입 변환
     df.select(
         df.ActualElapsedTime.cast('int'),
         df.AirTime.cast('int'), 
         df.DepDelay.cast('int'),
         df.ArrDelay.cast('int')
     ).describe().show()
     ```

    - 예제문제 (1. PYSPARK, 2. SQL ver)

      1. 데이터에 포함된 항공사 목록

      ```python
      #pyspark
      %pyspark
      from pyspark.sql.functions import asc
      z.show(df.select("UniqueCarrier").distinct().orderBy(asc("UniqueCarrier"))) # .show(100)
      ```

      ```sql
      #sql
      %sql
      select
          distinct UniqueCarrier
      from
          df
      order by UniqueCarrier asc
      ```

      2. 항공사별 비행 횟수

      ```python
      #pyspark
      %pyspark
      from pyspark.sql.functions import count
      
      df_temp2 = df.groupBy("UniqueCarrier").agg(count("*").alias("flight_count"))
      
      df_temp2.show()
      z.show(df_temp2)
      ```

      ```sql
      #sql
      %sql
      select
          UniqueCarrier,
          count(*) as flight_count
      from
          df
      group by UniqueCarrier
      ```

      3. 항공사별 계획된 비행 횟수 vs 실제 비행 횟수 vs 취소된 비행 비율(%)

      ```python
      #pyspark
      %pyspark
      from pyspark.sql.functions import count, sum, when, col, round
      
      df.groupBy("UniqueCarrier") \
        .agg(count("*").alias("flight_count"), 
             sum("Cancelled").alias("flight_cancel_count"),
             sum(when(col("Cancelled") == 0, 1).otherwise(0)).alias("flight_real_count"),
             (sum(when(df.Cancelled == 1, 1).otherwise(0)) + sum(when(df.Cancelled == 0, 1).otherwise(0))).alias("flight_total_count")) \
        .withColumn("flight_gap", col("flight_count") - col("flight_total_count")) \
        .withColumn("cancel_rate(%)", round(col("flight_cancel_count") / col("flight_count") * 100, 1)) \
        .orderBy(col("cancel_rate(%)").desc(), col("flight_cancel_count").desc(), col("flight_count").desc()) \
        .show(100)
      ```

      ```sql
      #sql
      %sql
      select
          *,
          (flight_count - flight_total_count) as flight_gap,
          round(flight_cancel_count / flight_count * 100, 1) as `cancel_rate(%)` # 취소율 계산
      from
      (
      select
          UniqueCarrier,
          count(*) as flight_count,
          sum(Cancelled) as flight_cancel_count, # cancelled 값이 0 or 1
          sum(case when Cancelled = 0 then 1 else 0 end) as flight_real_count,
          sum(case when Cancelled = 1 then 1 else 0 end) + sum(case when Cancelled = 0 then 1 else 0 end) as flight_total_count
      from 
          df
      group by UniqueCarrier
      )
      order by `cancel_rate(%)` desc, flight_cancel_count desc, flight_count desc
      ```

      4. 붐비는 공항(출발, 도착이 많은 공항)

      ```python
      %pyspark
      from pyspark.sql.functions import *
      
      
      # 출발지 공항별 횟수....
      df_origin = df.groupBy("Origin") \
          .count() \
          .select("Origin", col("count").alias("origin_count"))
        
      #df_origin.show()
      #print(df_origin.count())
      
      
      # 도착지 공항별 횟수....
      df_dest = df.groupBy("Dest") \
          .count() \
          .select("Dest", col("count").alias("dest_count"))
        
      #df_dest.show()
      #print(df_dest.count())
      
      
      # 조인.... 출발지/도착지 공항별 횟수.... full outer 조인....
      df_origin_dest = df_origin \
          .join(df_dest, df_origin.Origin == df_dest.Dest, "full_outer")
      
      #df_origin_dest.show()
      #print(df_origin_dest.count())
      
      
      # 공항별 출발/도착 총 횟수....
      df_origin_dest_total = df_origin_dest \
          .select("Origin",
                  "Dest",
                  "origin_count",
                  "dest_count",
                  (df_origin["origin_count"] + df_dest["dest_count"]).alias("total_count")) \
          .orderBy(desc("total_count"))
      
      df_origin_dest_total.show(1000)
      #print(df_origin_dest_total.count())
      ```

      ```sql
      %sql
      
      select
          Origin,
          Dest,
          origin_count,
          dest_count,
          (df_origin.origin_count + df_dest.dest_count) as total_count
      from
      
      (
      select
          Origin,
          count(*) as origin_count
      from 
          df
      group by Origin
      ) as df_origin
      
      full outer join
      
      (
      select 
          Dest,
          count(*) as dest_count
      from 
          df
      group by Dest
      ) as df_dest
      
      on df_origin.Origin = df_dest.Dest
      
      order by total_count desc
      ```

       