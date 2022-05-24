## Spark SQL

- 기본 구조

1. 정형/반정형file (csv, json, avro, )
2. 정형 noSQL(MongoDB, Redis, HBase)
3. 정형 DW(Hive, Spark DW)
4. 비정형 RDD: 텍스트 파일을 읽기 위한 용도
5. RDB를 읽기 위한 Dataframe:

- Jupyter Notebook
    - [참고 URL](https://spark.apache.org/docs/latest/sql-getting-started.html) (Spark 공식문서-Getting Started)

```shell
spark@spark-master-01:~$ jupyter notebook --ip 0.0.0.0 --port 9998
```

```python
! sudo pip install pyspark
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .getOrCreate()
```

<img width="203" alt="image" src="https://user-images.githubusercontent.com/64065318/169928381-3784bd20-a293-48fa-ad69-71981259a5c8.png">