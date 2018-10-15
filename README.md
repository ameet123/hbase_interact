## HBase Check and Load 

### Objective
+ Stream a file of fixed width records
+ construct a Primary key from certain fields based on position.
+ check whether these keys exist in a specific HBase table.
+ if they do not, then insert them.
+ if they do, then write them to a file as duplicate records.
+ If there are duplicate records in the file itself, then write all such records to the same duplicate file.

### Solution:
1. Data Model: 

    `MurmurHash3` of PK columns.
2. Check record existence: API
3. INSERT: API
4. Scaling: Parallelization
5. Streaming: Event Bus
 
### Software:
+ Spring
+ ReactiveX 
+ HBase client Java API

### Properties
Before compiling, modify the properties file, `src/main/resources/application.yml` with appropriate parameters,

```json
spring:
  profiles: dev
my:
    krb: /home/af55267/app/hbase_interact/src/main/resources/krb5.conf
    keytab: /home/af55267/app/hbase_interact/src/main/resources/hbase_interact.keytab
    table: "dv_hb_bdfrawz_nogbd_r1a_wh:voyager_dq"
    small: /home/af55267/app/hbase_interact/src/main/resources/sample_hbase.csv
    data: /home/af55267/app/hbase_interact/data/Sample_220K_unique.txt
    outputDir: /home/af55267/app/hbase_interact/data/output
    bucketSize: 5000
    threadPoolSize: 75
    thresholdSec: 5
    backpressure: 75
    threadPrefix: "hbaseRx-%d"
    user: "AF55267@DEVAD.WELLPOINT.COM"
    column: "sanity_col"
```

**Update these values as appropriate for your environment**

### Compile
Note: 

```bash
git clone ssh://git@bitbucket.anthem.com:7999/~af55267/hbase_interact.git

### Update PRoperties
cd hbase_interact
./gradlew clean build
```

### Run
Imp*: Put `hbase-site.xml` in classpath or in src/main/resources
```bash
java -Dspring.profiles.active=dev  -jar build/libs/HBaseInteract-0.1.0.jar
```
#### Architecture

![arch](image/arch_hbaseapp.png)

#### Generate sample data
```python
with open('C:\\Users\\AF55267\\Documents\\anthem\\source\\hbase_interact\\src\\main\\resources\\sample_10k_hbase.csv','+w') as f:
    for i in range(10000):
        f.write(''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(40))+"\n")
```

#### Preparation
Clean out the HBase table,
```sql
hbase shell
> truncate 'dv_hb_bdfrawz_nogbd_r1a_wh:voyager_dq'
```

