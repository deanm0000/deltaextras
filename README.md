## Deltaextras

Delta Extras, right now, consists of what I call row order optimization. It is a compaction operation that will put all the unique values of a column in their own row group. It does this so that in subsequent queries for a particular entity, the reader can limit its reading to a single row group.

### Hypothetical use case

Suppose you have a table with a time column, an entity column, and some values. When the table is queried, users are most often filtering by the entity and only taking one or two at a time but taking the full time history. It is rare that users want all entities at a particular time. The natural solution would be to partition by the entity column. However, if the cardinality is high partitioning in this way might leave each file at only a couple megabytes. 

Instead, the table is partitioned by a new field whose name is `<entity>_range` and the value of it is arbitrary with no meaning. Each of those will receive a sorted range of entities such that a file in this partition will be a more appropriate size. This is helpful because subsequent queries for a single entity can skip reading all but one partition through the use of min/max statistics. There might still be many files in the partition because of frequent time updates. A good way to optimize this would be to z-order by the entity column. The less than ideal aspect of that approach in each row group of the output file will contain multiple entities. Even worse, some entities will span 2 row groups. Most queries will result in a single row group being downloaded but that row group will have a lot of data that is thrown away. A few unlucky queries will hit the entity whose data spans 2 row groups. One idea is to set the max row group size to the number of time intervals but this fails if the entities aren't uniform in size.

### Usage example

```
from deltaextras import rorder
from deltalake import DeltaTable

dt=DeltaTable(path_to_table)

rorder(
    dt,
    partition=("entity_range", "=", 1),
)
```

### How it works

Unfortunately this is not a rust backed library, it uses pyarrow and fsspec so it is rather slow and it can only do one partition at a time. It is left to the user to implement multiprocessing.

It relies on the table having a column which is suffixed by "_range" as its partitioning column. The column without the suffix is the one whose unique values will be put in their own row groups. It uses `dt.table_uri` to get the path of the delta table and then it uses `fsspec` to access the underlying file system. This implicitly assumes that whatever environment variables used by ObjectStore in deltalake will work for fsspec.

It uses `dt.get_add_actions()` to get state of the table and then parses it with `pyarrow`. It puts all the files into a pyarrow dataset and scans them to create the unique values. It opens a `ParquetWriter` object which will create a row group each time its `write` method is called. With the writer open, it will iterate over each of the unique values from the previous step filtering the dataset and writing each batch to the file. 

After it is finished writing the file, it closes and reopens it (although doesn't read the data) to verify that it is readable and to get the overall statistics of the file. It creates the log entries modeled after deltalake's output.

It checks for what the next log file number should be by incrementing up from `dt.version()` until a file doesn't exist at that number. Any log files that do exist that are bigger than the current version are inspected for any remove actions performed on any of the input files. If it finds one then the operation raises an Error. (I don't know if there are other operations that should cause this to fail). Otherwise it'll double check that the file it is about to write doesn't exist and writes it. It then waits 5 seconds, and reads the file it just wrote. If the file hasn't changed then it's complete and returns None. If the file has changed (due to some race condition) it then goes back to looking for the next log file name trying to write another file.

### Future Features (maybe)

1. Helper function to create the table. It would create a check constraint that would look like `(entity_range=1 and entity>=0 and entity<=10) or (entity_range=2 and entity>=11 and entity <=20)`. The user would need to decide on the ranges.

2. Appender function which would create the `_range` column in the background so the user doesn't have to.

3. Port to rust. I probably wouldn't do this and keep this stand alone but 