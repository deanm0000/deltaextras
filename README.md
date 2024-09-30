## Deltaextras

Delta Extras, right now, consists of what I call a [Row Group Order Optimization](#row-group-order-optimization). It is a compaction operation that will put all the unique values of a column in their own row group. It does this so that in subsequent queries for a particular value of that column need only read a single row group

### Setup

Suppose you have a table like this
```markdown
| node_id | utc_time | data_values |
|---------|----------|-------------|
| 1       | 10:00    | 12.13       |
| 1       | 10:05    | 13.56       |
| ...     | ...      | ...         |
| 2       | 10:00    | 55.31       |
| 2       | 10:05    | 43.23       |
| ...     | ...      | ...         |
| 15000   | 10:00    | 4.23        |
| 15000   | 10:05    | 4.25        |
| ...     | ...      | ...         |
| 15001   | 10:00    | 24.23        |
| 15001   | 10:05    | 4.35        |
```
When the table is partitioned by `node_id` it generates 15000 folders and each file is tiny (maybe 1MB). That is problematic already. To make this worse consider that new data comes in every 5 minutes for all `node_id`s at once so every 5 minutes, the writer would need to write 15000 files. One solution is to create a new column which maps groups of `node_id` values to fewer index values of a new column, let's call it `node_id_range` and make that the delta partition column so now the table looks like

```markdown
| node_id | utc_time | data_values | node_id_range |
|---------|----------|-------------|---------------|
| 1       | 10:00    | 12.13       | 0             |
| 1       | 10:05    | 13.56       | 0             |
| ...     | ...      | ...         | ...           |
| 2       | 10:00    | 55.31       | 0             |
| 2       | 10:05    | 43.23       | 0             |
| ...     | ...      | ...         | ...           |
| 15000   | 10:00    | 4.23        | 100           |
| 15000   | 10:05    | 4.25        | 100           |
| ...     | ...      | ...         | ...           |
| 15001   | 10:00    | 24.23       | 100           |
| 15001   | 10:05    | 4.35        | 100           |
```

The issue with this approach is that a parquet file, by default, will have row groups based on size only. If each `node_id` can be different in length, that will lead to files where the stats of the file might look like this

```markdown
| row_group | min_node_id | max_node_id |
|-----------|-------------|-------------|
| 0         | 1           | 3           |
| 1         | 3           | 7           |
| 2         | 7           | 12          |
| 3         | 12          | 18          |
| 4         | 18          | 23          |
```
With this layout, if a user queries the table for node_id=2 then the reader will have to get the row group containing 1-3 which would be about 2-3x more data than they want. If the user wnats to query for node_id=3 then they have to get two row groups since node_id=3 got split up.

### Row Group Order Optimization

Instead we want a layout that looks like this

```markdown
| row_group | min_node_id | max_node_id |
|-----------|-------------|-------------|
| 0         | 1           | 1           |
| 1         | 2           | 2           |
| ...       | ...         | ...         |
| 22        | 22          | 22          |
| 23        | 23          | 23          |
```
With this layout a query for any particular node_id can limit what the reader needs to exactly just what it needs. The downside of this approach is it makes more row groups than might otherwise be needed. This means the file sizes can be much bigger because of metadata overhead and since compression is by row group. If queries are regularly done that include nearby `node_id`s then it could be slower.  


### Usage example from single computer

```python
from deltaextras import rorder
from deltalake import DeltaTable

dt=DeltaTable(path_to_table)

rorder(
    dt,
    sort_by=some_other_column,
    unique_by=some_other_column
)
```

### Cloud multiprocessing (using Azure Functions)

"Master/Orchestrator" function, uses Azure Storage Queue as means of starting workers.
```python
from deltalake import DeltaTable
from deltaextras import rorder_setup_multi
from azure.storage.queue.aio import QueueServiceClient as QSC
from azure.storage.queue import TextBase64EncodePolicy
import os
from pyarrow import compute as pc
import asyncio
AIO_SERVE = QSC.from_connection_string(conn_str=os.environ["AzureWebJobsStorage"])

async def main(timer_or_whatever):
    dt=DeltaTable(...)
    (current_version, dt_path, partition_name, by_group) = rorder_setup_multi(dt)
    single_dict = {
        "version": current_version,
        "path": dt_path,
        "partition_name": partition_name
    }
    tasks = set()
    async with AIO_SERVE.get_queue_client(
        queue, message_encode_policy=TextBase64EncodePolicy()
        ) as aio_client:
        for i in range(by_group.shape[0]):
            files = []
            for j in range(pc.list_value_length(by_group["path_list"][i]).as_py()):
                files.append(
                    (
                        by_group["path_list"][i][j].as_py(),
                        by_group["size_bytes_list"][i][j].as_py(),
                    )
                )
            single_dict_one = {
                **single_dict,
                "partition_value": by_group["partition_values"][i].as_py(),
                "files": files,
            }
            tasks.add(asyncio.create_task(aio_client_send_message(single_dict_one)))
        await asyncio.wait(tasks)
            

```

per partition function

```python
from deltaextras import rorder_single
from azure.functions import QueueMessage
def main(msg: QueueMessage):
    rorder_single(msg.get_json())
```

### How it works

Unfortunately this is not a rust backed library, it uses pyarrow and fsspec. 

It relies on the table having a column which is suffixed by "_range" as its partitioning column. The column without the suffix is the one whose unique values will be put in their own row groups. It uses `dt.table_uri` to get the path of the delta table and then it uses `fsspec` to access the underlying file system. This implicitly assumes that whatever environment variables used by ObjectStore in deltalake will work for fsspec. It uses `multiprocessing` to handle multiple partitions at once. Each partition will be its own transaction so that it is possible for some partitions to succeed and others to fail. The benefit of that is it is possible to disconnect the individual partition processes from the one that called it so that it can be run with with serverless cloud functions (ie Azure Functions, AWS Lambda, GCS cloud run)



It uses `dt.get_add_actions()` to get state of the table and then parses it with `pyarrow`. Its `pq.ParquetWriter` class allows for fine control of row groups. When it is opened, it has a `write` method which creates a row group in the file. With that feature, it allows us to loop through each unique value in the sub-partition (`node_id` in the example above) combining all the input files into a Table and then calling `write` for each one. It deals with input files in one of two ways. For small files (as specified by `max_materialize_size`), it'll load them into memory as a Table all at once. For big files, it creates a stats map and will only read one row group at a time from them (unless the needed data spans multiple row groups). 

After it is finished writing the file, it reopens it to get the statistics of the file and to verify it is readable. It creates the log entries modeled after deltalake's output.

It checks for what the next log file number should be by incrementing up from `dt.version()` until a file doesn't exist at that number. Any log files that do exist, with a version bigger than the current version, are inspected to verify that none of the input files have been subsequently removed by that transaction. If it finds one then the operation raises an Error. (I don't know if there are other operations that should cause this to raise). Otherwise it double checks that the file it is about to write still doesn't exist and writes it. Out of an abundance of caution, it then waits 5 seconds, and reads the log file it just wrote. If the file hasn't changed then it's complete and returns None. If the file has changed (due to some race condition with another process) it then goes back to looking for the next log file name trying to write another file as though it didn't write anything already.

### Future Features (maybe)

<s>1. Multiprocessing multiple partitions at once.</s>

2. Helper function to create the table. It would create a check constraint that would look like `(entity_range=1 and entity>=0 and entity<=10) or (entity_range=2 and entity>=11 and entity <=20)`. The user would need to decide on the ranges.

3. Appender function which would create the `_range` column in the background so the user doesn't have to.

4. Port to rust and inclusion into [delta-rs](https://github.com/delta-io/delta-rs). I'm an even worse rust developer as I am python and I don't have any sway on features that get added to delta-rs so this one is pretty unlikely any time soon.

5. Start from scratch with another Table library that is tied to [polars](https://github.com/pola-rs/polars) instead of pyarrow and would use Cosmos (or other NoSQL) instead of json files. The manualness of creating the Table and the staticness of the _range column is lacking. 