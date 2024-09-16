from __future__ import annotations

import importlib.metadata
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
from fsspec import filesystem
from pyarrow import NumericArray, StringArray
from pyarrow import compute as pc

if TYPE_CHECKING:
    import pyarrow.parquet as pq
    from fsspec.spec import AbstractFileSystem
    from pyarrow import Array, RecordBatch


class Conflict(Exception):
    """Error when a file being compacted was removed by a subsequent operation."""


try:
    __version__ = importlib.metadata.version("deltaextras")
except importlib.metadata.PackageNotFoundError:
    try:
        with Path("../../pyproject.toml").open("r") as lines:
            while True:
                this_line = next(lines)
                if this_line[0:7] == "version":
                    __version__ = (
                        this_line.split("=")[1].replace('"', "").replace("\n", "")
                    )
                    break
    except Exception:
        __version__ = "0.0.0"


def _make_remove_entry(
    batch: RecordBatch,
    partition: tuple[str, str, str | int | float],
    remove_time: int,
):
    filepath = batch["path"][0].as_py()
    size = batch["size_bytes"][0].as_py()
    return {
        "remove": {
            "path": filepath,
            "dataChange": False,
            "deletionTimestamp": remove_time,
            "partitionValues": {partition[0]: str(partition[2])},
            "size": size,
        }
    }


def _make_add_entry(
    pqfile: pq.ParquetFile,
    new_file_path: str,
    new_file_size: int,
    partition: tuple[str, str, str | int | float],
    write_time: int,
) -> dict[str, Any]:
    stats = {
        "numRecords": pqfile.metadata.num_rows,
        "minValues": {},
        "maxValues": {},
        "nullCount": {},
    }
    for r in range(pqfile.num_row_groups):
        for c in range(pqfile.metadata.num_columns):
            if pqfile.metadata.row_group(r).column(c).statistics is None:
                continue
            cur_stats = pqfile.metadata.row_group(r).column(c).statistics
            assert cur_stats is not None
            col_name = pqfile.metadata.row_group(r).column(c).path_in_schema
            cur_min = _safe_val(cur_stats.min)
            if cur_min is not None and (
                col_name not in stats["minValues"]
                or stats["minValues"][col_name] > cur_min
            ):
                stats["minValues"][col_name] = cur_min
            cur_max = _safe_val(cur_stats.max)
            if cur_max is not None and (
                col_name not in stats["maxValues"]
                or stats["maxValues"][col_name] < cur_max
            ):
                stats["maxValues"][col_name] = cur_max
            if col_name not in stats["nullCount"]:
                stats["nullCount"][col_name] = cur_stats.null_count
            else:
                stats["nullCount"][col_name] += cur_stats.null_count

    return {
        "add": {
            "path": new_file_path,
            "partitionValues": {partition[0]: str(partition[2])},
            "size": new_file_size,
            "modificationTime": write_time,
            "dataChange": False,
            "stats": json.dumps(stats),
            "tags": None,
            "deletionVector": None,
            "baseRowId": None,
            "defaultRowCommitVersion": None,
            "clusteringProvider": None,
        }
    }


def _make_commit_entry(
    write_time: int,
    new_file_size: int,
    partition_batch: RecordBatch,
    numBatches: int,
    readVersion: int,
    partition: tuple[str, str, str | int | float],
):
    size_array = cast(NumericArray, partition_batch["size_bytes"])

    filesAdded = {
        "avg": float(new_file_size),
        "max": new_file_size,
        "min": new_file_size,
        "totalFiles": 1,
        "totalSize": new_file_size,
    }
    filesRemoved = {
        "avg": pc.mean(size_array).as_py(),
        "max": pc.max(size_array).as_py(),
        "min": pc.min(size_array).as_py(),
        "totalFiles": partition_batch.shape[0],
        "totalSize": pc.sum(size_array).as_py(),
    }

    predicate = json.dumps([f"{partition[0]} = '{partition[2]}'"])
    return {
        "commitInfo": {
            "timestamp": write_time,
            "operation": "OPTIMIZE",
            "operationParameters": {"predicate": predicate},
            "operationMetrics": {
                "filesAdded": json.dumps(filesAdded),
                "filesRemoved": json.dumps(filesRemoved),
                "numBatches": numBatches,
                "numFilesAdded": 1,
                "numFilesRemoved": partition_batch.shape[0],
                "partitionsOptimized": 0,
                "preserveInsertionOrder": True,
                "totalConsideredFiles": partition_batch.shape[0],
                "totalFilesSkipped": 0,
            },
            "clientVersion": f"deltaextras{__version__}",
            "readVersion": readVersion,
        }
    }


def _safe_val(x: str | int | datetime | date | None) -> str | int | None:
    if isinstance(x, datetime | date):
        return x.isoformat()
    else:
        return x


def _get_fs_and_dir(
    partition: tuple[str, str, str | int | float], dt_path: str
) -> tuple[AbstractFileSystem, str, str, str]:
    if len(partition) != 3:
        msg = "partition must be in the form ('column_name', '=', value)"
        raise ValueError(msg)
    if partition[1] != "=":
        msg = "partition's second element must be ="
        raise ValueError(msg)
    dt_path_split = dt_path.split("://")
    if len(dt_path_split) == 1:
        root_dir = dt_path
        fs = filesystem("local")
    elif len(dt_path_split) == 2:
        root_dir = dt_path_split[1]
        fs = filesystem(dt_path_split[0])
    else:
        msg = "how are there more than one ://??"
        raise ValueError(msg)

    if root_dir[-1] != "/":
        root_dir += "/"

    data_dir = f"{root_dir}{partition[0]}={partition[2]}"

    log_dir = root_dir + "_delta_log/"
    return (fs, root_dir, data_dir, log_dir)


def _make_filter_expr(
    partition: tuple[str, str, str | int | float],
    min_commit_interval: int | timedelta | None = None,
) -> pc.Expression:
    """
    Started with this one but converted it to _make_part_batch for the casting
    partition[2] to match type.
    """  # noqa: D205
    part_filter: pc.Expression = (
        pc.field("partition_values", partition[0]) == partition[2]
    )
    if min_commit_interval is None or (
        isinstance(min_commit_interval, int) and min_commit_interval == 0
    ):
        return part_filter
    elif isinstance(min_commit_interval, timedelta):
        time_cutoff = datetime.now(tz=timezone.utc) - min_commit_interval
    else:
        time_cutoff = datetime.now(tz=timezone.utc) - timedelta(
            seconds=min_commit_interval
        )
    time_cutoff = time_cutoff.replace(tzinfo=None)
    time_filter: pc.Expression = pc.field("modification_time") >= time_cutoff
    return part_filter & time_filter


def _make_part_batch(
    action_batch: RecordBatch,
    partition: tuple[str, str, str | int | float],
    min_commit_interval: int | timedelta | None = None,
) -> RecordBatch:
    part_filter: pc.Expression = pc.field(
        "partition_values", partition[0]
    ) == pc.scalar(partition[2]).cast(
        action_batch.schema.field("partition_values").type.field(0).type
    )
    time_cutoff = None
    filter_expr = pc.field("a")
    if min_commit_interval is None or (
        isinstance(min_commit_interval, int) and min_commit_interval == 0
    ):
        filter_expr = part_filter
    elif isinstance(min_commit_interval, timedelta):
        time_cutoff = datetime.now(tz=timezone.utc) - min_commit_interval
    else:
        time_cutoff = datetime.now(tz=timezone.utc) - timedelta(
            seconds=min_commit_interval
        )
    if time_cutoff is not None:
        time_cutoff = time_cutoff.replace(tzinfo=None)
        time_filter: pc.Expression = pc.field("modification_time") >= time_cutoff
        filter_expr = part_filter & time_filter
    return action_batch.filter(filter_expr)


def _big_small(
    partition_batch: RecordBatch, max_materialize_size: int, max_total_size: int
) -> tuple[list[str], list[str]]:
    try:
        sizes_filt = partition_batch.filter(
            pc.field("size_bytes") <= max_materialize_size
        )
        sizes = sizes_filt["size_bytes"]
        assert isinstance(sizes, pa.Int64Array)
        total_size = pc.sum(sizes)
        if total_size.as_py() >= max_total_size:
            small = pa.array([], type=pa.string())
        else:
            small = sizes_filt["path"]
    except IndexError:
        small = pa.array([], type=pa.string())
    try:
        big = partition_batch.filter(~pc.field("path").isin(small))["path"]
    except IndexError:
        big = pa.array([], type=pa.string())
    return (_paarray_to_str_list(big), _paarray_to_str_list(small))


def _find_next_log(version: int, log_dir: str, fs: AbstractFileSystem):
    # This function could be async for faster file checking. If the version
    # is low enough it could assume that there aren't a lot of files and then ls
    # the dir instead of checking every file number
    # Doing ls on a lot of files is really slow
    # log_files = fs.ls(log_dir, invalidate_cache=True)
    # max_version = max(
    #     [
    #         int(x.replace(log_dir, "").replace(".json", ""))
    #         for x in log_files
    #         if x[-5:] == ".json"
    #     ]
    # )
    # import time
    # strt =time.time()
    # i=1804
    # while time.time()-strt<2.8:
    #     log_file = f"{log_dir}{i:20d}.json"
    #     abfs.exists(log_file)
    #     i+=1
    # in 2.8s it can only check for 21 exists, may need async
    i = version
    while fs.exists(f"{log_dir}{i:020d}.json"):
        i += 1
    return i


def _paarray_to_str_list(array: Array) -> list[str]:
    assert isinstance(array, StringArray)
    strlist = [x for x in array.tolist() if x is not None]
    return strlist


def _check_intermediate_logs_for_conflicts(
    start_i: int,
    last_i_plus_1: int,
    log_dir: str,
    partition_batch: RecordBatch,
    fs: AbstractFileSystem,
) -> None:
    for i in range(start_i, last_i_plus_1):
        with fs.open(f"{log_dir}{i:020}.json", "r") as f:
            log = f.read()
            if log is not None:
                assert isinstance(log, str)
                for entry in log.split("\n"):
                    entry_dict = json.loads(entry)
                    if (
                        "remove" in entry_dict
                        and "path" in entry_dict["remove"]
                        and pc.is_in(
                            entry_dict["remove"]["path"], partition_batch["path"]
                        ).as_py()
                    ):
                        msg = f"{entry_dict['remove']['path']} was removed by {i:020}.json"
                        raise Conflict(msg)


__all__ = [
    "_make_remove_entry",
    "_make_add_entry",
    "_make_commit_entry",
    "_safe_val",
    "_get_fs_and_dir",
    "_make_part_batch",
    "_find_next_log",
    "_paarray_to_str_list",
    "_check_intermediate_logs_for_conflicts",
    "_big_small",
]
