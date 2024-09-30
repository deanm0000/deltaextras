from __future__ import annotations

import json
from concurrent.futures import ProcessPoolExecutor, wait
from datetime import datetime, timezone
from multiprocessing.context import SpawnContext
from time import sleep
from typing import TYPE_CHECKING, Any, Literal, Sequence, TypeAlias, cast
from uuid import uuid4

import pyarrow.dataset as ds
import pyarrow.parquet as pq
from fsspec import filesystem
from pyarrow import Array, StructArray, Table, array, concat_tables
from pyarrow import compute as pc

from deltaextras.utils._utils import (
    _check_intermediate_logs_for_conflicts,
    _find_next_log,
    _get_time_cutoff,
    _make_add_entry,
    _make_commit_entry2,
    _make_remove_entry2,
)
from deltaextras.utils.sortunique import (
    _sort_by_fixer,
    _unique_by,
)

if TYPE_CHECKING:
    from datetime import timedelta

    from deltalake import DeltaTable

    from deltaextras.utils.sortunique import (
        Order,
    )


class TooManyPartitions(Exception):
    """Only one partition is allowed."""


# def do_not_run():
#     dt = DeltaTable("abfs://deltas/pjm/rt_unverified_fivemin_lmps")
# rorder(dt, sort_by='utcbegin', unique_by='utcbegin')
#     sort_by=unique_by='utcbegin'
#     all_files = dt.files()
#     uniq_range = {x.split("/")[0].split("=")[1] for x in all_files}
#     files_per_part = {
#         y: [x for x in all_files if x.split("/")[0].split("=")[1] == y]
#         for y in uniq_range
#     }
#     for i, (part, files) in enumerate(files_per_part.items()):
#         if len(files) <= 1:
#             continue
#         print(f"{datetime.now()}: {part} start")
#         rorder(
#             dt,
#             ("node_id_range", "=", part),
#             sort_by="utcbegin",
#             unique_by=["utcbegin", "node_id"],
#         )
#         print(f"{datetime.now()}: done {part}")

equal: TypeAlias = Literal["="]


def rorder(
    dt: DeltaTable,
    *,
    partitions: Sequence[str | int] | Sequence[tuple[str, equal, str]] | None = None,
    min_commit_interval: int | timedelta | None = None,
    max_file_size_bytes: int | None = None,
    pyarrow_writer_properties: dict[str, Any] | None = None,
    custom_metadata: dict[str, str] | None = None,
    sort_by: Sequence[str] | str | Sequence[tuple[str, Order]] | None = None,
    unique_by: Sequence[str] | str | None = None,
    max_materialize_size: int = 1_000_000,
    max_total_size: int = 5_000_000,
    max_workers=None,
):
    """
    Function to do rorder on all partitions in parallel.

    Args:
        dt (DeltaTable):
        min_commit_interval (int | timedelta | None, optional): .
        max_file_size_bytes (int | None, optional): .
        pyarrow_writer_properties (dict[str, Any] | None, optional): .
        custom_metadata (dict[str, str] | None, optional): .
        sort_by (list[str] | str | list[tuple[str, Order]] | None, optional):.
        unique_by (list[str] | str | None, optional): .
        max_materialize_size (int, optional): . Defaults to 1_000_000.
        max_total_size (int, optional): . Defaults to 5_000_000.
    """
    if partitions is not None:
        if isinstance(partitions, Sequence) and all(
            isinstance(x, tuple) for x in partitions
        ):
            partitions = [cast(tuple, x)[2] for x in partitions]
        partitions = cast(Sequence[str | int], partitions)
    (current_version, dt_path, partition_name, by_group) = rorder_setup_multi(
        dt,
        partitions=partitions,
        min_commit_interval=min_commit_interval,
        max_file_size_bytes=max_file_size_bytes,
    )
    single_dict = {
        "version": current_version,
        "path": dt_path,
        "partition_name": partition_name,
        "pyarrow_writer_properties": pyarrow_writer_properties,
        "sort_by": sort_by,
        "unique_by": unique_by,
        "custom_metadata": custom_metadata,
        "max_materialize_size": max_materialize_size,
        "max_total_size": max_total_size,
    }
    work = []
    with ProcessPoolExecutor(
        max_workers=max_workers, mp_context=SpawnContext()
    ) as executor:
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
            if sort_by is not None and unique_by is not None and len(files) == 1:
                continue
            work.append(executor.submit(rorder_single, single_dict_one))
        wait(work)


def rorder_setup_multi(
    dt: DeltaTable,
    *,
    partitions: Sequence[str | int] | None = None,
    max_file_size_bytes: int | None = None,
    min_commit_interval: int | timedelta | None = None,
):
    """
    Get node list for parallel execution running.

    Args:
        dt (DeltaTable):
        partition (tuple[str, str, str  |  int  |  float]):
        min_commit_interval (int | timedelta | None, optional):
        max_file_size_bytes (int | None, optional):.
        pyarrow_writer_properties (dict[str, Any] | None, optional):.
        custom_metadata (dict[str, str] | None, optional):.
        sort_by (list[str] | str | list[tuple[str, Order]] | None, optional):.
        unique_by (list[str] | str | None, optional):.
    """
    dt_path = dt.table_uri

    current_version = dt.version()
    action_batch = dt.get_add_actions()
    ## check only one partition field
    if action_batch.field("partition_values").type.num_fields > 1:
        raise TooManyPartitions
    partition_name: str = action_batch.field("partition_values").type.field(0).name

    partition_values = cast(StructArray, action_batch["partition_values"]).field(0)
    partition_values = pc.unique(partition_values)
    action_table = Table.from_arrays(
        [
            cast(StructArray, action_batch["partition_values"]).field(partition_name),
            action_batch["path"],
            action_batch["size_bytes"],
            action_batch["modification_time"],
        ],
        names=["partition_values", "path", "size_bytes", "modification_time"],
    )
    time_cutoff = _get_time_cutoff(min_commit_interval)
    if time_cutoff is not None:
        action_table = action_table.filter(pc.field("modification_time") > time_cutoff)
    if partitions is not None:
        action_table.filter(
            pc.is_in(action_table["partition_values"], array(partitions))  # type: ignore
        )

    by_group = action_table.group_by("partition_values").aggregate(  # type: ignore
        [("path", "list"), ("size_bytes", "list")]
    )

    return (
        current_version,
        dt_path,
        partition_name,
        by_group,
    )


def rorder_single(in_dict: dict[str, Any]):
    """
    rorder one partition at a time with json inputs.

    Args:
        in_dict (dict[str, Any]):
    """
    unique_by = in_dict.get("unique_by")
    sort_by = in_dict.get("sort_by")
    version = cast(int, in_dict.get("version"))
    assert version is not None
    files = cast(list[tuple[str, int]], in_dict.get("files") or [])
    partition_name = in_dict.get("partition_name")
    partition_value = in_dict.get("partition_value")
    assert partition_value is not None
    partition_value = str(partition_value)
    custom_metadata = in_dict.get("custom_metadata")  # noqa: F841
    root_dir = in_dict.get("path")
    max_materialize_size: int = in_dict.get("max_materialize_size") or 1_000_000
    max_total_size: int = in_dict.get("max_total_size") or 5_000_000

    assert root_dir is not None
    root_dir_split = root_dir.split("://", maxsplit=1)
    if len(root_dir_split) == 1:
        root_dir = root_dir_split
        fs = filesystem("local")
    else:
        root_dir = root_dir_split[1]
        fs = filesystem(root_dir_split[0])

    if root_dir[-1] != "/":
        root_dir += "/"
    partition = [partition_name, partition_value]
    data_dir = f"{root_dir}{partition[0]}={partition[1]}"

    new_file = f"{data_dir}/part-00001-{uuid4()}-c000.zstd.parquet"
    log_dir = f"{root_dir}_delta_log"
    materialize_files_sizes = [x for x in files if x[1] <= max_materialize_size]
    lazy_files_sizes = [x for x in files if x[1] > max_materialize_size]
    if sum(x[1] for x in materialize_files_sizes) > max_total_size:
        materialize_files_sizes = []
        lazy_files_sizes = files

    remove_entries = _make_remove_entry2(fs, root_dir, files, partition)

    first_file = f"{root_dir}/{files[0][0]}"

    with pq.ParquetFile(first_file, filesystem=fs) as ff:
        schema = ff.schema_arrow
    assert schema is not None
    pyarrow_writer_properties = in_dict.get("pyarrow_writer_properties")
    if pyarrow_writer_properties is None:
        pyarrow_writer_properties = {}
    pyarrow_writer_properties = cast(dict[str, Any], pyarrow_writer_properties)
    pyarrow_writer_properties["filesystem"] = fs
    if "compression" not in pyarrow_writer_properties:
        pyarrow_writer_properties["compression"] = "ZSTD"
    materialize_files = [f"{root_dir}{x[0]}" for x in materialize_files_sizes]
    sm_table = ds.dataset(
        materialize_files, schema=schema, format="parquet", filesystem=fs
    ).to_table()
    target_column = partition[0].replace("_range", "")
    unique_entries = pc.unique(cast(Array, sm_table[target_column]))
    lazy_files = [f"{root_dir}{x[0]}" for x in lazy_files_sizes]
    big_files = [pq.ParquetFile(lp, filesystem=fs) for lp in lazy_files]
    unique_locator = {}
    range_locator = []
    rg_ranges = {}
    for i, big_file in enumerate(big_files):
        meta = big_file.metadata
        col_i = None
        rg_range = []

        for r in range(meta.num_row_groups):
            if col_i is None:
                for c in range(meta.num_columns):
                    if meta.row_group(r).column(c).path_in_schema == target_column:
                        col_i = c
                        break
            assert col_i is not None
            stats = meta.row_group(r).column(col_i).statistics
            assert stats is not None
            rg_range.append((stats.min, stats.max))
            if stats.max == stats.min:
                value = stats.max
                if value not in unique_locator:
                    unique_locator[value] = []
                unique_locator[value].append((i, r))
            else:
                range_locator.append([(stats.min, stats.max), i, r, False])
        if not all(x[0] == x[1] for x in rg_range):
            rg_ranges[i] = rg_range
    potentials = []
    if (x := pc.min(unique_entries).as_py()) is not None:
        potentials.append(x)
    if len(unique_locator.keys()) > 0:
        potentials.append(min(unique_locator.keys()))
    if len(y := [x[0][0] for x in range_locator]) > 0:
        potentials.append(min(y))
    current_entry = min(potentials)
    new_pq_file = pq.ParquetWriter(
        new_file,
        schema,
        **pyarrow_writer_properties,
    )
    range_cache = None
    numBatches = len(materialize_files) + len(unique_locator) + len(range_locator)
    while True:
        entry_tbl = [sm_table.filter(pc.field(target_column) == current_entry)]
        if current_entry in unique_locator:
            for big_file_i, rg_i in unique_locator[current_entry]:
                entry_tbl.append(big_files[big_file_i].read_row_group(rg_i))
        for i, ((begin, end), big_file_i, rg_i, already_cached) in enumerate(
            range_locator
        ):
            if already_cached or current_entry < begin or current_entry > end:
                continue
            if range_cache is None:
                range_cache = big_files[big_file_i].read_row_group(rg_i)
            else:
                range_cache = concat_tables(
                    [range_cache, big_files[big_file_i].read_row_group(rg_i)]
                )
            range_locator[i][3] = True
        if range_cache is not None:
            entry_tbl.append(
                range_cache.filter(pc.field(target_column) == current_entry)
            )
            range_cache = range_cache.filter(pc.field(target_column) != current_entry)
        entry_tbl = concat_tables(entry_tbl)
        if unique_by is not None:
            entry_tbl = _unique_by(entry_tbl, unique_by)
        if sort_by is not None:
            sort_by = _sort_by_fixer(sort_by)
            entry_tbl = entry_tbl.sort_by(sort_by)
        entry_tbl = entry_tbl.select(schema.names)
        new_pq_file.write(entry_tbl)
        potentials = []
        if (
            x := pc.min(
                unique_entries.filter(pc.greater(unique_entries, current_entry))
            ).as_py()
        ) is not None:
            potentials.append(x)
        if len(y := [x for x in unique_locator if x > current_entry]) > 0:
            potentials.append(min(y))
        if len(y := [x[0][0] for x in range_locator if x[0][0] > current_entry]) > 0:
            potentials.append(min(y))
        if isinstance(range_cache, Table):
            x = pc.min(
                cast(
                    Array,
                    range_cache.filter(pc.field(target_column) > current_entry)[
                        target_column
                    ],
                )
            ).as_py()
            if x is not None:
                potentials.append(x)
        if len(potentials) == 0:
            break

        current_entry = min(potentials)
    new_pq_file.close()
    print(f"{datetime.now()} wrote {new_file}")
    ## get stats and check file is readable
    new_file_size = fs.size(new_file)
    new_file_read = pq.ParquetFile(new_file, filesystem=fs)
    write_time = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    add_log_entry = _make_add_entry(
        new_file_read,
        new_file.replace(root_dir, ""),
        new_file_size,
        (partition[0], "=", partition[1]),
        write_time,
    )

    commit_log_entry = _make_commit_entry2(
        write_time,
        new_file_size,
        remove_entries,
        numBatches,
        version,
        (partition[0], "=", partition[1]),
    )
    new_log_entries = cast(list[dict[str, Any]], remove_entries)
    new_log_entries.append(add_log_entry)
    new_log_entries.append(commit_log_entry)
    log_as_string = []
    for log in new_log_entries:
        log_as_string.append(json.dumps(log))
    log_as_string = "\n".join(log_as_string)
    start_ver_check = version + 1
    files_array = array([x[0] for x in files])
    while True:
        next_log_i = _find_next_log(start_ver_check, log_dir, fs)
        print(
            f"{datetime.now()} checking conflicts for {new_file} [{start_ver_check}, {next_log_i}]"
        )
        _check_intermediate_logs_for_conflicts(
            start_ver_check, next_log_i, log_dir, files_array, fs
        )
        print(f"{datetime.now()} no conflicts for {new_file}")
        log_file_path = f"{log_dir}/{next_log_i:020}.json"
        # double check that next_log_i still doesn't exist
        if fs.exists(log_file_path):
            print(f"tried log {log_file_path} but is taken")
            start_ver_check = next_log_i
            continue
        with fs.open(log_file_path, "w") as f:
            f.write(log_as_string)
        print(f"{datetime.now()} wrote {log_file_path} for {new_file}")
        # wait 5 seconds and check if log file hasn't been clobbered
        sleep(5)
        with fs.open(log_file_path, "r") as f:
            check_log = f.read()
        if log_as_string == check_log:
            break
        else:
            print(f"{datetime.now()} conflict on {log_file_path} for {new_file}")
            start_ver_check = next_log_i
            continue


def _can_int(x):
    try:
        return int(x)
    except ValueError:
        return None


def move_old(log_dir, fs):
    """Moves log files from before last checkpoint to a different folder."""
    if not fs.exists(f"{log_dir}/_last_checkpoint"):
        msg = "no checkpoints"
        raise ValueError(msg)

    with fs.open(f"{log_dir}/_last_checkpoint", "r") as ff:
        last_checkpoint = ff.read()
    last_checkpoint = json.loads(last_checkpoint)["version"]

    logs = fs.ls(log_dir, invalidate_cache=True)

    logs_to_move = [
        x
        for x in logs
        if (y := _can_int(x.split("/")[-1].split(".", maxsplit=1)[0])) is not None
        and y < last_checkpoint
    ]

    old_path = log_dir.replace("_delta_log", "_old_delta_log")
    for log in logs_to_move:
        fs.mv(log, old_path)
