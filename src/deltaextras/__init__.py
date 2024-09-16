from __future__ import annotations

import json
from datetime import datetime, timezone
from time import sleep
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyarrow import Array, Table, concat_tables
from pyarrow import compute as pc

from deltaextras._utils import (
    _big_small,
    _check_intermediate_logs_for_conflicts,
    _find_next_log,
    _get_fs_and_dir,
    _make_add_entry,
    _make_commit_entry,
    _make_part_batch,
    _make_remove_entry,
)

if TYPE_CHECKING:
    from datetime import timedelta

    from deltalake import DeltaTable


def rorder(
    dt: DeltaTable,
    partition: tuple[str, str, str | int | float],
    min_commit_interval: int | timedelta | None = None,
    pyarrow_writer_properties: dict[str, Any] | None = None,
    custom_metadata: dict[str, str] | None = None,
    sort_by: str | None = None,
    max_materialize_size: int = 1_000_000,
    max_total_size: int = 5_000_000,
):
    # TODO: add parameter to feed it unique values of the subpartition so it can skip
    # scanning the underlying parquet files. Add storage_options to feed to fsspec.
    """
    Makes a row group optimized compaction of files in delta table.

    Args:
        dt (DeltaTable): A DeltaTable object
        partition (tuple[str, str, str  |  int  |  float]): single partition filter.
            tuple in the form ('column','=',value)
        min_commit_interval (int | timedelta): Limit to files within this time.
        pyarrow_writer_properties (dict[str, Any] | None, optional): Options to
            ParquetWriter.
        custom_metadata (dict[str, str] | None, optional): #TODO
    """
    dt_path = dt.table_uri
    fs, root_dir, data_dir, log_dir = _get_fs_and_dir(partition, dt_path)

    current_version = dt.version()

    new_file = f"{data_dir}/part-00001-{uuid4()}-c000.zstd.parquet"

    action_batch = dt.get_add_actions()
    partition_batch = _make_part_batch(action_batch, partition, min_commit_interval)
    if partition_batch.shape[0] == 1:
        ### If there is only one file, there's nothing to do, should this raise?
        return
    lazy_files, materialize_files = _big_small(
        partition_batch, max_materialize_size, max_total_size
    )
    lazy_files_paths = [root_dir + x for x in lazy_files]
    materialize_files_paths = [root_dir + x for x in materialize_files]
    remove_time = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    new_log_entries = [
        _make_remove_entry(partition_batch.slice(i, 1), partition, remove_time)
        for i in range(partition_batch.shape[0])
    ]

    first_file = partition_batch["path"][0].as_py()

    with pq.ParquetFile(root_dir + first_file, filesystem=fs) as ff:
        schema = ff.schema_arrow
    assert schema is not None
    pyarrow_writer_properties = pyarrow_writer_properties or cast(dict[str, str], {})
    pyarrow_writer_properties["filesystem"] = fs
    if "compression" not in pyarrow_writer_properties:
        pyarrow_writer_properties["compression"] = "ZSTD"
    sm_table = ds.dataset(
        materialize_files_paths, schema=schema, format="parquet", filesystem=fs
    ).to_table()

    target_column = partition[0].replace("_range", "")
    unique_entries = pc.unique(cast(Array, sm_table[target_column]))
    big_files = [pq.ParquetFile(lp, filesystem=fs) for lp in lazy_files_paths]
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

    def old_iterator():
        pass
        # i = -1
        # for i, batch in enumerate(scan_entries.to_batches()):
        #     unique_entries = pc.unique(
        #         concat_arrays([unique_entries, batch[target_column]])
        #     ).sort()
        # numBatches = i + 1
        # TODO: check files' stats to see if row groups are sufficient to give unique
        # values
        # if they are then can iterate across row groups instead of dataset batches

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
        if sort_by is not None:
            entry_tbl = entry_tbl.sort_by([(sort_by, "ascending")])
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
    ## get stats and check file is readable
    new_file_size = fs.size(new_file)
    new_file_read = pq.ParquetFile(new_file, filesystem=fs)
    write_time = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    new_log_entries.append(
        _make_add_entry(
            new_file_read,
            new_file.replace(root_dir, ""),
            new_file_size,
            partition,
            write_time,
        )
    )
    new_log_entries.append(
        _make_commit_entry(
            write_time,
            new_file_size,
            partition_batch,
            numBatches,
            current_version,
            partition,
        )
    )
    log_as_string = []
    for log in new_log_entries:
        log_as_string.append(json.dumps(log))
    log_as_string = "\n".join(log_as_string)
    start_ver_check = dt.version() + 1
    while True:
        next_log_i = _find_next_log(start_ver_check, log_dir, fs)
        _check_intermediate_logs_for_conflicts(
            start_ver_check, next_log_i, log_dir, partition_batch, fs
        )
        log_file_path = f"{log_dir}{next_log_i:020}.json"
        # double check that next_log_i still doesn't exist
        if fs.exists(log_file_path):
            start_ver_check = next_log_i
            continue
        with fs.open(log_file_path, "w") as f:
            f.write(log_as_string)
        # wait 5 seconds and check if log file hasn't been clobbered
        sleep(5)
        with fs.open(log_file_path, "r") as f:
            check_log = f.read()
        if log_as_string == check_log:
            break
        else:
            start_ver_check = next_log_i
            continue
