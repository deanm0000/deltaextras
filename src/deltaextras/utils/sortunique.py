from __future__ import annotations

from typing import Literal, TypeAlias, cast, get_args

import pyarrow as pa
from pyarrow import compute as pc

Order: TypeAlias = Literal["ascending", "descending"]
Order_Values: tuple[Order] = get_args(Order)


class Conflict(Exception):
    """Error when a file being compacted was removed by a subsequent operation."""


class InvalidSort(Exception):
    """sort_by parameter is invalid."""


class NonReduntantDupes(Exception):
    """When getting rid of duplicates, if there are dupes that have different values."""


def _unique_by(table: pa.Table, columns: list[str] | str):
    COUNT_COL = "__COUNT_COL_JKLSFJS"
    if isinstance(columns, str):
        columns = [columns]

    original_column_order = table.column_names
    other_columns = [x for x in table.column_names if x not in columns]

    agged_cols = []
    filt_exprs = pc.field(COUNT_COL) > 1
    for x in other_columns:
        agged_cols.append((x, "min"))
        agged_cols.append((x, "max"))
        agged_cols.append((x, "list"))
        filt_exprs = filt_exprs & (pc.field(f"{x}_min") == pc.field(f"{x}_max"))
    agged = table.group_by(columns).aggregate(agged_cols)  # type: ignore

    agged = pa.Table.from_arrays(
        [agged[x] for x in agged.column_names]
        + [pc.list_value_length(agged[f"{other_columns[0]}_list"])],
        names=agged.column_names + [COUNT_COL],
    )

    dupe_keys = agged.filter(pc.field(COUNT_COL) > 1)

    dupe_vals = dupe_keys.filter(filt_exprs)

    if dupe_keys.shape[0] != dupe_vals.shape[0]:
        raise NonReduntantDupes

    deduped = pa.Table.from_arrays(
        [agged[x] for x in columns] + [agged[f"{x}_min"] for x in other_columns],
        names=columns + other_columns,
    )
    return deduped.select(original_column_order)


def _sort_by_fixer(
    sort_by: list[str] | str | list[tuple[str, Order]],
) -> list[tuple[str, Order]]:
    if isinstance(sort_by, str):
        return [(sort_by, Order_Values[0])]
    elif isinstance(sort_by, list) and all(isinstance(x, str) for x in sort_by):
        return [
            (x, cast(Order, Order_Values[0])) for x in sort_by if isinstance(x, str)
        ]
    elif isinstance(sort_by, list) and all(
        isinstance(x, tuple)
        and isinstance(x[0], str)
        and isinstance(x[1], str)
        and x[1] in Order_Values
        for x in sort_by
    ):
        return cast(list[tuple[str, Order]], sort_by)
    else:
        raise InvalidSort


__all__ = [
    "Order",
    "_sort_by_fixer",
    "_unique_by",
]
