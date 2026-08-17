#!/usr/bin/env python3
"""Tái cấu trúc dataset Parquet của dashboard — NHIỆM VỤ 4.  CHƯA CÓ LOGIC.

Hiện trạng: `data/gold_events/` gồm 5.000 file, mỗi file vài chục KB, không
partition, thứ tự hàng ngẫu nhiên.

Yêu cầu: đọc toàn bộ dataset cũ, ghi ra dataset mới có layout hợp lý hơn, sau đó cập
nhật `queries/dashboard.sql` để trỏ vào dataset mới.

    python tools/compact.py       # ghi dataset mới
    python tools/explain.py       # đo lại và so với baseline

KHUNG THỰC HIỆN

    COPY (
        SELECT *
        FROM   read_parquet('data/gold_events/*.parquet')
        ORDER  BY <cột A>, <cột B>
    ) TO 'data/gold_events_v2' (
        FORMAT          parquet,
        PARTITION_BY    (<cột partition>),
        OVERWRITE_OR_IGNORE,
        ROW_GROUP_SIZE  <?>
    )

Ba quyết định, mỗi quyết định cần một lý do viết được ra giấy:

  <cột partition>   Engine chỉ bỏ qua được file mà nó biết là vô ích TRƯỚC khi
                    mở file. Thông tin đó đến từ đường dẫn. Vậy cột nào của
                    truy vấn dashboard nên xuất hiện trong tên thư mục? Cột đó
                    có bao nhiêu giá trị phân biệt — tức bao nhiêu thư mục?
                    Partition theo cột có 650 giá trị thì hệ quả là gì?

  <cột A>, <cột B>  Thứ tự hàng trong file quyết định thống kê min/max của mỗi
                    row group có ích hay vô dụng. Sắp thế nào để các hàng cùng
                    một khách hàng nằm liền nhau?

  ROW_GROUP_SIZE    Mặc định 122.880 hàng. Một ngày có khoảng bao nhiêu hàng?
                    Nếu cả ngày gói gọn trong MỘT row group thì min/max của
                    row group đó phủ những gì, và còn tác dụng lọc không?

Sau khi chạy xong, kiểm tra lại bằng `python tools/explain.py`: `rows scanned`
phải giảm, `files` phải giảm, và `result hash` phải GIỮ NGUYÊN.
"""

from __future__ import annotations

import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import DATA  # noqa: E402

SRC = DATA / "gold_events"
DST = DATA / "gold_events_v2"


# Ba quyết định, và lý do của từng cái:
#
#   partition_by (event_date)   Truy vấn lọc theo hai cột: customer_name và ngày.
#                               Chỉ một trong hai được đưa vào đường dẫn, vì mỗi
#                               giá trị phân biệt là một thư mục. event_date có
#                               14 giá trị -> 14 thư mục, engine bỏ qua 13 thư mục
#                               mà không cần mở file nào. customer_name có 650 giá
#                               trị -> 650 thư mục file tí hon, tức tái tạo lại
#                               đúng small-file problem đang phải chữa.
#
#   order by customer_name      Cột không nằm trong đường dẫn vẫn lọc được, nhưng
#                               bằng cơ chế khác: min/max của từng row group. Thống
#                               kê đó chỉ có ích khi các hàng cùng một khách nằm
#                               liền nhau; dữ liệu cũ xếp ngẫu nhiên nên mọi row
#                               group đều có min='AAA', max='ZZZ' và không loại
#                               được cái nào.
#
#   row_group_size 2048         Một ngày ~9.300 hàng. Để mặc định 122.880 thì cả
#                               ngày gói trong MỘT row group, min/max phủ toàn bộ
#                               650 khách và trở lại vô dụng. 2048 chia ngày thành
#                               ~5 row group, mỗi cái phủ ~140 khách.
ROW_GROUP_SIZE = 2048


def main() -> int:
    con = duckdb.connect()

    n_src = len(list(SRC.glob("*.parquet")))
    print(f"  nguồn : {SRC}  ({n_src:,} file)")

    n_before = con.execute(
        f"select count(*) from read_parquet('{SRC}/*.parquet')"
    ).fetchone()[0]

    con.execute(f"""
        copy (
            select * from read_parquet('{SRC}/*.parquet')
            order by customer_name, event_time
        ) to '{DST}' (
            format          parquet,
            partition_by    (event_date),
            overwrite_or_ignore,
            row_group_size  {ROW_GROUP_SIZE}
        )
    """)

    n_after = con.execute(
        f"select count(*) from read_parquet('{DST}/**/*.parquet', hive_partitioning = true)"
    ).fetchone()[0]
    assert n_before == n_after, f"mất hàng: {n_before:,} -> {n_after:,}"

    n_dst = len(list(DST.glob("**/*.parquet")))
    print(f"  đích  : {DST}  ({n_dst:,} file, {n_after:,} hàng — khớp nguồn)")
    print(f"\n  {n_src:,} file -> {n_dst:,} file. Đo lại: python tools/explain.py\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
