# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** …  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details>
<summary>Output ba lượt chạy</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 53.1s
  run 2/3 … 61.4s
  run 3/3 … 60.8s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✗ 5,000,000 → 5,000,000 (1.0×, cần ≥ 10×)
    số file parquet                           ✗ 5,000 → 5,000
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**. Dòng `dashboard rows scanned` ✗ thuộc bài mở
rộng A trong `EXTRA.md` — không làm, không tính vào thang 100.

Trạng thái khi tiếp nhận: **1 / 4**
(`gold_training_set` 38.750 · `gold_feature_daily` 8.645 · `quarantine_tickets` 0
· `silver_tickets.priority` 6.606 hàng sai · `dbt test` 9/9 pass).

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau ba lượt chạy, `gold_training_set` có 38.750 hàng thay vì 12.480; checksum ba lượt khác nhau hoàn toàn (`7c461563f4` / `d11657ff21` / `2b76a4f850`); 12.480 ticket bị lặp. Không có lỗi nào được báo, `dbt test` vẫn 9/9 pass. |
| **Nguyên nhân** | Model khai báo `materialized='incremental'` nhưng **không khai `unique_key`**, nên dbt không có khoá để so khớp hàng cũ với hàng mới và sinh ra câu `INSERT` thuần. Mệnh đề `where` theo `run_date` chỉ giới hạn **phạm vi đọc** ở nguồn, nó không hề ràng buộc **phép ghi** ở đích. Hệ quả: chạy lại cùng một partition là một lần ghi thêm chứ không phải ghi đè — bản thân phép ghi không idempotent, nên **mọi cơ chế retry ở tầng trên đều biến thành cơ chế nhân bản**: `retries=2` của Airflow, thao tác Clear Task, và `catchup=True` khi DAG được bật lại đều cộng thêm một bản sao đầy đủ. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key='ticket_id'` và `incremental_strategy='merge'` vào `config()`; giữ nguyên mệnh đề `where` theo `run_date`. Chọn `merge` chứ **không** chọn `delete+insert` theo partition ngày, vì nguồn CDC có `op='u'`: một ticket tạo ngày D1 rồi sửa ngày D2 nằm ở hai partition khác nhau, xoá partition D2 không dọn được bản cũ còn sót ở D1. `dags/ai_training_pipeline.py`: `catchup=False`, `max_active_runs=1`. |
| **Bằng chứng** | trước: 38.750 hàng, 12.480 ticket lặp · sau: 12.480 hàng, 0 ticket lặp · checksum 3 lượt: `8dd7c98653` × 3 |

Hai tham số DAG **không phải root cause** — chúng chỉ giảm tần suất kích hoạt
lỗi. Sửa DAG mà không sửa materialization thì lần retry đầu tiên vẫn nhân đôi
bảng; sửa materialization mà không sửa DAG thì kết quả vẫn đúng.

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | 8.645 / 9.100 hàng — thiếu 455 (5,0%), chỉ thiếu ở những ngày đã chạy xong từ lâu. Bảng vẫn `ỔN ĐỊNH ✓`: chạy lại bao nhiêu lần cũng ra đúng cùng một kết quả sai. |
| **P99 độ trễ đo được** | **2,726 ngày** *(p50 = 0,128 · p95 = 1,814 · max = 2,945 · 5,05% bản ghi tới kho muộn hơn một ngày)* |
| **Lookback đã chọn** | **3 ngày** — làm tròn lên từ P99 = 2,726. Kiểm chứng bằng phân bố theo ngày lịch: trễ 0 ngày 108.862 bản ghi · 1 ngày 14.165 · 2 ngày 3.842 · 3 ngày 2.593 · không có bản ghi nào trễ quá 3 ngày. |
| **Nguyên nhân** | Bộ lọc incremental dùng watermark `event_date > max(event_date) của bảng đích` — tức lấy mốc theo **thời điểm sự kiện xảy ra**, trong khi thứ quyết định dữ liệu đã có mặt hay chưa là **thời điểm dữ liệu tới kho**. Hai đại lượng này lệch nhau tới 3 ngày với 5% dữ liệu. Ngay khi một event của ngày mới được nạp, watermark nhảy lên ngày đó và **mọi ngày ≤ watermark bị đóng vĩnh viễn**: bản ghi của ngày cũ về sau không còn lượt chạy nào để lọt vào nữa. Đây là lỗi im lặng — không hàng nào sai, chỉ đơn giản là không bao giờ được xử lý, nên không test nào bắt được. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: đổi điều kiện thành `event_date >= (select max(event_date) from {{ this }}) - interval 3 day`, đồng thời thêm `unique_key=['event_date','customer_id']` + `incremental_strategy='merge'`. |
| **Bằng chứng** | trước: 8.645 hàng · sau: 9.100 hàng (= 14 ngày × 650 customer) · checksum 3 lượt: `3db448685c` × 3 |

Nới window mà **không** thêm `unique_key` sẽ tái tạo đúng lỗi của nhiệm vụ 1
trên một bảng khác: cùng một cặp `(event_date, customer_id)` được tính lại ở 4
lượt liên tiếp và cộng dồn thay vì thay thế. Grain ở đây có hai cột nên
`unique_key` phải là một list.

**Vì sao chọn P99 làm căn cứ thay vì `max`?** Chi phí của lookback không trả một
lần mà trả ở **mọi lượt chạy sau này**: lùi N ngày nghĩa là mỗi đêm tính lại N+1
ngày dữ liệu, mãi mãi. `max` là một quan sát đơn lẻ — nó có thể là một sự cố cá
biệt của tháng trước và sẽ kéo chi phí thường trực của pipeline lên theo. P99 là
đại lượng có thể phát biểu thành cam kết vận hành ("bắt đủ 99% dữ liệu muộn"),
phần đuôi còn lại xử lý bằng backfill thủ công khi cần — rẻ hơn nhiều so với
việc trả giá mỗi đêm. Trong bộ dữ liệu này hai con số tình cờ gần nhau (2,726 và
2,945 đều làm tròn lên thành 3 ngày) nên lựa chọn trùng nhau; nếu đuôi phân bố
kéo tới 30 ngày thì khác biệt là 4 ngày tính lại mỗi đêm so với 31 ngày.

---

## 3 · Kiểu dữ liệu cột `priority` thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Từ 2026-08-10 team backend đổi `priority` từ số sang chuỗi. Pipeline không dừng, `dbt test` vẫn 9/9 pass, nhưng `silver_tickets.priority` có 6.606 hàng sai (NULL hoặc ngoài miền 1..4) và model phân loại kém hẳn từ đúng ngày đó. |
| **Nguyên nhân** | Hai lớp phòng thủ đều đang tắt, nên một thay đổi hợp lệ ở nguồn đi thẳng vào Gold mà không gặp cửa kiểm nào. (1) `contract: enforced: false` — dbt không đối chiếu kiểu dữ liệu thực tế với kiểu đã cam kết. (2) Bước chuẩn hoá dùng `try_cast(priority_raw as integer)`, biểu thức này **sai theo hai hướng ngược nhau cùng lúc**: nó biến toàn bộ nhãn chữ hợp lệ (`urgent`/`high`/`medium`/`low` — 7.142 hàng dữ liệu tốt) thành NULL, đồng thời chấp nhận `'0'`, `'5'`, `'-1'` vì chúng đúng là số nguyên dù contract chỉ cho phép 1..4. Không có test miền giá trị nào trên cột này nên bộ test vẫn xanh. Kết quả là **silent data corruption**: pipeline báo khoẻ trong khi hơn một nửa nhãn huấn luyện đã hỏng. |
| **Ba nhóm giá trị và cách xử lý** | **Nhóm 1 — số 1..4** (6.846 hàng): đúng contract cũ → giữ nguyên. **Nhóm 2 — `urgent`/`high`/`medium`/`low`** (7.142 hàng): đây là *schema evolution*, nguồn đổi cách biểu diễn nhưng ý nghĩa không đổi → **map** về 1/2/3/4 theo tài liệu API. **Nhóm 3 — `P1` `P2` `unknown` `0` `5` `-1` `''` NULL** (312 hàng): dữ liệu hỏng thật → **quarantine**. Tiêu chí phân biệt nhóm 2 với nhóm 3: giá trị đó có mang đúng thông tin của contract cũ, chỉ khác cách biểu diễn hay không. Xử lý nhóm 2 như nhóm 3 sẽ vứt bỏ một nửa dữ liệu tốt chỉ vì nguồn đổi format. |
| **Cách khắc phục** | `dbt/macros/normalize_priority.sql`: thay `try_cast` bằng `CASE` xử lý đủ ba nhóm (kèm điều kiện `between 1 and 4` để chặn số ngoài miền), và điền `priority_reject_reason` phân biệt bốn loại lỗi. `dbt/models/silver/silver_tickets.sql`: **lọc bản ghi hỏng trước, `row_number` sau** — thêm CTE `valid` đứng trước CTE `ranked`. `dbt/models/silver/quarantine_tickets.sql`: `where {{ normalize_priority('priority_raw') }} is null`, dùng đúng macro mà Silver dùng nên hai bảng không thể lệch nhau. `dbt/models/silver/schema.yml`: `enforced: true` + test `not_null` và `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng, đúng grain (312 cặp `ticket_id`+`cdc_seq` phân biệt), khớp đúng 312 bản ghi rác đếm được trực tiếp trên `bronze_tickets_cdc` · `silver_tickets` = 12.480 hàng / 12.480 ticket (không mất ticket nào) · `priority` phân bố 1:3.134 · 2:3.029 · 3:3.115 · 4:3.202, không còn NULL · `dbt test` 11/11 pass (bản gốc 9) |

Thứ tự lọc/xếp hạng quyết định số hàng: nếu chỉ thêm điều kiện lọc vào cuối
model, ticket nào có bản ghi **mới nhất** bị hỏng sẽ biến mất khỏi Silver và số
ticket tụt xuống 12.168. Ta loại **bản ghi** hỏng, không loại cả **ticket** —
ticket đó vẫn còn trạng thái hợp lệ từ lần cập nhật trước.

`contract` và `test` không thay thế được nhau: contract ràng buộc **kiểu**
(nó vẫn cho `priority = 99` đi qua vì 99 đúng là integer), test ràng buộc **miền
giá trị** (nó không chặn được việc cột đổi kiểu). Sự cố này cần cả hai.

**Câu hỏi thiết kế 1 — chặn ở Bronze hay Silver?** Ở **Silver**. Bronze phải là
bản sao trung thực của nguồn: nó là bằng chứng duy nhất để tái hiện sự cố và là
thứ duy nhất cho phép **replay** sau khi sửa logic. Nếu Bronze từ chối bản ghi
lỗi, thì hôm nay ta mất khả năng trả lời "nguồn thực sự đã gửi gì", còn ngày mai
— khi phát hiện `P1`/`P2` thật ra cũng là nhãn hợp lệ cần map — ta không còn dữ
liệu để chạy lại. Bronze nhận tất cả, Silver là nơi contract có hiệu lực.

**Câu hỏi thiết kế 2 — vì sao không để `dbt test` fail và dừng DAG?** Vì quy mô
không tương xứng: 312 bản ghi hỏng trên 14.300 bản ghi CDC (2,2%) không có quyền
chặn 12.480 ticket, 130.683 event và 31.200 chunk hoàn toàn bình thường đến tay
người dùng. Dừng DAG biến một sự cố **chất lượng dữ liệu cục bộ** thành một sự cố
**ngưng trệ toàn hệ thống**, và nó còn tạo áp lực ngược khiến người trực có xu
hướng tắt test cho pipeline chạy được. Tách riêng thì pipeline chạy tiếp, còn
bản ghi lỗi không bị bỏ qua im lặng: chúng nằm trong một bảng có tên, có
`reject_reason`, làm hàng đợi cho người trực xử lý.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | không làm |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Chạy pipeline hai lượt liên tiếp trên cùng dữ liệu rồi so số hàng và checksum. Mọi bảng `incremental` phải có `unique_key`; không có khoá thì phép ghi là `INSERT` và pipeline chỉ đúng đúng một lần duy nhất trong đời. |
| 2 | Xem bộ lọc incremental lấy watermark theo **event time** hay **ingestion time**, rồi đo phân bố độ lệch giữa hai mốc đó. Lọc theo event time mà không có lookback là mất dữ liệu muộn, im lặng và không hồi phục được. |
| 3 | Kiểm tra bộ test có ràng buộc **miền giá trị** không, hay chỉ có `unique`/`not_null`. Một pipeline "xanh" chỉ chứng minh là nó chạy xong, không chứng minh dữ liệu còn đúng — nhất là khi nguồn có quyền đổi schema mà không cần ta đồng ý. |

---

## Phụ lục · Môi trường chạy

Máy chạy bài này là Windows, không có `make` và không có `python3`, nên
Makefile không dùng được. Các script không phụ thuộc shell (`tools/run_pipeline.py`
gọi dbt qua Python API), nên chạy trực tiếp tương đương:

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements.txt
PYTHONUTF8=1 .venv/Scripts/python seed/generate.py        # = make setup
PYTHONUTF8=1 .venv/Scripts/python tools/run_pipeline.py   # = make pipeline
PYTHONUTF8=1 .venv/Scripts/python tools/verify.py         # = make verify
```

`PYTHONUTF8=1` là bắt buộc: console Windows mặc định cp1252 nên các script in
tiếng Việt sẽ dừng vì `UnicodeEncodeError`.

Ngoài ra `expected/dashboard_baseline.json` được commit sẵn trong repo, nên
`tools/verify.py` luôn cho rằng dữ liệu bài mở rộng đã tồn tại và đi query
`data/gold_events/*.parquet`; khi chưa chạy `make seed-extra` thì `verify` **dừng
bằng IOException** ngay trước phần tổng kết. Đã xử lý bằng cách chạy
`seed/generate.py --extra` để sinh thư mục đó (không đụng vào `expected/`).
