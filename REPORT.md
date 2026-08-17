# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** …  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

> Trước khi làm bài này em chưa dùng dbt bao giờ, nên em đọc mục 0b của
> GUIDE.md trước rồi mới bắt tay vào sửa. Cách làm của em là: chạy
> `make verify` để xem hỏng ở đâu, tìm hiểu vì sao hỏng, sửa một nhiệm vụ,
> rồi chạy lại verify để chắc là không làm hỏng phần đã xong.

---

## 0 · Kết quả `make verify`

<details>
<summary>Output ba lượt chạy</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 57.8s
  run 2/3 … 63.2s
  run 3/3 … 63.5s

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
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
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

Tổng kết: **4 / 4 tiêu chí đạt**. Em làm thêm cả hai bài mở rộng, kết quả ở
mục 4.

<details>
<summary>Trạng thái lúc em mới nhận repo — <b>1 / 4</b></summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 60.7s
  run 2/3 … 59.4s
  run 3/3 … 61.4s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✗ FAIL            38,750      12,480   ✗ thừa 26,270 hàng
  gold_feature_daily    ✓ ok               8,645       9,100   ✗ thiếu 455 hàng
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                   0         312   ✗ thiếu 312 hàng

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     7c461563f4    d11657ff21    2b76a4f850   ✗
  gold_feature_daily    4eee63cd82    4eee63cd82    4eee63cd82   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    empty         empty         empty        ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 9/9 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✗ 6,606 hàng sai
  quarantine_tickets đúng số bản ghi lỗi      ✗ 0 / 312
  gold_training_set: 1 hàng / 1 ticket        ✗ 12,480 ticket bị lặp
```

Chỗ em thấy rõ nhất là ba checksum của `gold_training_set` khác nhau ở cả ba
lượt. Cùng một dữ liệu đầu vào mà ra ba kết quả khác nhau, nên chắc chắn lỗi
nằm ở cách ghi chứ không phải ở dữ liệu.

</details>

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau ba lượt chạy, `gold_training_set` có 38.750 hàng trong khi đúng ra chỉ có 12.480. Checksum ba lượt khác nhau hoàn toàn, và có 12.480 ticket bị lặp. Điều làm em thấy khó là pipeline không báo lỗi gì cả, `dbt test` vẫn 9/9 pass. |
| **Nguyên nhân** | Model để `materialized='incremental'` nhưng không khai `unique_key`. Không có khoá thì dbt không biết hàng nào ở bảng đích là "cùng một hàng" với hàng mới, nên nó chỉ sinh ra câu `INSERT` (em có kiểm chứng bằng cách đọc SQL thật dbt gửi xuống DuckDB, xem phụ lục A.5). Mệnh đề `where` theo `run_date` thì chỉ giới hạn phần dữ liệu **đọc** từ nguồn, nó không quyết định gì về việc **ghi** vào bảng đích. Vì vậy chạy lại cùng một ngày lần thứ hai là ghi thêm chứ không ghi đè. Bản thân phép ghi đã không idempotent, nên mọi thứ chạy lại ở tầng trên — `retries=2` của Airflow, thao tác Clear Task, `catchup=True` khi DAG bật lại — đều thành ra thêm một bản sao đầy đủ. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_training_set.sql` em thêm `unique_key='ticket_id'` và `incremental_strategy='merge'` vào `config()`, giữ nguyên mệnh đề `where` như đề bài dặn. Trong `dags/ai_training_pipeline.py` em đặt `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | trước: 38.750 hàng, 12.480 ticket lặp · sau: 12.480 hàng, không ticket nào lặp · checksum ba lượt đều là `8dd7c98653` |

**Vì sao em chọn `merge` chứ không phải `delete+insert`.** Lúc đầu em định
dùng `delete+insert` theo partition ngày, vì nghe có vẻ hợp lý là "xoá ngày đó
đi rồi ghi lại". Nhưng em chạy thử `select op, count(*) from bronze_tickets_cdc
group by 1` thì được `c` 12.735 · `d` 255 · `u` 1.310, tức là nguồn có bản ghi
cập nhật. Em kiểm tra tiếp xem các bản ghi của một ticket có nằm cùng một ngày
không:

```sql
select count(*) from (
  select ticket_id from bronze_tickets_cdc
  group by 1 having count(distinct _ingested_at::date) > 1
);
-- 1310
```

Cả 1.310 ticket bị cập nhật đều có bản ghi rải ở từ hai ngày trở lên. Vậy nếu
dùng `delete+insert` theo partition ngày thì xoá partition ngày D2 xong, bản cũ
của ticket đó ở ngày D1 vẫn nằm nguyên trong bảng và vẫn bị lặp. Bảng này là
bảng entity (1 hàng / 1 ticket) chứ không phải bảng event, khoá tự nhiên là
`ticket_id`, nên em nghĩ `merge` theo khoá mới là cái đúng.

Em cũng muốn nói rõ là hai tham số của DAG **không phải nguyên nhân gốc**.
Chúng chỉ làm lỗi ít bị kích hoạt hơn thôi: kể cả khi không ai bấm Clear Task
và không có catchup, chỉ cần một lần `retries` của Airflow là bảng lại nhân đôi,
vì lỗi nằm ở chính phép ghi.

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng có 8.645 hàng, thiếu 455 hàng so với 9.100, tức khoảng 5%. Chỗ lạ là bảng này vẫn được báo `ỔN ĐỊNH ✓`. Chạy lại bao nhiêu lần cũng ra đúng một kết quả, chỉ có điều kết quả đó sai. |
| **P99 độ trễ đo được** | **2,726 ngày** *(p50 = 0,128 · p95 = 1,814 · max = 2,945 · 5,05% bản ghi tới kho muộn hơn một ngày)* |
| **Lookback đã chọn** | **3 ngày**, làm tròn lên từ P99 = 2,726. Em kiểm tra thêm bằng phân bố theo ngày lịch: trễ 0 ngày có 108.862 bản ghi, 1 ngày 14.165, 2 ngày 3.842, 3 ngày 2.593, và không có bản ghi nào trễ quá 3 ngày. |
| **Nguyên nhân** | Điều kiện lọc đang là `event_date > (select max(event_date) from {{ this }})`, tức là lấy mốc theo **thời điểm sự kiện xảy ra**. Nhưng thứ quyết định một bản ghi đã có mặt trong kho hay chưa lại là **thời điểm nó tới kho**, và hai mốc này lệch nhau tới 3 ngày với khoảng 5% dữ liệu. Chỉ cần một event của ngày mới được nạp là `max(event_date)` nhảy lên ngày đó, và tất cả những ngày nhỏ hơn hoặc bằng mốc coi như đóng luôn. Bản ghi của ngày cũ về sau không còn lượt chạy nào để lọt vào nữa. Em nghĩ điểm khó chịu nhất của lỗi này là không có hàng nào sai cả — chúng chỉ đơn giản là không bao giờ được xử lý, nên không test nào bắt được. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_feature_daily.sql` em đổi điều kiện thành `event_date >= (select max(event_date) from {{ this }}) - interval 3 day`, và thêm `unique_key=['event_date','customer_id']` cùng `incremental_strategy='merge'`. |
| **Bằng chứng** | trước: 8.645 hàng · sau: 9.100 hàng (đúng bằng 14 ngày × 650 customer) · checksum ba lượt đều là `3db448685c` |

Ban đầu em định chỉ đổi `>` thành `>=` cho nhanh, nhưng làm vậy chỉ nới thêm
đúng một ngày, không đủ cho dữ liệu trễ 3 ngày.

Một chỗ nữa em suýt bỏ sót: khi window rộng ra thì cùng một cặp
`(event_date, customer_id)` sẽ được tính lại ở nhiều lượt chạy. Nếu không thêm
`unique_key` thì các lần tính sẽ cộng dồn, tức là em vô tình tạo lại đúng lỗi
của nhiệm vụ 1 trên một bảng khác. Grain ở đây gồm hai cột nên `unique_key`
phải viết dưới dạng list.

**Vì sao em lấy P99 làm căn cứ chứ không lấy `max`?** Vì chi phí của lookback
không phải trả một lần mà trả ở **mọi lượt chạy sau này**: lùi N ngày nghĩa là
mỗi đêm phải tính lại N+1 ngày dữ liệu, mãi mãi. `max` chỉ là một quan sát đơn
lẻ, nó có thể là một sự cố cá biệt của tháng trước, mà lại kéo chi phí thường
xuyên của pipeline lên theo. P99 thì phát biểu được thành một cam kết ("bắt đủ
99% dữ liệu về muộn"), phần đuôi còn lại nếu cần thì backfill tay.

Em cũng thành thật là trong bộ dữ liệu này hai con số quá gần nhau (2,726 và
2,945 đều làm tròn lên thành 3), nên chọn kiểu nào cũng ra 3 ngày. Sự khác biệt
chỉ rõ nếu đuôi phân bố kéo dài, ví dụ max là 30 ngày thì lookback theo P99 là
4 ngày còn theo max là 31 ngày, chênh nhau rất nhiều.

---

## 3 · Kiểu dữ liệu cột `priority` thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Từ 2026-08-10 team backend đổi `priority` từ số sang chuỗi. Pipeline không dừng, `dbt test` vẫn 9/9 pass, nhưng `silver_tickets.priority` có tới 6.606 hàng sai (NULL hoặc nằm ngoài 1..4). |
| **Nguyên nhân** | Có hai lớp kiểm tra nhưng cả hai đều đang tắt, nên một thay đổi ở nguồn đi thẳng vào Gold mà không gặp cửa nào. Thứ nhất, `contract: enforced: false` nên dbt không đối chiếu kiểu dữ liệu thật với kiểu đã khai trong `schema.yml`. Thứ hai, bước chuẩn hoá dùng `try_cast(priority_raw as integer)`, và biểu thức này sai theo hai hướng ngược nhau cùng lúc: nó biến toàn bộ nhãn chữ hợp lệ (`urgent`/`high`/`medium`/`low`, 7.142 bản ghi tốt) thành NULL, mà đồng thời lại chấp nhận `'0'`, `'5'`, `'-1'` vì chúng đúng là số nguyên, dù contract chỉ cho phép 1..4. Trên cột này không có test nào kiểm tra miền giá trị nên bộ test vẫn xanh. Kết quả là dữ liệu hỏng âm thầm: nhìn vào pipeline thì thấy mọi thứ bình thường, trong khi hơn một nửa nhãn dùng để huấn luyện đã sai. |
| **Ba nhóm giá trị và cách xử lý** | **Nhóm 1 — số 1..4** (6.846 bản ghi): đúng contract cũ, giữ nguyên. **Nhóm 2 — `urgent`/`high`/`medium`/`low`** (7.142 bản ghi): đây là schema evolution, nguồn đổi cách viết nhưng ý nghĩa không đổi, nên phải map về 1/2/3/4 theo tài liệu API. **Nhóm 3 — `P1` `P2` `unknown` `0` `5` `-1` `''` NULL** (312 bản ghi): hỏng thật, đưa vào quarantine. Câu hỏi em dùng để phân biệt nhóm 2 với nhóm 3 là: giá trị này có mang đúng thông tin của contract cũ, chỉ khác cách biểu diễn không? Có thì map, không thì quarantine. |
| **Cách khắc phục** | Bốn chỗ: `dbt/macros/normalize_priority.sql` thay `try_cast` bằng `CASE` xử lý đủ ba nhóm, có thêm điều kiện `between 1 and 4` để chặn số ngoài miền, và điền `priority_reject_reason` cho biết lý do bị loại. `dbt/models/silver/silver_tickets.sql` thêm CTE `valid` đứng **trước** CTE `ranked`. `dbt/models/silver/quarantine_tickets.sql` đổi `where false` thành `where {{ normalize_priority('priority_raw') }} is null`. `dbt/models/silver/schema.yml` bật `enforced: true` và thêm test `not_null` + `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` có 312 hàng, đúng grain (312 cặp `ticket_id`+`cdc_seq` phân biệt), và khớp đúng với 312 bản ghi rác em đếm độc lập trên `bronze_tickets_cdc` · `silver_tickets` giữ đủ 12.480 hàng / 12.480 ticket · `priority` phân bố 1:3.134 · 2:3.029 · 3:3.115 · 4:3.202, không còn NULL · `dbt test` 11/11 pass (bản gốc có 9 test) |

**Chỗ em suýt làm sai.** Phản xạ đầu tiên của em là thêm điều kiện lọc vào cuối
model `silver_tickets`, tức xếp hạng trước rồi mới lọc. Trong file có cảnh báo
về chuyện này nên em viết một query để kiểm chứng xem cách đó sai bao nhiêu:

```sql
-- lọc SAU khi xếp hạng: ticket nào có bản ghi mới nhất bị hỏng thì mất luôn
with ranked as (
  select *, row_number() over (partition by ticket_id
                               order by event_time desc, cdc_seq desc) as rn
  from bronze_tickets_cdc
)
select count(*) from ranked
where rn = 1 and op <> 'd' and <macro chuẩn hoá> is not null;
-- 12168   (đúng ra phải là 12480)
```

Mất 312 ticket, đúng bằng số bản ghi hỏng. Nên phải lọc bản ghi hỏng trước rồi
mới `row_number`, vì mình loại **bản ghi** chứ không loại cả **ticket** — ticket
đó vẫn còn trạng thái hợp lệ từ lần cập nhật trước đó.

Em cũng dùng chung một macro cho cả `silver_tickets` và `quarantine_tickets`.
Như vậy hai bảng không thể lệch nhau: bản ghi nào bị loại khỏi Silver thì chắc
chắn xuất hiện ở quarantine.

Một điều em rút ra được: `contract` và `test` không thay thế cho nhau.
`contract` ràng buộc **kiểu** dữ liệu, nhưng nó vẫn cho `priority = 99` đi qua
vì 99 đúng là integer. Còn `test` ràng buộc **miền giá trị**, nhưng nó không
chặn được việc cột đổi kiểu. Sự cố này cần cả hai.

**Câu hỏi thiết kế 1 — nên chặn ở Bronze hay Silver?** Em chọn Silver. Bronze
nên là bản sao trung thực của nguồn, vì đó là thứ duy nhất giúp mình trả lời
được câu "nguồn thực sự đã gửi cái gì". Nếu Bronze từ chối bản ghi lỗi thì khi
điều tra sự cố mình không còn gì để xem, và quan trọng hơn là không replay được:
giả sử sau này phát hiện `P1`/`P2` cũng là nhãn hợp lệ cần map, mình sẽ không
còn dữ liệu để chạy lại. Nên Bronze nhận hết, Silver là nơi contract có hiệu lực.

**Câu hỏi thiết kế 2 — vì sao không để `dbt test` fail và dừng DAG?** Em nghĩ
là do quy mô không tương xứng. 312 bản ghi hỏng trên tổng 14.300 bản ghi CDC,
tức khoảng 2,2%, mà lại chặn 12.480 ticket, 130.683 event và 31.200 chunk hoàn
toàn bình thường không đến được tay người dùng. Như vậy là biến một sự cố chất
lượng dữ liệu ở phạm vi nhỏ thành sự cố ngưng trệ cả hệ thống. Em nghĩ thêm là
nếu để DAG dừng vì mấy trăm hàng lỗi thì lâu dài người trực sẽ có xu hướng tắt
test đi cho pipeline chạy được, và như thế còn tệ hơn. Tách riêng thì pipeline
vẫn chạy, mà bản ghi lỗi cũng không bị bỏ qua im lặng: chúng nằm trong một bảng
có tên, có cột `reject_reason` để người trực biết phải làm gì.

---

## 4 · *(mở rộng)* Bài trong EXTRA.md — em làm cả hai bài

### 4A · Query dashboard chậm

| | |
|---|---|
| **Triệu chứng** | Dashboard load 38 giây, ba tháng trước chỉ 2 giây, mà không ai sửa dòng code nào. Em đo bằng `make explain` thì thấy `rows scanned` = 5.000.000 cho một tập dữ liệu chỉ có 130.683 hàng thật. |
| **Nguyên nhân** | Có hai chuyện cộng lại, và cùng dẫn tới một kết quả là engine không có cách nào biết file nào vô ích **trước khi mở file đó ra**. Thứ nhất là small-file problem: dataset gồm 5.000 file, em đo thì mỗi file chỉ 2,3–4,0 KB (trung bình 3,2 KB, khoảng 26 hàng), và không partition. Theo docstring của `tools/explain.py` thì DuckDB đọc Parquet theo lô và làm tròn lên theo từng file, một file mấy chục hàng vẫn tốn công quét như khoảng 1.000 hàng — 5.000 file tí hon thành ra 5.000.000 đơn vị công quét, khớp với con số đo được. Thứ hai là điều kiện lọc viết dạng `strftime(event_time, '%Y-%m-%d') = '2026-08-09'`, cột bị bọc trong một lời gọi hàm nên engine không so được nó với tên thư mục partition, cũng không so được với min/max của row group, đành phải đọc hết rồi mới lọc. Chi phí này tăng theo số file, nên tuy "không ai sửa gì" mà query vẫn chậm dần — thứ thay đổi là số file tích lại theo thời gian. |
| **Cách khắc phục** | Viết `tools/compact.py`: `COPY ... TO 'data/gold_events_v2' (format parquet, partition_by (event_date), row_group_size 2048)` với `order by customer_name, event_time`. Rồi sửa `queries/dashboard.sql` trỏ vào dataset mới, bật `hive_partitioning`, và đổi điều kiện thành `event_date = date '2026-08-09'` để cột đứng một mình ở một vế. |
| **Bằng chứng** | `rows scanned` **5.000.000 → 9.324**, tức giảm **536,3×** (yêu cầu ≥ 10×) · `files` **5.000 → 14** · `result hash` vẫn là `4379e4c5d9f3`, không đổi · thời gian 10,5 ms |

**Ba quyết định em phải cân nhắc khi viết `compact.py`:**

*Partition theo `event_date` chứ không theo `customer_name`.* Mỗi giá trị phân
biệt sẽ thành một thư mục. `event_date` có 14 giá trị nên được 14 thư mục,
engine bỏ qua 13 thư mục mà không cần mở file nào. Còn `customer_name` có tới
650 giá trị, partition theo cột đó thì được 650 thư mục toàn file bé tí, tức là
em tạo lại đúng cái small-file problem đang phải chữa.

*Sắp xếp theo `customer_name`.* Cột không nằm trong đường dẫn thì vẫn lọc được,
nhưng bằng cơ chế khác là min/max của từng row group. Thống kê đó chỉ có ích khi
các hàng của cùng một khách nằm gần nhau. Em đọc thử metadata của 5 file trong
dataset cũ thì thấy đúng là vô dụng — file nào cũng khoảng 30 hàng và khoảng giá
trị đều bắt đầu từ `ACME`:

```
part-00000.parquet   30 hàng   ACME -> Cust_0608
part-00001.parquet   33 hàng   ACME -> Cust_0483
part-00002.parquet   27 hàng   ACME -> Cust_0528
part-00003.parquet   35 hàng   ACME -> Cust_0629
part-00004.parquet   27 hàng   ACME -> Cust_0643
```

Khoảng nào cũng "có thể chứa ACME" nên engine không loại được file nào, buộc
phải mở hết. Sau khi sắp xếp lại, em đọc metadata của file ngày 08-09 thì các
khoảng đã tách hẳn ra:

```
row_group   rows   customer_name min → max
    0       2048   ACME       → ACME
    1       2048   ACME       → Cust_0071
    2       2048   Cust_0071  → Cust_0295      <- khoảng này không chứa ACME
    3       2048   Cust_0295  → Cust_0520      <- khoảng này không chứa ACME
    4       1190   Cust_0002  → Cust_0650
```

*Đặt `row_group_size` 2048 thay vì để mặc định 122.880.* Một ngày chỉ có khoảng
9.300 hàng, nên nếu để mặc định thì cả ngày nằm gọn trong một row group duy nhất,
min/max của nó phủ đủ 650 khách và lại thành vô dụng. Với 2048 thì mỗi ngày chia
được khoảng 5 row group, mỗi cái phủ tầm 140 khách.

Có một chỗ em chưa giải thích được hết: `rows scanned` dừng lại ở 9.324, đúng
bằng số hàng của một ngày. Nghĩa là phần giảm 536× là nhờ partition pruning,
còn phần lọc theo row group tuy min/max đã dùng được (như bảng trên) nhưng không
thấy thể hiện thêm trên chỉ số này. Em đoán là chỉ số `OPERATOR_ROWS_SCANNED`
đếm ở mức file chứ không ở mức row group, nhưng em chưa kiểm chứng được.

Em cũng có nghĩ tới việc thêm index, nhưng file Parquet nằm trên đĩa thì không
có index. Thứ duy nhất mình điều khiển được là file nằm ở đâu và hàng nằm theo
thứ tự nào trong file.

### 4B · Consumer gặp sự cố giữa batch

| | |
|---|---|
| **Triệu chứng** | `make crash-test` giết consumer ở lô 7 rồi khởi động lại. Trước khi sửa thì kết quả là **mất dữ liệu**: offset đã commit tới 3.500 trong khi kho mới ghi được 3.000 hàng, nên lần khởi động lại đọc tiếp từ 3.500 và 500 message của lô 7 mất luôn. Không có lỗi nào được báo. |
| **Nguyên nhân** | Trong `consume()`, thứ tự đang là `commit()` rồi mới `write_batch()`, tức là ghi nhận "đã xử lý xong" trước khi thực sự xử lý. Đó là ngữ nghĩa at-most-once: sự cố nào rơi vào khoảng giữa hai thao tác cũng thành mất dữ liệu, và mất im lặng, vì offset đã nói lô đó xong rồi nên không lần khởi động lại nào quay về đọc nó nữa. Theo bài giảng thì exactly-once không tồn tại ở tầng giao vận, cái mình chọn được chỉ là mất hay trùng. |
| **Cách khắc phục** | Trong `ingest/consumer.py` em đảo lại thành `write_batch()` rồi mới `commit()`, tức chuyển sang at-least-once. Nhưng chỉ đảo thứ tự thì mới đổi từ mất dữ liệu sang trùng dữ liệu, nên em phải làm phép ghi idempotent nữa: thêm `primary key` trên `event_id` trong `DDL` và đổi `INSERT` thành `INSERT ... ON CONFLICT (event_id) DO UPDATE SET ...`. Thiếu một trong hai là chưa đủ. |
| **Bằng chứng** | `make crash-test`: **ĐẠT ✓**. Crash ở lô 7, offset commit dừng ở 3.000, lần khởi động lại đọc lại từ đó và ghi 17.000 message (thừa 500 hàng của lô 7 được phát lại), kết quả cuối là **20.000 hàng / 20.000 `event_id`**, bằng đúng lượt chạy không có sự cố. |

Để so được trước và sau, em chạy lại đúng kịch bản đó ở quy mô nhỏ hơn (5.000
message, vẫn `batch_size` 500 và vẫn giết ở lô 7):

```
                        offset commit    hàng đã ghi    kết quả sau restart
trước (commit -> write)      3.500           3.000      4.500  -> MẤT 500
sau   (write -> commit)      3.000           3.500      5.000  -> đủ, không trùng
```

Em thấy hai dòng này thể hiện rất rõ hai ngữ nghĩa, chỉ cần nhìn xem con số nào
lớn hơn. Offset chạy trước dữ liệu thì khoảng trống giữa hai thao tác thành dữ
liệu mất, không lấy lại được. Dữ liệu chạy trước offset thì khoảng trống đó
thành dữ liệu bị phát lại, mà phát lại thì `write_batch()` upsert đè lên nên tự
khử được.

**`DO UPDATE` khác `DO NOTHING` ở đâu khi message được phát lại với nội dung đã
đổi?** `DO NOTHING` sẽ giữ nguyên bản ghi cũ, còn `DO UPDATE` ghi đè bằng bản
mới. Nếu nguồn sửa lại một message rồi gửi lại dưới cùng `event_id` thì
`DO NOTHING` làm kho lệch khỏi nguồn vĩnh viễn mà không báo gì. Em chọn
`DO UPDATE`, vì ở đây `event_id` là khoá của một thực thể có thể được sửa, chứ
không phải mã của một lần gửi message.

**Một chuyện ngoài lề nhưng em nghĩ nên ghi lại.** Lần đầu chạy `make crash-test`
sau khi sửa, em đợi hơn 10 phút mà không xong, tưởng là code bị treo. Em thử đo
riêng thì thấy `executemany()` gửi từng hàng một, kèm `ON CONFLICT` thì chỉ được
khoảng 145 hàng/s. Em sửa lại thành gộp cả lô vào một câu lệnh
`INSERT ... VALUES (...), (...), …` thì lên khoảng 625 hàng/s (còn nhanh hơn cả
bản `INSERT` thuần ban đầu, khoảng 270 hàng/s), và crash-test chạy xong trong
khoảng một phút.

---

## 5 · Tổng kết

| Nhiệm vụ | Khi nhận một hệ thống chưa quen, em sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Chạy pipeline hai lượt liền trên cùng dữ liệu rồi so số hàng và checksum. Bảng `incremental` nào cũng phải có `unique_key`, không có khoá thì dbt chỉ sinh `INSERT` và pipeline chỉ đúng đúng một lần duy nhất. |
| 2 | Xem điều kiện lọc incremental lấy mốc theo **event time** hay **ingestion time**, rồi đo độ lệch giữa hai mốc đó. Lọc theo event time mà không có lookback thì mất dữ liệu về muộn, mất im lặng và không lấy lại được. |
| 3 | Xem bộ test có ràng buộc miền giá trị không hay chỉ có `unique`/`not_null`. Pipeline "xanh" chỉ chứng minh là nó chạy xong chứ không chứng minh dữ liệu còn đúng, nhất là khi nguồn có thể đổi schema mà không hỏi mình. |

---

## Phụ lục A · Các lệnh em dùng để đo

Số liệu ở mục 2 và 3 đều lấy từ các lệnh dưới đây, chạy trên `warehouse.duckdb`.

### A.1 · Nhiệm vụ 2 — phân bố độ trễ dữ liệu

```sql
select
  quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.50) as p50,
  quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.95) as p95,
  quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.99) as p99,
  max(date_diff('second', event_time, _ingested_at)/86400.0)                 as max_ngay,
  avg(case when _ingested_at > event_time + interval 1 day then 1.0 else 0 end) as ty_le_late
from bronze_events;
```

```
┌────────┬────────┬────────┬──────────┬────────────┐
│  p50   │  p95   │  p99   │ max_ngay │ ty_le_late │
├────────┼────────┼────────┼──────────┼────────────┤
│  0.128 │  1.814 │  2.726 │    2.945 │     0.0505 │
└────────┴────────┴────────┴──────────┴────────────┘
```

Phân bố theo ngày lịch, đây là căn cứ để em dừng lookback ở 3 ngày:

```
┌──────────┬────────┐
│ tre_ngay │   n    │
├──────────┼────────┤
│        0 │ 108862 │
│        1 │  14165 │
│        2 │   3842 │
│        3 │   2593 │
└──────────┴────────┘
```

### A.2 · Nhiệm vụ 3 — ba nhóm giá trị ở nguồn

```sql
select '[' || coalesce(priority_raw, '<NULL>') || ']' as val, count(*) as n
from bronze_tickets_cdc group by 1 order by n desc;
```

```
┌───────────┬───────┐          nhóm 1 (số hợp lệ)  : 1748+1710+1705+1683 =  6.846
│    val    │   n   │          nhóm 2 (nhãn chữ)   : 1845+1819+1783+1695 =  7.142
├───────────┼───────┤          nhóm 3 (rác)        :  312
│ [low]     │  1845 │                                ─────────────────────────────
│ [urgent]  │  1819 │          tổng bản ghi CDC    : 14.300
│ [medium]  │  1783 │
│ [4]       │  1748 │
│ [3]       │  1710 │
│ [1]       │  1705 │
│ [high]    │  1695 │
│ [2]       │  1683 │
│ [0]       │    49 │
│ []        │    43 │
│ [unknown] │    39 │
│ [P1]      │    39 │
│ [P2]      │    38 │
│ [5]       │    37 │
│ [<NULL>]  │    35 │
│ [-1]      │    32 │
└───────────┴───────┘
```

Em kiểm tra thêm mốc thời gian nguồn đổi format bằng
`count(*) filter (where try_cast(priority_raw as integer) is null)` theo ngày,
và thấy nó nhảy vọt đúng ngày 2026-08-10 như phiếu sự cố mô tả:

```
2026-08-03    0 /  897      2026-08-10    977 /  984
2026-08-04    6 /  927      2026-08-11   1065 / 1071
2026-08-05    8 / 1019      2026-08-12   1005 / 1007
2026-08-06   14 /  974      2026-08-13   1045 / 1051
2026-08-07   12 / 1012      2026-08-14   1053 / 1064
2026-08-08   20 / 1083      2026-08-15   1047 / 1058
2026-08-09   15 / 1054      2026-08-16   1069 / 1099
```

### A.3 · Nhiệm vụ 3 — `silver_tickets.priority` trước và sau

**Trước.** Lúc này em đã sửa xong rồi nên không xem lại bảng cũ được, em viết
lại đúng logic cũ (`try_cast`, xếp hạng trên toàn bộ bản ghi) để tái hiện:

```sql
with ranked as (
  select *, try_cast(priority_raw as integer) as p,
         row_number() over (partition by ticket_id
                            order by event_time desc, cdc_seq desc) as rn
  from bronze_tickets_cdc
)
select p as priority, count(*) as n
from ranked where rn = 1 and op <> 'd' group by 1 order by 1 nulls last;
```

```
┌──────────┬───────┐
│ priority │   n   │      tổng ticket : 12.480
├──────────┼───────┤      hàng sai    :  6.606   (NULL 6.488 + ngoài miền 118)
│       -1 │    32 │
│        0 │    49 │      -> khớp đúng dòng "silver_tickets.priority ∈ 1..4,
│        1 │  1458 │         không NULL  ✗ 6,606 hàng sai" của make verify
│        2 │  1454 │
│        3 │  1456 │      Tách 6.488 NULL ra thì có 6.294 là nhãn chữ HỢP LỆ
│        4 │  1506 │      bị try_cast vứt đi, chỉ 194 là chuỗi rác thật.
│        5 │    37 │
│     NULL │  6488 │
└──────────┴───────┘
```

Con số 6.606 tái hiện được khớp đúng với con số `make verify` báo lúc đầu, nên
em tin là mình đã hiểu đúng logic cũ.

**Sau:**

```
┌──────────┬───────┐
│ priority │   n   │      tổng : 12.480 hàng / 12.480 ticket
├──────────┼───────┤      không còn NULL, không còn giá trị ngoài 1..4
│        1 │  3134 │
│        2 │  3029 │      không mất ticket nào, vì bản ghi hỏng bị loại TRƯỚC
│        3 │  3115 │      row_number nên ticket lùi về trạng thái hợp lệ gần nhất
│        4 │  3202 │
└──────────┴───────┘
```

### A.4 · Nhiệm vụ 3 — đối chiếu quarantine

```
quarantine_tickets                              : 312 hàng / 312 cặp (ticket_id, cdc_seq)
đếm độc lập bản ghi rác trên bronze_tickets_cdc : 312
```

Em đếm bản ghi rác trực tiếp trên Bronze bằng một câu query riêng, không dùng
macro, để chắc là không phải mình tự kiểm tra bằng chính logic của mình. Hai con
số bằng nhau nghĩa là quarantine không thừa cũng không thiếu, và 7.142 nhãn chữ
đã được map chứ không bị bắt oan.

Phân loại lý do bị loại, để người trực đọc log là biết phải làm gì:

```
priority là số nhưng ngoài miền 1..4                               118
priority là chuỗi không có trong bảng quy đổi (urgent/high/…)      116
priority rỗng: chuỗi trắng                                          43
priority rỗng: nguồn không gửi giá trị                              35
                                                                   ───
                                                                   312
```

### A.5 · Nhiệm vụ 1 — SQL mà dbt thực sự gửi xuống DuckDB

GUIDE.md mục 0b có chỉ cách xem câu lệnh thật dbt sinh ra, em thấy đây là cách
kiểm chứng chắc chắn nhất nên có làm thử với cả hai phiên bản config.

Với config gốc (chỉ có `materialized='incremental'`):

```sql
insert into "warehouse"."main"."gold_training_set" ("ticket_id", "customer_id", …)
(
    select "ticket_id", "customer_id", …
    from "gold_training_set__dbt_tmp20260817123806947344"
)
```

Sau khi thêm `unique_key` và `incremental_strategy`:

```sql
MERGE INTO "warehouse"."main"."gold_training_set" AS DBT_INTERNAL_DEST
    USING "gold_training_set__dbt_tmp20260817114739354256" AS DBT_INTERNAL_SOURCE
    ON (DBT_INTERNAL_SOURCE.ticket_id = DBT_INTERNAL_DEST.ticket_id)
WHEN MATCHED     THEN UPDATE BY NAME
WHEN NOT MATCHED THEN INSERT BY NAME
```

Câu đầu không có chỗ nào nhắc tới khoá, nên chạy lại là cộng thêm. Câu sau có
`WHEN MATCHED THEN UPDATE`, nên chạy lại là ghi đè.

---

## Phụ lục B · Môi trường chạy

Máy em dùng là Windows và không có `make`, cũng không có `python3`, nên Makefile
chạy không được. May là các script không phụ thuộc shell (`tools/run_pipeline.py`
gọi dbt qua Python API chứ không gọi lệnh `dbt` ngoài terminal), nên em chạy
thẳng từng script:

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements.txt
PYTHONUTF8=1 .venv/Scripts/python seed/generate.py        # = make setup
PYTHONUTF8=1 .venv/Scripts/python tools/run_pipeline.py   # = make pipeline
PYTHONUTF8=1 .venv/Scripts/python tools/verify.py         # = make verify
```

`PYTHONUTF8=1` là bắt buộc, không có nó thì mọi script in tiếng Việt đều dừng vì
`UnicodeEncodeError` (console Windows mặc định dùng cp1252).

Vì `queries/dashboard.sql` giờ trỏ vào dataset v2 nên trên repo sạch phải dựng
dataset đó trước rồi mới đo được:

```bash
python seed/generate.py --extra   # = make seed-extra  (tạo data/gold_events/)
python tools/compact.py           # = make compact     (tạo data/gold_events_v2/)
python tools/verify.py            # = make verify
python tools/crash_test.py        # = make crash-test
```

Em cũng gặp một lỗi mà em nghĩ không phải do mình: `expected/dashboard_baseline.json`
có sẵn trong repo, nên `tools/verify.py` luôn cho rằng dữ liệu bài mở rộng đã tồn
tại và đi query `data/gold_events/*.parquet`. Khi chưa chạy `make seed-extra` thì
thư mục đó chưa có và `verify` dừng bằng `IOException` ngay trước phần tổng kết.
Em xử lý bằng cách chạy `seed/generate.py --extra` để sinh thư mục đó, và không
đụng gì vào `expected/`.
