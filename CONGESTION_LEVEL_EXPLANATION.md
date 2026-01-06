# Giải thích: Mức độ tắc (Congestion Level) & Trạng thái (Congestion Status)

## Vấn đề gặp: Mức độ tắc hiển thị NaN%

### Nguyên nhân
Trong screenshot, trường "Mức độ tắc" hiển thị `NaN%`. Điều này xảy ra vì:

1. **Frontend code** (`backend/templates/index.html`, dòng ~368) cố tính:
   ```javascript
   <strong>${(road.congestion_level * 100).toFixed(0)}%</strong>
   ```

2. **Nhưng** document trong MongoDB `predictions` collection **KHÔNG CÓ** trường `congestion_level`.
   - Document chỉ có: `timestamp`, `road_id`, `road_name`, `speed`, `vehicle_count`, `lat`, `lon`, `prediction`, `congestion_status`
   - Khi JavaScript truy cập `road.congestion_level`, nó là `undefined`
   - `undefined * 100` → `NaN`

### Giải pháp (đã áp dụng)
**Thay đổi**: Dùng trường `prediction` thay vì `congestion_level`
```javascript
// Trước (sai):
<strong>${(road.congestion_level * 100).toFixed(0)}%</strong>

// Sau (đúng):
<strong>${(road.prediction * 100).toFixed(0)}%</strong>
```

---

## Cách tính Mức độ tắc (Congestion Level / Prediction)

### Luồng tính toán

#### 1. **Input Data** (từ `traffic_data` MongoDB collection)
Mỗi bản ghi raw từ data generator (hoặc TomTom API) chứa:
- `speed` (km/h) — tốc độ trung bình trên con đường
- `vehicle_count` (số chiếc) — lượng xe
- `timestamp` — thời gian
- `road_id` — mã tuyến đường
- `congestion_level` (nếu generator mô phỏng) — giá trị mục tiêu (0-1) để model học

#### 2. **Feature Engineering** (trong `prediction_pyspark.py`)
Spark job trích xuất & tạo các đặc trưng (features) từ dữ liệu raw:

| Feature | Mô tả | Cách tính |
|---------|-------|----------|
| `speed` | Tốc độ trung bình | Từ input (cast to double) |
| `vehicle_count` | Số lượng xe | Từ input (cast to double) |
| `hour` | Giờ trong ngày | `hour(timestamp)` → 0-23 |
| `is_peak` | Giờ cao điểm? | 1 nếu hour ∈ {7,8,9,17,18,19}, else 0 |
| `day_of_week` | Thứ trong tuần | `dayofweek(timestamp)` → 1-7 (1=Sunday) |
| `is_weekend` | Cuối tuần? | 1 nếu day_of_week ∈ {1,7}, else 0 |
| `hour_sin`, `hour_cos` | Mã hóa cyclic của giờ | `sin(2π/24 × hour)`, `cos(2π/24 × hour)` |
| `speed_lag` | Tốc độ bước trước (per road) | `lag(speed, 1)` over road_id partition |
| `speed_change` | Thay đổi tốc độ | `speed - speed_lag`, or 0 if null |
| `vehicle_count_lag` | Số xe bước trước | `lag(vehicle_count, 1)` |
| `vehicle_count_change` | Thay đổi số xe | `vehicle_count - vehicle_count_lag` |
| `avg_speed_road` | Tốc độ trung bình gần đây per road | `avg(speed)` over last 5 rows per road |
| `speed_normalized` | Tốc độ chuẩn hóa | `speed / avg_speed_road` (so với trung bình) |

#### 3. **Model Training** (RandomForest Regressor)
Khi có đủ dữ liệu (≥ 200 bản ghi), Spark:
- Gom các features vào một vector (`VectorAssembler`)
- Huấn luyện RandomForest với 200 cây, độ sâu tối đa 15
- Model học mối quan hệ giữa features và giá trị mục tiêu (target = `congestion_level` từ raw data)
- Lưu model xuống `models/traffic_pipeline/` để tái sử dụng

#### 4. **Prediction (Dự đoán)** (mỗi 5 giây)
Cho dữ liệu mới nhất (per road_id):
- Áp dụng feature engineering như bước 2
- Input features → RandomForest model → **Output: `prediction` (giá trị 0-1)**
  - 0 = không tắc (đường thông thoáng)
  - 1 = tắc nặng (đường nguy hiểm)
- Công thức visualization:
  ```
  Mức độ tắc (%) = prediction × 100
  ```
  Ví dụ:
  - prediction = 0.85 → 85% tắc → NGUY HIỂM
  - prediction = 0.55 → 55% tắc → TẮC
  - prediction = 0.30 → 30% tắc → THÔNG THOÁNG

#### 5. **Status Conversion** (UDF trong Spark)
Spark UDF `determine_status(prediction)` chuyển giá trị dự đoán thành nhãn trạng thái:
```python
def determine_status(prediction):
    if prediction > 0.7:
        return "NGUY HIỂM"     # Dangerous, very congested
    elif prediction > 0.4:
        return "TẮC"           # Warning, moderately congested
    else:
        return "THÔNG THOÁNG"  # Safe, free flow
```

Ngưỡng (thresholds):
- **NGUY HIỂM** (danger): prediction > 70%
- **TẮC** (warning): 40% < prediction ≤ 70%
- **THÔNG THOÁNG** (safe): prediction ≤ 40%

#### 6. **Output to MongoDB**
Spark ghi vào `predictions` collection:
```json
{
  "timestamp": "2026-01-06T16:01:07.818Z",
  "road_id": "HN_12",
  "road_name": "Núi Trúc",
  "speed": 21.0,
  "vehicle_count": 196.0,
  "lat": 21.0242,
  "lon": 105.8342,
  "prediction": 0.4898,         // ← Đây là % tắc (0-1)
  "congestion_status": "TẮC"    // ← Từ UDF determine_status()
}
```

#### 7. **Frontend Display**
Khi lấy từ API `/api/current-predictions`:
```javascript
// Hiển thị % tắc
muc_do_tac = (road.prediction * 100).toFixed(0) + "%"
// Ví dụ: 0.4898 × 100 = 48.98% → "49%"

// Hiển thị trạng thái
trang_thai = road.congestion_status  // "TẮC"

// Hiển thị trên bản đồ: 
// - Màu sắc theo status (đỏ/vàng/xanh)
// - Marker hiển thị số xe
// - Popup hiển thị chi tiết
```

---

## Sơ đồ luồng

```
Raw Data (traffic_data)
    ↓
    ├─ speed, vehicle_count, timestamp, ...
    └─ congestion_level (mục tiêu để model học)

Feature Engineering
    ↓
    ├─ hour, is_peak, day_of_week, is_weekend
    ├─ hour_sin, hour_cos (cyclic encoding)
    ├─ speed_lag, speed_change
    ├─ vehicle_count_lag, vehicle_count_change
    └─ speed_normalized

VectorAssembler
    ↓
    └─ Features → Dense Vector

RandomForest Model (trained)
    ↓
    └─ Input: Dense Vector → Output: prediction (0-1)

Determine Status UDF
    ↓
    ├─ prediction > 0.7 → "NGUY HIỂM" (red)
    ├─ 0.4 < prediction ≤ 0.7 → "TẮC" (yellow)
    └─ prediction ≤ 0.4 → "THÔNG THOÁNG" (green)

MongoDB predictions collection
    ↓
    ├─ prediction (double, 0-1)
    ├─ congestion_status (string)
    └─ (other fields: timestamp, road_id, speed, ...)

Flask API /api/current-predictions
    ↓
    └─ Return JSON with prediction, congestion_status

Frontend Dashboard (index.html)
    ↓
    ├─ Mục độ tắc: (road.prediction × 100).toFixed(0) + "%"
    ├─ Trạng thái: road.congestion_status
    ├─ Markers: Leaflet map với màu sắc per status
    └─ Sidebar: danh sách tuyến, thống kê
```

---

## Tóm tắt nhanh

| Thuật ngữ | Giải thích | Giá trị | Nguồn |
|-----------|-----------|--------|-------|
| `congestion_level` | Mục tiêu dạy model | 0-1 | `traffic_data` (input từ generator/API) |
| `prediction` | Output của RandomForest model (% tắc) | 0-1 | `predictions` (output Spark) |
| `congestion_status` | Nhãn trạng thái từ UDF | NGUY HIỂM / TẮC / THÔNG THOÁNG | `predictions` (output Spark) |
| Mức độ tắc (%) | Hiển thị trên UI | 0-100 | `prediction × 100` |

**Fix đã áp dụng:**
- ✅ Template HTML đã đổi từ `road.congestion_level` → `road.prediction`
- ✅ NaN% sẽ không còn xuất hiện
- ✅ Giải thích thêm comment trong Spark code về field nào được ghi vào DB

---

## Testing

Kiểm tra nhanh:
```bash
# 1. Kiểm tra MongoDB có prediction documents không
mongo mongodb://localhost:27017/traffic_db
> db.predictions.findOne()  # Xem có trường `prediction` không

# 2. Kiểm tra API
curl http://localhost:5000/api/current-predictions | jq '.data[0]'
# Xem .prediction field có giá trị 0-1 không

# 3. Mở dashboard
# http://localhost:5000/
# Kiểm tra "Mức độ tắc" có hiển thị % bình thường không (không NaN)
```
