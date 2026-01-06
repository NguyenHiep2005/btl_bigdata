import time
import requests
import random
import logging
import os
from datetime import datetime
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler

# 1. ĐIỀN API KEY
TOMTOM_API_KEY = "rIvFmAtm2hx9QVipvvOGo0OeFtloAm69" 

# 2. Cấu hình MongoDB
MONGO_URI = "mongodb://root:password@localhost:27017/?authSource=admin"

os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_generator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TrafficDataGenerator:
    def __init__(self):
        logger.info(" Khởi động bộ thu thập dữ liệu TomTom...")
        
        # Kết nối Database
        try:
            self.client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            self.client.server_info() 
            self.db = self.client['traffic_db']
            self.collection = self.db['traffic_data']
            logger.info("MongoDB connected thành công!")
        except Exception as e:
            logger.error(f" Lỗi kết nối MongoDB: {e}")
            raise

        # Danh sách 15 tuyến đường (tăng từ 5 lên 15)
        self.roads = [
            # Tuyến chính (5 tuyến)
            {'id': 'HN_1', 'name': 'Đinh Tiên Hoàng', 'lat': 21.0293, 'lon': 105.8527},
            {'id': 'HN_2', 'name': 'Phố Huế', 'lat': 21.0185, 'lon': 105.8505},
            {'id': 'HN_3', 'name': 'Tây Sơn', 'lat': 21.0081, 'lon': 105.8236},
            {'id': 'HN_4', 'name': 'Đường Láng', 'lat': 21.0135, 'lon': 105.8105},
            {'id': 'HN_5', 'name': 'Bà Triệu', 'lat': 21.0205, 'lon': 105.8490},
            # Tuyến phụ (10 tuyến mới)
            {'id': 'HN_6', 'name': 'Cầu Giấy', 'lat': 21.0089, 'lon': 105.7869},
            {'id': 'HN_7', 'name': 'Ngã Tư Sở', 'lat': 21.0194, 'lon': 105.8408},
            {'id': 'HN_8', 'name': 'Thanh Xuân', 'lat': 21.0070, 'lon': 105.8408},
            {'id': 'HN_9', 'name': 'Xã Đàn', 'lat': 21.0132, 'lon': 105.8398},
            {'id': 'HN_10', 'name': 'Quang Trung', 'lat': 21.0238, 'lon': 105.8291},
            {'id': 'HN_11', 'name': 'Thái Hà', 'lat': 21.0164, 'lon': 105.8289},
            {'id': 'HN_12', 'name': 'Núi Trúc', 'lat': 21.0242, 'lon': 105.8342},
            {'id': 'HN_13', 'name': 'Trường Chinh', 'lat': 21.0089, 'lon': 105.8475},
            {'id': 'HN_14', 'name': 'Tôn Đức Thắng', 'lat': 21.0236, 'lon': 105.8420},
            {'id': 'HN_15', 'name': 'Kim Mã', 'lat': 21.0247, 'lon': 105.8347},
        ]
    
    def get_real_traffic(self, lat, lon):
        """Gọi API TomTom lấy dữ liệu thật"""
        if "DÁN_API_KEY" in TOMTOM_API_KEY:
            logger.warning("Chưa điền API Key! Đang dùng dữ liệu giả lập...")
            return self.generate_fake_data()

        try:
            url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?key={TOMTOM_API_KEY}&point={lat},{lon}"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                flow = data.get('flowSegmentData', {})
                
                # tốc độ
                current_speed = flow.get('currentSpeed', 30) 
                free_flow_speed = flow.get('freeFlowSpeed', 30) 
                
                if free_flow_speed > 0:
                    congestion = 1 - (current_speed / free_flow_speed)
                else:
                    congestion = 0
                
                congestion = max(0.0, min(1.0, congestion))
                
                return int(current_speed), congestion
            else:
                logger.error(f"Lỗi API TomTom: {response.status_code}")
                return self.generate_fake_data()
                
        except Exception as e:
            logger.error(f"Lỗi mạng khi gọi API: {e}")
            return self.generate_fake_data()

    def generate_fake_data(self):
        """Hàm dự phòng khi mất mạng hoặc chưa có Key"""
        speed = random.randint(10, 50)
        congestion = random.uniform(0, 0.9)
        return speed, congestion

    def fetch_and_save(self):
        """Lấy dữ liệu thật và thêm biến động ngẫu nhiên theo xác suất"""
        try:
            logger.info("📡 Đang cập nhật dữ liệu từ API...")
            
            for road in self.roads:
                # 1. Lấy dữ liệu GỐC từ API TomTom
                real_speed, real_congestion = self.get_real_traffic(road['lat'], road['lon'])
                
                # --- SỬA ĐỔI: DÙNG XÁC SUẤT ĐỂ TẠO TÌNH HUỐNG ---
                
                # Sinh một số ngẫu nhiên từ 0.0 đến 1.0
                chance = random.random()
                
                if chance < 0.7:
                    # 70% trường hợp: Bình thường (Biến động nhẹ +/- 5km/h)
                    noise_speed = random.randint(-5, 5)
                elif chance < 0.9:
                    # 20% trường hợp: Đường đông (Giảm 10-20km/h)
                    noise_speed = random.randint(-20, -10)
                else:
                    # 10% trường hợp: TẮC ĐƯỜNG (NGUY HIỂM) - Giảm sâu 25-40km/h
                    # Đây chính là lúc "lâu lâu" mới xuất hiện đỏ
                    noise_speed = random.randint(-40, -25)

                # ---------------------------------------------
                
                # Tốc độ hiển thị = Tốc độ thật + Biến động
                display_speed = real_speed + noise_speed
                
                # Đảm bảo tốc độ tối thiểu là 1km/h
                display_speed = max(1, display_speed) 
                
                # Tính lại mức độ tắc nghẽn (Giả sử chuẩn là 40km/h)
                new_congestion = 1 - (display_speed / 40)
                new_congestion = max(0.0, min(1.0, new_congestion)) 
                
                # Số xe biến thiên theo độ tắc
                # Nếu tắc (new_congestion cao) -> xe đông và ngược lại
                base_vehicle = 20
                if new_congestion > 0.7: base_vehicle = 200 # Tắc thì đông xe
                elif new_congestion > 0.4: base_vehicle = 100
                
                vehicle_noise = random.randint(-10, 30)
                vehicle_count = int(base_vehicle + (new_congestion * 150)) + vehicle_noise

                # 3. Đóng gói dữ liệu
                data = {
                    'road_id': road['id'],
                    'road_name': road['name'],
                    'speed': float(display_speed),
                    'congestion_level': float(new_congestion),
                    'vehicle_count': vehicle_count,
                    'lat': road['lat'],
                    'lon': road['lon'],
                    'timestamp': datetime.now()
                }
                
                # 4. Lưu vào MongoDB
                self.collection.insert_one(data)
                
                # Log trạng thái 
                status = "🟢" if new_congestion < 0.4 else "🟡" if new_congestion < 0.7 else "🔴"
                logger.info(f"{status} {road['name']}: {display_speed}km/h (Gốc: {real_speed}) | Tắc: {int(new_congestion*100)}%")
            
            logger.info(f"✓ Đã lưu {len(self.roads)} bản ghi mới.")
            
        except Exception as e:
            logger.error(f"Lỗi trong quá trình xử lý: {e}")
    def start(self):
        """Chạy bộ lập lịch"""
        logger.info(" Bắt đầu thu thập (Chu kỳ: 2s/lần - tăng từ 5s)...")
        
        scheduler = BackgroundScheduler()
        self.fetch_and_save()
        
        # Giảm từ 5s → 2s để tăng dữ liệu
        # Với 15 tuyến × 6 lần/phút = 90 records/phút
        # So với trước: 5 tuyến × 12 lần/phút = 60 records/phút
        scheduler.add_job(self.fetch_and_save, 'interval', seconds=2)
        scheduler.start()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info(" Đang dừng chương trình...")
            scheduler.shutdown()


if __name__ == '__main__':
    generator = TrafficDataGenerator()
    generator.start()
