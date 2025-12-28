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

        # Danh sách các đường
        self.roads = [
            {'id': 'HN_1', 'name': 'Đinh Tiên Hoàng', 'lat': 21.0293, 'lon': 105.8527},
            {'id': 'HN_2', 'name': 'Phố Huế', 'lat': 21.0185, 'lon': 105.8505},
            {'id': 'HN_3', 'name': 'Tây Sơn', 'lat': 21.0081, 'lon': 105.8236},
            {'id': 'HN_4', 'name': 'Đường Láng', 'lat': 21.0135, 'lon': 105.8105},
            {'id': 'HN_5', 'name': 'Bà Triệu', 'lat': 21.0205, 'lon': 105.8490},
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
        """Lấy dữ liệu thật và thêm chút biến động để Demo sinh động hơn"""
        try:
            logger.info("📡 Đang cập nhật dữ liệu từ API...")
            
            for road in self.roads:
                # 1. Lấy dữ liệu GỐC từ API TomTom
                real_speed, real_congestion = self.get_real_traffic(road['lat'], road['lon'])
                
                # --- PHẦN THÊM VÀO: TẠO BIẾN ĐỘNG GIẢ LẬP ---
                # Mục đích: Để số liệu nhảy múa liên tục mỗi giây, không bị đứng im
                
                # Tạo biến động ngẫu nhiên từ -3 đến +3 km/h
                noise_speed = random.randint(-3, 3) 
                
                # Tốc độ hiển thị = Tốc độ thật + Biến động
                display_speed = real_speed + noise_speed
                display_speed = max(1, display_speed) # Không để âm
                
                # Tính lại mức độ tắc nghẽn dựa trên tốc độ hiển thị mới
                # Giả sử tốc độ chuẩn là 40km/h
                new_congestion = 1 - (display_speed / 40)
                new_congestion = max(0.0, min(1.0, new_congestion)) # Giới hạn 0-1
                
                # Số xe cũng nhảy múa theo
                vehicle_noise = random.randint(-15, 15)
                vehicle_count = int(20 + (new_congestion * 180)) + vehicle_noise
                vehicle_count = max(5, vehicle_count)

                # ---------------------------------------------

                # 3. Đóng gói dữ liệu (Dùng số liệu đã biến động)
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
        logger.info(" Bắt đầu thu thập (Chu kỳ: 10s/lần)...")
        
        scheduler = BackgroundScheduler()
        self.fetch_and_save()
        
        scheduler.add_job(self.fetch_and_save, 'interval', seconds=5)
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