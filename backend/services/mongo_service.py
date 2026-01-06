from pymongo import MongoClient, DESCENDING
import logging

logger = logging.getLogger(__name__)


class MongoService:
    def __init__(self):
        try:
            # Kết nối MongoDB
            uri = 'mongodb://root:password@localhost:27017/'
            self.client = MongoClient(uri)
            self.db = self.client['traffic_db']
            # Read from 'predictions' (written by Spark with congestion_status)
            self.collection = self.db['predictions']
            logger.info(" MongoDB Connected via Service")
        except Exception as e:
            logger.error(f" MongoDB Connection Failed: {e}")
            raise

    def get_latest_predictions(self, minutes=5):
        """Lấy dữ liệu mới nhất (Duy nhất cho mỗi con đường)"""
        try:
            pipeline = [
                # 1. Sắp xếp mới nhất lên đầu
                {
                    "$sort": {"timestamp": -1}
                },
                # 2. Gom nhóm theo road_id, chỉ lấy cái đầu tiên (mới nhất)
                {
                    "$group": {
                        "_id": "$road_id",
                        "doc": {"$first": "$$ROOT"}
                    }
                },
                # 3. Trả về định dạng gốc
                {
                    "$replaceRoot": {"newRoot": "$doc"}
                }
            ]

            data = list(self.collection.aggregate(pipeline))
            
            # Xử lý dữ liệu để không lỗi JSON
            for item in data:
                item['_id'] = str(item['_id'])
                if 'timestamp' in item:
                    item['timestamp'] = str(item['timestamp'])
                
                # Logic gán trạng thái màu sắc
                if 'congestion_status' not in item:
                    lv = item.get('congestion_level', 0)
                    if lv > 0.7:
                        item['congestion_status'] = "NGUY HIỂM"
                    elif lv > 0.4:
                        item['congestion_status'] = "TẮC"
                    else:
                        item['congestion_status'] = "THÔNG THOÁNG"
            
            return data

        except Exception as e:
            logger.error(f"Error fetching predictions: {e}")
            return []

    def get_statistics(self):
        """Tính toán thống kê cơ bản"""
        try:
            # Lấy dữ liệu mới nhất để thống kê
            current_data = self.get_latest_predictions(minutes=10)
            
            total = len(current_data)
            danger = sum(
                1 for x in current_data
                if x.get('congestion_status') == 'NGUY HIỂM'
            )
            warning = sum(
                1 for x in current_data
                if x.get('congestion_status') == 'TẮC'
            )
            safe = sum(
                1 for x in current_data
                if x.get('congestion_status') == 'THÔNG THOÁNG'
            )
            
            return {
                "total": total,
                "danger": danger,
                "warning": warning,
                "safe": safe
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}

    def get_road_status(self, road_id):
        try:
            record = self.collection.find_one(
                {'road_id': road_id},
                sort=[('timestamp', DESCENDING)]
            )
            if record:
                record['_id'] = str(record['_id'])
                record['timestamp'] = record['timestamp'].isoformat()
            return record
        except Exception as e:
            logger.error(f"Error finding road {road_id}: {e}")
            return None

    def get_all_roads(self):
        return self.collection.distinct('road_id')


mongo_service = MongoService()
