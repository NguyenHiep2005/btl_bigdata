from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, when, lit, desc, row_number, udf
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import os
import time

# Tạo thư mục logs
os.makedirs('logs', exist_ok=True)

# Cấu hình Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/prediction_spark.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Cấu hình MongoDB URI (Nhớ khớp với user/pass của bạn)
MONGO_URI_INPUT = "mongodb://root:password@localhost:27017/traffic_db.traffic_data?authSource=admin"
MONGO_URI_OUTPUT = "mongodb://root:password@localhost:27017/traffic_db.predictions?authSource=admin"

class SparkPredictionService:
    def __init__(self):
        logger.info(" Initializing PySpark Session...")
        try:
            # Khởi tạo Spark Session có kèm MongoDB Connector
            # Lưu ý: Phiên bản package phải khớp với phiên bản Spark bạn cài (ở đây ví dụ cho Spark 3.x)
            self.spark = SparkSession.builder \
                .appName("TrafficPrediction") \
                .config("spark.mongodb.input.uri", MONGO_URI_INPUT) \
                .config("spark.mongodb.output.uri", MONGO_URI_OUTPUT) \
                .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
                .getOrCreate()
            
            logger.info("✓ Spark Session Created!")
            self.pipeline_model = None
            
        except Exception as e:
            logger.error(f"✗ Spark Initialization Error: {e}")
            raise

    def get_data(self):
        """Đọc dữ liệu từ MongoDB vào Spark DataFrame"""
        try:
            df = self.spark.read.format("mongo").load()
            # Chuyển đổi kiểu dữ liệu nếu cần
            df = df.withColumn("speed", col("speed").cast("double")) \
                   .withColumn("vehicle_count", col("vehicle_count").cast("double")) \
                   .withColumn("congestion_level", col("congestion_level").cast("double"))
            return df
        except Exception as e:
            logger.error(f"Error reading data: {e}")
            return None

    def feature_engineering(self, df):
        """Tạo các đặc trưng (Feature Engineering)"""
        # 1. Trích xuất giờ từ timestamp
        df = df.withColumn("hour", hour(col("timestamp")))
        
        # 2. Tạo cột is_peak (Giờ cao điểm: 7-9h, 17-19h)
        df = df.withColumn("is_peak", 
            when(col("hour").isin([7, 8, 9, 17, 18, 19]), 1.0).otherwise(0.0)
        )
        
        return df

    def train_model(self):
        """Huấn luyện mô hình Random Forest với Spark MLlib"""
        logger.info(" Training Spark model...")
        try:
            df = self.get_data()
            if df is None or df.count() < 10:
                logger.warning(" Not enough data to train.")
                return

            df_processed = self.feature_engineering(df)

            # 3. Gom các cột feature vào vector (Spark yêu cầu bước này)
            assembler = VectorAssembler(
                inputCols=["speed", "vehicle_count", "is_peak"],
                outputCol="features"
            )
            

            # 4. Khởi tạo mô hình
            rf = RandomForestRegressor(featuresCol="features", labelCol="congestion_level", numTrees=10, maxDepth=5)

            # 5. Tạo Pipeline
            pipeline = Pipeline(stages=[assembler, rf])

            # 6. Fit mô hình
            self.pipeline_model = pipeline.fit(df_processed)
            logger.info("✓ Model trained successfully!")

        except Exception as e:
            logger.error(f"Training error: {e}")

    def determine_status(self, prediction):
        """Hàm UDF để xác định trạng thái từ số dự đoán"""
        if prediction > 0.7:
            return "NGUY HIỂM"
        elif prediction > 0.4:
            return "TẮC"
        else:
            return "THÔNG THOÁNG"

    def make_predictions(self):
        """Dự đoán dữ liệu mới nhất"""
        if self.pipeline_model is None:
            self.train_model() # Train lần đầu nếu chưa có
            if self.pipeline_model is None: return

        try:
            logger.info(" Running predictions...")
            df = self.get_data()
            
            # --- LẤY DỮ LIỆU MỚI NHẤT CỦA TỪNG ĐƯỜNG ---
            windowSpec = Window.partitionBy("road_id").orderBy(col("timestamp").desc())
            
            # Đánh số thứ tự, dòng mới nhất là 1
            latest_df = df.withColumn("row_num", row_number().over(windowSpec)) \
                          .filter(col("row_num") == 1) \
                          .drop("row_num")

            # Xử lý Feature
            latest_df_processed = self.feature_engineering(latest_df)

            # Dự đoán
            predictions = self.pipeline_model.transform(latest_df_processed)

            # Đăng ký hàm UDF để chuyển số thành chữ trạng thái
            status_udf = udf(self.determine_status, StringType())

            # Chọn các cột cần lưu và xử lý kết quả
            final_result = predictions.withColumn("prediction", col("prediction").cast("double")) \
                                      .withColumn("congestion_status", status_udf(col("prediction"))) \
                                      .select(
                                          col("timestamp"),
                                          col("road_id"),
                                          col("road_name"),
                                          col("speed"),
                                          col("vehicle_count"),
                                          col("lat"),
                                          col("lon"),
                                          col("prediction"),
                                          col("congestion_status")
                                      )

            # Ghi xuống MongoDB (Collection predictions)
            final_result.write \
                .format("mongo") \
                .mode("append") \
                .option("uri", MONGO_URI_OUTPUT) \
                .save()

            logger.info(f"✓ Predictions saved for {final_result.count()} roads")

        except Exception as e:
            logger.error(f"Prediction error: {e}")

    def start(self):
        """Chạy định kỳ"""
        logger.info("Starting Spark Prediction Service (interval: 5s)...")
        # Spark khởi động nặng hơn sklearn nên để 30s hoặc 1 phút
        scheduler = BackgroundScheduler()
        scheduler.add_job(self.make_predictions, 'interval', seconds=5)
        scheduler.start()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping Spark Session...")
            self.spark.stop()
            scheduler.shutdown()

if __name__ == '__main__':
    service = SparkPredictionService()
    service.start()