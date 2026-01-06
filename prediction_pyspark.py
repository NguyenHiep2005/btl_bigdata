from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, desc, row_number, udf, hour, dayofweek,
    sin, cos, lag, avg
)
import math
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import os
import time

# Tạo thư mục logs
os.makedirs('logs', exist_ok=True)

# ===== DEFINE FUNCTION TRƯỚC CLASS =====
def determine_status(prediction):
    """Hàm UDF để xác định trạng thái từ số dự đoán
    (PHẢI độc lập, không phải method)"""
    if prediction > 0.7:
        return "NGUY HIỂM"
    elif prediction > 0.4:
        return "TẮC"
    else:
        return "THÔNG THOÁNG"


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

# Cấu hình MongoDB URI
MONGO_URI_INPUT = (
    "mongodb://root:password@localhost:27017/"
    "traffic_db.traffic_data?authSource=admin"
)
MONGO_URI_OUTPUT = (
    "mongodb://root:password@localhost:27017/"
    "traffic_db.predictions?authSource=admin"
)


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

            # Try to load previously saved pipeline model to avoid retraining from scratch
            self.model_path = 'models/traffic_pipeline'
            try:
                if os.path.exists(self.model_path):
                    logger.info(f"Loading existing model from {self.model_path}")
                    self.pipeline_model = PipelineModel.load(self.model_path)
                    logger.info("✓ Loaded saved pipeline model")
            except Exception:
                logger.info("No existing model loaded; will train a new one when enough data is available")
            
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
        """Tạo các đặc trưng (Feature Engineering - Cải thiện)"""
        # 1. Trích xuất giờ từ timestamp
        df = df.withColumn("hour", hour(col("timestamp")))

        # 2. Tạo cột is_peak (Giờ cao điểm: 7-9h, 17-19h)
        df = df.withColumn(
            "is_peak",
            when(col("hour").isin([7, 8, 9, 17, 18, 19]), 1.0)
            .otherwise(0.0)
        )

       
        df = df.withColumn("day_of_week", dayofweek(col("timestamp")))

        
        df = df.withColumn(
            "is_weekend",
            when(col("day_of_week").isin([1, 7]), 1.0).otherwise(0.0)
        )

        
        df = df.withColumn(
            "hour_sin",
            sin(lit(2 * math.pi / 24) * col("hour"))
        )
        df = df.withColumn(
            "hour_cos",
            cos(lit(2 * math.pi / 24) * col("hour"))
        )

       
        windowSpec = (
            Window.partitionBy("road_id")
            .orderBy(col("timestamp"))
        )
        df = df.withColumn(
            "speed_lag",
            lag("speed", 1).over(windowSpec)
        )
        df = df.withColumn(
            "speed_change",
            when(col("speed_lag").isNotNull(),
                 col("speed") - col("speed_lag"))
            .otherwise(0.0)
        )

        df = df.withColumn(
            "vehicle_count_lag",
            lag("vehicle_count", 1).over(windowSpec)
        )
        df = df.withColumn(
            "vehicle_count_change",
            when(col("vehicle_count_lag").isNotNull(),
                 col("vehicle_count") - col("vehicle_count_lag"))
            .otherwise(0.0)
        )

    
        windowSpecRoad = (
            Window.partitionBy("road_id")
            .orderBy(col("timestamp"))
            .rowsBetween(-5, 0)  # last 5 records per road
        )
        df = df.withColumn(
            "avg_speed_road",
            avg("speed").over(windowSpecRoad)
        )
        df = df.withColumn(
            "speed_normalized",
            when(col("avg_speed_road") > 0,
                 col("speed") / col("avg_speed_road"))
            .otherwise(1.0)
        )

        df = df.drop("speed_lag", "vehicle_count_lag", "avg_speed_road")

        return df

    def train_model(self):
        """Huấn luyện mô hình Random Forest với Spark MLlib"""
        logger.info(" Training Spark model...")
        try:
            df = self.get_data()
            # Require larger dataset for robust training (reduce overfitting)
            min_rows = 200
            total_rows = 0 if df is None else df.count()
            if df is None or total_rows < min_rows:
                logger.warning(f" Not enough data to train (have={total_rows}, need={min_rows}). Skipping training.")
                return

            df_processed = self.feature_engineering(df)

           
            feature_cols = [
                "speed",
                "vehicle_count",
                "is_peak",
                "is_weekend",
                "hour_sin",
                "hour_cos",
                "speed_change",
                "vehicle_count_change",
                "speed_normalized",
            ]

            assembler = VectorAssembler(
                inputCols=[c for c in feature_cols if c in df_processed.columns],
                outputCol="features"
            )

            # 4. Khởi tạo mô hình (tăng capacity để model biểu diễn tốt hơn)
            rf = RandomForestRegressor(
                featuresCol="features",
                labelCol="congestion_level",
                numTrees=200,
                maxDepth=15,
                seed=42
            )

            # 5. Tạo Pipeline
            pipeline = Pipeline(stages=[assembler, rf])

            # 6. Fit mô hình
            self.pipeline_model = pipeline.fit(df_processed)
            logger.info("✓ Model trained successfully!")

            # Save pipeline model for reuse
            try:
                os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
                self.pipeline_model.write().overwrite().save(self.model_path)
                logger.info(f"✓ Pipeline model saved to {self.model_path}")
            except Exception as e:
                logger.warning(f"Could not save pipeline model: {e}")

        except Exception as e:
            logger.error(f"Training error: {e}")

    def make_predictions(self):
        """Dự đoán dữ liệu mới nhất"""
        if self.pipeline_model is None:
            self.train_model()  # Train lần đầu nếu chưa có
            if self.pipeline_model is None:
                return

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
            predictions = self.pipeline_model.transform(
                latest_df_processed
            )

            # Đăng ký hàm UDF (function độc lập, không phải method)
            status_udf = udf(determine_status, StringType())

            # Chọn các cột cần lưu và xử lý kết quả
            # Lưu ý: `prediction` (output model) được ghi vào DB
            # frontend dùng `prediction` để hiển thị % tắc
            final_result = predictions.withColumn(
                "prediction",
                col("prediction").cast("double")
            ).withColumn(
                "congestion_status",
                status_udf(col("prediction"))
            ).select(
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
            # Lưu ý: chỉ lưu `prediction` không lưu congestion_level
            # (congestion_level từ traffic_data; prediction từ model)
            final_result.write \
                .format("mongo") \
                .mode("append") \
                .option("uri", MONGO_URI_OUTPUT) \
                .save()

            count = final_result.count()
            logger.info(f"✓ Predictions saved for {count} roads")

        except Exception as e:
            logger.error(f"Prediction error: {e}")

    def start(self):
        """Chạy định kỳ"""
        msg = ("Starting Spark Prediction Service "
               "(predictions: 5s, training: 60s)...")
        logger.info(msg)
        scheduler = BackgroundScheduler()
        # Run predictions frequently
        scheduler.add_job(self.make_predictions, 'interval', seconds=5)
        # Retrain periodically so model improves as more data arrives
        scheduler.add_job(self.train_model, 'interval', seconds=60)
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
