from flask import Blueprint, jsonify
from backend.services.mongo_service import mongo_service
import logging

logger = logging.getLogger(__name__)

api_bp = Blueprint('api', __name__, url_prefix='/api')

@api_bp.route('/current-predictions', methods=['GET'])
def get_predictions():
    """Lấy dự đoán hiện tại"""
    try:
        data = mongo_service.get_latest_predictions(minutes=5)
        return jsonify({
            "status": "success",
            "count": len(data),
            "data": data
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@api_bp.route('/road-status/<road_id>', methods=['GET'])
def get_road(road_id):
    """Lấy trạng thái đường cụ thể"""
    try:
        data = mongo_service.get_road_status(road_id)
        if data:
            return jsonify({"status": "success", "data": data})
        return jsonify({"status": "not_found"}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@api_bp.route('/statistics', methods=['GET'])
def get_stats():
    """Lấy thống kê"""
    try:
        stats = mongo_service.get_statistics()
        total_roads = len(mongo_service.get_all_roads())
        return jsonify({
            "status": "success",
            "total_roads": total_roads,
            "stats": stats
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@api_bp.route('/health', methods=['GET'])
def health():
    """Health check"""
    return jsonify({"status": "healthy"}), 200
