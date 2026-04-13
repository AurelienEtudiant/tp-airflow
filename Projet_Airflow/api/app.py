"""
API simulee pour les mini-projets Airflow - Formation IPSSI
Endpoints : /orders, /customers, /products, /metrics
Auth : Bearer token (formation-token-2026)
Donnees deterministes : meme date = meme resultat
"""

import hashlib
import os
from datetime import datetime, timedelta
from functools import wraps

from flask import Flask, jsonify, request

app = Flask(__name__)
API_TOKEN = os.environ.get("API_TOKEN", "formation-token-2026")


def require_auth(f):
    """Middleware d'authentification Bearer token."""
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return jsonify({"error": "Missing Authorization header"}), 401
        token = auth.replace("Bearer ", "")
        if token != API_TOKEN:
            return jsonify({"error": "Invalid token"}), 403
        return f(*args, **kwargs)
    return decorated


def _seed(date_str, salt=""):
    """Generateur deterministe base sur la date."""
    h = hashlib.md5(f"{date_str}{salt}".encode()).hexdigest()
    return int(h[:8], 16)


PRODUCTS = [
    "Widget A", "Widget B", "Gadget Pro", "Sensor X", "Module Y",
    "Board Z", "Cable Kit", "Power Supply", "Display Unit", "Connector Pack",
    "Relay Module", "Filter Unit", "Antenna Set", "Battery Pack", "Bracket Mount",
    "Switch Panel", "Fuse Box", "Motor Drive", "Cooling Fan", "LED Strip",
]
CITIES = [
    "Paris", "Lyon", "Marseille", "Bordeaux", "Lille",
    "Toulouse", "Nantes", "Strasbourg", "Nice", "Rennes",
]
CATEGORIES = ["electronics", "accessories", "power", "connectivity", "mechanical"]


@app.route("/health")
def health():
    return jsonify({"status": "ok", "version": "1.0.0"})


@app.route("/orders")
@require_auth
def get_orders():
    """GET /orders?date=YYYY-MM-DD — retourne 10-50 commandes."""
    date_str = request.args.get("date")
    if not date_str:
        return jsonify({"error": "Missing 'date' parameter (YYYY-MM-DD)"}), 400
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

    seed = _seed(date_str, "orders")
    count = 10 + (seed % 41)
    orders = []
    for i in range(count):
        s = _seed(date_str, f"order_{i}")
        qty = 1 + (s % 20)
        price = round(9.99 + (s % 100) * 1.5, 2)
        orders.append({
            "id": f"ORD-{date_str.replace('-', '')}-{i:04d}",
            "date": date_str,
            "customer_id": f"CUST-{(s % 200):04d}",
            "product": PRODUCTS[s % len(PRODUCTS)],
            "quantity": qty,
            "unit_price": price,
            "total": round(qty * price, 2),
            "status": ["completed", "pending", "shipped"][s % 3],
        })
    return jsonify(orders)


@app.route("/customers")
@require_auth
def get_customers():
    """GET /customers?limit=N — retourne N clients (max 500)."""
    limit = request.args.get("limit", 100, type=int)
    limit = max(1, min(limit, 500))
    customers = []
    for i in range(limit):
        s = _seed(str(i), "customer")
        customers.append({
            "id": f"CUST-{i:04d}",
            "name": f"Client {i}",
            "email": f"client{i}@example.com",
            "city": CITIES[s % len(CITIES)],
            "registered_date": (datetime(2024, 1, 1) + timedelta(days=s % 730)).strftime("%Y-%m-%d"),
            "active": s % 5 != 0,
        })
    return jsonify(customers)


@app.route("/products")
@require_auth
def get_products():
    """GET /products — retourne le catalogue (20 produits)."""
    products = []
    for i, name in enumerate(PRODUCTS):
        s = _seed(str(i), "product")
        products.append({
            "id": f"PROD-{i:04d}",
            "name": name,
            "category": CATEGORIES[s % len(CATEGORIES)],
            "price": round(9.99 + (s % 200) * 0.5, 2),
            "stock": 10 + (s % 500),
            "active": True,
        })
    return jsonify(products)


@app.route("/metrics")
@require_auth
def get_metrics():
    """GET /metrics?date=YYYY-MM-DD&metric_type=page_views — metriques horaires."""
    date_str = request.args.get("date")
    metric_type = request.args.get("metric_type", "page_views")
    if not date_str:
        return jsonify({"error": "Missing 'date' parameter"}), 400
    valid_types = ["page_views", "api_calls", "errors", "latency_ms"]
    if metric_type not in valid_types:
        return jsonify({"error": f"Invalid metric_type. Valid: {valid_types}"}), 400

    base_map = {"page_views": 5000, "api_calls": 10000, "errors": 10, "latency_ms": 100}
    var_map = {"page_views": 3000, "api_calls": 5000, "errors": 50, "latency_ms": 200}
    metrics = []
    for hour in range(24):
        s = _seed(date_str, f"metrics_{metric_type}_{hour}")
        metrics.append({
            "date": date_str,
            "hour": hour,
            "metric_type": metric_type,
            "value": base_map[metric_type] + (s % var_map[metric_type]),
        })
    return jsonify(metrics)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
