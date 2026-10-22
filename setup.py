"""
RescuEats: Real-Time Food Surplus Redistribution System
Course  : CSE494 - Intelligent NoSQL Databases (Academic Task 2)

Covers core requirements:
  1. Document design        -> 3 collections: donations, ngos, claims
  2. Validation rules       -> $jsonSchema validators + unique/regular indexes
  3. CRUD operations        -> insert_one/many, find, update_one/many, delete_one/many
  4. Aggregation pipelines  -> $group, $match, $sort, $project, $bucket, $lookup, $unwind
  5. PyMongo integration    -> full driver integration

Run:  pip install pymongo   then   python setup.py
"""

import os
import random
import sys
from datetime import datetime, timedelta, timezone

from bson import ObjectId
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import PyMongoError

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "rescueats"

random.seed(42)  # reproducible data for presentation


# ==============================================================================
# OUTPUT HELPERS
# ==============================================================================
def banner(title):
    print("\n" + "=" * 90)
    print(f"  {title}")
    print("=" * 90)


def table(headers, rows):
    """Print rows as an auto-sized terminal table."""
    if not rows:
        print("  (No matching documents found)")
        return
    rows = [[str(c) for c in r] for r in rows]
    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(headers)]
    line = "  ".join(h.ljust(w) for h, w in zip(headers, widths))
    print(line)
    print("-" * len(line))
    for r in rows:
        print("  ".join(c.ljust(w) for c, w in zip(r, widths)))
    print(f"  -> {len(rows)} row(s)")


def show_donations(docs):
    table(
        ["ID(6)", "Food Item", "Category", "Qty(Kg)", "City", "Status", "Exp(h)"],
        [[str(d["_id"])[-6:], d["food_item"][:24], d["category"], f"{d['quantity_kg']:.1f}",
          d["location"]["city"], d["status"], d["expiry_hours"]] for d in docs],
    )


# ==============================================================================
# UNIT 2: DOCUMENT DESIGN + VALIDATION ($jsonSchema validators)
# ==============================================================================
CATEGORIES = ["Prepared Food", "Bakery", "Produce", "Packaged Goods", "Dairy"]
STATUSES = ["AVAILABLE", "CLAIMED", "EXPIRED"]

DONATION_SCHEMA = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["food_item", "category", "quantity_kg", "donor_name",
                     "status", "expiry_hours", "location", "created_at"],
        "properties": {
            "food_item":   {"bsonType": "string", "minLength": 2, "maxLength": 80},
            "category":    {"enum": CATEGORIES},
            "quantity_kg": {"bsonType": "number", "minimum": 0.1, "maximum": 5000},
            "donor_name":  {"bsonType": "string", "minLength": 2},
            "status":      {"enum": STATUSES},
            "expiry_hours": {"bsonType": "int", "minimum": 1, "maximum": 168},
            "created_at":  {"bsonType": "date"},
            "location": {
                "bsonType": "object",
                "required": ["city", "coordinates"],
                "properties": {
                    "city": {"bsonType": "string"},
                    "coordinates": {
                        "bsonType": "array", "minItems": 2, "maxItems": 2,
                        "items": {"bsonType": "number"},
                    },
                },
            },
        },
    }
}

NGO_SCHEMA = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["name", "city", "registration_no", "capacity_kg", "contact", "total_kg_received"],
        "properties": {
            "name":            {"bsonType": "string", "minLength": 2},
            "city":            {"bsonType": "string"},
            "registration_no": {"bsonType": "string", "pattern": "^NGO-[0-9]{4}$"},
            "capacity_kg":     {"bsonType": "number", "minimum": 1},
            "total_kg_received": {"bsonType": "number", "minimum": 0},
            "contact": {
                "bsonType": "object",
                "required": ["phone"],
                "properties": {
                    "phone": {"bsonType": "string", "pattern": "^[0-9]{10}$"},
                    "email": {"bsonType": "string", "pattern": "^.+@.+\\..+$"},
                },
            },
        },
    }
}

CLAIM_SCHEMA = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["donation_id", "ngo_id", "quantity_kg", "claimed_at"],
        "properties": {
            "donation_id": {"bsonType": "objectId"},
            "ngo_id":      {"bsonType": "objectId"},
            "quantity_kg": {"bsonType": "number", "minimum": 0.1},
            "claimed_at":  {"bsonType": "date"},
        },
    }
}


def setup_database(db):
    """Drop old collections and recreate them WITH validators + indexes."""
    for name in ("donations", "ngos", "claims"):
        db.drop_collection(name)

    db.create_collection("donations", validator=DONATION_SCHEMA,
                         validationLevel="strict", validationAction="error")
    db.create_collection("ngos", validator=NGO_SCHEMA,
                         validationLevel="strict", validationAction="error")
    db.create_collection("claims", validator=CLAIM_SCHEMA,
                         validationLevel="strict", validationAction="error")

    db.donations.create_index([("status", ASCENDING), ("location.city", ASCENDING)],
                              name="idx_status_city")
    db.donations.create_index([("quantity_kg", DESCENDING)], name="idx_qty_desc")
    db.ngos.create_index("registration_no", unique=True, name="uniq_registration")
    db.claims.create_index("ngo_id", name="idx_claim_ngo")
    print("  [+] Created collections with $jsonSchema validators and indexes.")


# ==============================================================================
# UNIT 3: CREATE
# ==============================================================================
CITY_COORDS = {
    "Delhi": [77.2090, 28.6139], "Mumbai": [72.8777, 19.0760],
    "Bengaluru": [77.5946, 12.9716], "Chandigarh": [76.7794, 30.7333],
    "Jalandhar": [75.5762, 31.3260],
}
DONORS = ["Royal Hotel", "Spice Route", "Fresh Bakers", "Green Farms", "City Supermarket",
          "Corner Dhaba", "Grand Banquet", "Sweet Corner", "Daily Dairy", "Metro Mart"]
FOODS = {
    "Prepared Food": ["Paneer Butter Masala", "Veg Biryani", "Dal Tadka & Rice", "Chole Bhature"],
    "Bakery": ["Whole Wheat Bread", "Pav Buns", "Veg Puffs", "Muffins"],
    "Produce": ["Fresh Apples", "Tomatoes", "Bananas", "Spinach"],
    "Packaged Goods": ["Biscuit Packs", "Instant Noodles", "Rice Bags", "Cooking Oil Pouches"],
    "Dairy": ["Paneer Blocks", "Curd Cups", "Milk Packets", "Butter Packs"],
}
NGOS = [
    ("Hope Foundation", "Delhi", 500), ("Roti Bank", "Delhi", 800),
    ("Annapurna Trust", "Mumbai", 600), ("Seva Kitchen", "Bengaluru", 400),
    ("Langar Aid", "Chandigarh", 700), ("Care Meals", "Jalandhar", 300),
]


def create_initial_data(db):
    """insert_many for NGOs and donations."""
    now = datetime.now(timezone.utc)

    ngos = [{
        "name": n, "city": c, "registration_no": f"NGO-{1001 + i}",
        "capacity_kg": float(cap), "total_kg_received": 0.0,
        "contact": {"phone": f"98765{43210 + i}", "email": f"contact{i}@ngo.org"},
    } for i, (n, c, cap) in enumerate(NGOS)]
    ngo_res = db.ngos.insert_many(ngos)

    donations = []
    for _ in range(60):
        cat = random.choice(CATEGORIES)
        city = random.choice(list(CITY_COORDS))
        donations.append({
            "food_item": random.choice(FOODS[cat]),
            "category": cat,
            "quantity_kg": round(random.uniform(2, 60), 1),
            "donor_name": random.choice(DONORS),
            "status": "AVAILABLE",
            "expiry_hours": random.choice([1, 2, 4, 6, 8, 12, 24, 48]),
            "created_at": now - timedelta(hours=random.randint(0, 72)),
            "location": {"city": city, "coordinates": CITY_COORDS[city]},
        })
    don_res = db.donations.insert_many(donations)
    print(f"  [+] insert_many: {len(ngo_res.inserted_ids)} NGOs, {len(don_res.inserted_ids)} donations.")

    claimed = 0
    for did in random.sample(don_res.inserted_ids, 22):
        if claim_donation(db, did, random.choice(ngo_res.inserted_ids), quiet=True):
            claimed += 1
    print(f"  [+] Simulated {claimed} claims (donations -> CLAIMED, claims collection filled).")
    return ngo_res.inserted_ids


def insert_single_donation(db, item, category, qty, donor, city):
    """insert_one demo."""
    doc = {
        "food_item": item, "category": category, "quantity_kg": float(qty),
        "donor_name": donor, "status": "AVAILABLE", "expiry_hours": 5,
        "created_at": datetime.now(timezone.utc),
        "location": {"city": city, "coordinates": CITY_COORDS.get(city, [0.0, 0.0])},
    }
    res = db.donations.insert_one(doc)
    print(f"  [+] insert_one: '{item}' (ID ...{str(res.inserted_id)[-6:]})")
    return res.inserted_id


# ==============================================================================
# UNIT 3: READ
# ==============================================================================
def read_available_by_city(db, city):
    """Dot notation + sort."""
    return list(db.donations.find({"status": "AVAILABLE", "location.city": city})
                .sort("quantity_kg", -1).limit(8))


def read_urgent(db):
    """Logical $and/$or + $in."""
    q = {"$and": [{"status": "AVAILABLE"},
                  {"$or": [{"expiry_hours": {"$lte": 2}},
                           {"category": {"$in": ["Prepared Food", "Dairy"]}, "expiry_hours": {"$lte": 4}}]}]}
    return list(db.donations.find(q).sort("expiry_hours", 1).limit(8))


def read_by_regex(db, pattern):
    """Regex search with projection."""
    return list(db.donations.find({"food_item": {"$regex": pattern, "$options": "i"}},
                                  {"food_item": 1, "donor_name": 1, "quantity_kg": 1, "_id": 0}).limit(6))


# ==============================================================================
# UNIT 3: UPDATE & DELETE
# ==============================================================================
def claim_donation(db, donation_id, ngo_id, quiet=False):
    """Atomic status update and claim registration."""
    now = datetime.now(timezone.utc)
    don = db.donations.find_one_and_update(
        {"_id": ObjectId(donation_id), "status": "AVAILABLE"},
        {"$set": {"status": "CLAIMED", "claimed_at": now, "claimed_by": ngo_id}},
        return_document=ReturnDocument.AFTER,
    )
    if don is None:
        if not quiet:
            print(f"  [x] Listing ...{str(donation_id)[-6:]} not found or already claimed.")
        return False
    db.claims.insert_one({"donation_id": don["_id"], "ngo_id": ngo_id,
                          "quantity_kg": don["quantity_kg"], "claimed_at": now})
    db.ngos.update_one({"_id": ngo_id}, {"$inc": {"total_kg_received": don["quantity_kg"]}})
    if not quiet:
        print(f"  [~] Listing ...{str(donation_id)[-6:]} -> CLAIMED, claim recorded, NGO total updated.")
    return True


def expire_stale_donations(db):
    """update_many: set expired status for stale available listings."""
    res = db.donations.update_many({"status": "AVAILABLE", "expiry_hours": {"$lte": 1}},
                                   {"$set": {"status": "EXPIRED"}})
    print(f"  [~] update_many: matched {res.matched_count}, modified {res.modified_count} -> EXPIRED.")


def remove_expired(db):
    res = db.donations.delete_many({"status": "EXPIRED"})
    print(f"  [-] delete_many: removed {res.deleted_count} expired listing(s).")


def remove_single(db, donation_id):
    res = db.donations.delete_one({"_id": ObjectId(donation_id)})
    print(f"  [-] delete_one: removed {res.deleted_count} listing (ID ...{str(donation_id)[-6:]}).")


# ==============================================================================
# AGGREGATION PIPELINES
# ==============================================================================
def agg_status_summary(db):
    """1. $group: count and total kg per status."""
    pipe = [
        {"$group": {"_id": "$status", "listings": {"$sum": 1},
                    "total_kg": {"$sum": "$quantity_kg"}, "avg_kg": {"$avg": "$quantity_kg"}}},
        {"$sort": {"total_kg": -1}},
    ]
    table(["Status", "Listings", "Total Kg", "Avg Kg"],
          [[r["_id"], r["listings"], f"{r['total_kg']:.1f}", f"{r['avg_kg']:.1f}"]
           for r in db.donations.aggregate(pipe)])


def agg_city_report(db):
    """2. City-wise availability: $match + $group + $project."""
    pipe = [
        {"$match": {"status": "AVAILABLE"}},
        {"$group": {"_id": "$location.city", "listings": {"$sum": 1},
                    "total_kg": {"$sum": "$quantity_kg"}, "max_kg": {"$max": "$quantity_kg"},
                    "min_expiry": {"$min": "$expiry_hours"}}},
        {"$project": {"_id": 0, "city": "$_id", "listings": 1,
                      "total_kg": {"$round": ["$total_kg", 1]},
                      "max_kg": 1, "min_expiry": 1,
                      "approx_meals": {"$floor": {"$multiply": ["$total_kg", 3]}}}},
        {"$sort": {"total_kg": -1}},
    ]
    table(["City", "Listings", "Total Kg", "Largest", "Soonest Expiry(h)", "~Meals"],
          [[r["city"], r["listings"], r["total_kg"], r["max_kg"], r["min_expiry"], r["approx_meals"]]
           for r in db.donations.aggregate(pipe)])


def agg_quantity_buckets(db):
    """3. $bucket: histogram distribution of donation sizes."""
    pipe = [{"$bucket": {"groupBy": "$quantity_kg", "boundaries": [0, 10, 25, 50, 5001],
                         "default": "other",
                         "output": {"listings": {"$sum": 1}, "total_kg": {"$sum": "$quantity_kg"}}}}]
    labels = {0: "0-10 kg", 10: "10-25 kg", 25: "25-50 kg", 50: "50+ kg"}
    table(["Size Bucket", "Listings", "Total Kg"],
          [[labels.get(r["_id"], r["_id"]), r["listings"], f"{r['total_kg']:.1f}"]
           for r in db.donations.aggregate(pipe)])


def agg_ngo_performance(db):
    """4. $lookup x2 + $unwind + $group: relational aggregation join."""
    pipe = [
        {"$lookup": {"from": "ngos", "localField": "ngo_id", "foreignField": "_id", "as": "ngo"}},
        {"$unwind": "$ngo"},
        {"$lookup": {"from": "donations", "localField": "donation_id", "foreignField": "_id", "as": "don"}},
        {"$unwind": "$don"},
        {"$group": {"_id": "$ngo.name", "city": {"$first": "$ngo.city"},
                    "claims": {"$sum": 1}, "total_kg": {"$sum": "$quantity_kg"},
                    "categories": {"$addToSet": "$don.category"}}},
        {"$sort": {"total_kg": -1}},
    ]
    table(["NGO", "City", "Claims", "Total Kg", "Categories Received"],
          [[r["_id"], r["city"], r["claims"], f"{r['total_kg']:.1f}", ", ".join(sorted(r["categories"]))]
           for r in db.claims.aggregate(pipe)])


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        print(f"[+] Connected to MongoDB at {MONGO_URI}")
    except PyMongoError as exc:
        print(f"[ERROR] Could not connect to MongoDB:\n        {exc}")
        sys.exit(1)

    db = client[DB_NAME]

    banner("1. DATABASE DESIGN & VALIDATION RULES")
    setup_database(db)

    banner("2. CREATE OPERATIONS")
    ngo_ids = create_initial_data(db)
    new_id = insert_single_donation(db, "Dal Tadka & Rice", "Prepared Food", 18.0, "Corner Dhaba", "Delhi")
    print(f"  Collection counts -> donations: {db.donations.count_documents({})}, "
          f"ngos: {db.ngos.count_documents({})}, claims: {db.claims.count_documents({})}")

    banner("3. READ OPERATIONS")
    print("\n--> AVAILABLE in Delhi (dot notation 'location.city'):")
    show_donations(read_available_by_city(db, "Delhi"))
    print("\n--> Urgent listings ($and / $or / $in):")
    show_donations(read_urgent(db))
    print("\n--> Regex search 'paneer' with projection:")
    table(["Food Item", "Donor", "Qty(Kg)"],
          [[d["food_item"], d["donor_name"], d["quantity_kg"]] for d in read_by_regex(db, "paneer")])

    banner("4. UPDATE & DELETE OPERATIONS")
    claim_donation(db, new_id, ngo_ids[0])
    expire_stale_donations(db)
    remove_expired(db)
    remove_single(db, new_id)

    banner("5. AGGREGATION PIPELINES")
    print("\n[A1] Status summary ($group, $sum, $avg):")
    agg_status_summary(db)
    print("\n[A2] City-wise availability ($match, $group, $project):")
    agg_city_report(db)
    print("\n[A3] Donation size distribution ($bucket):")
    agg_quantity_buckets(db)
    print("\n[A4] NGO performance ($lookup x2, $unwind, $group):")
    agg_ngo_performance(db)

    print(f"\n--> Final counts -> donations: {db.donations.count_documents({})}, "
          f"claims: {db.claims.count_documents({})}")

    print("\n[OK] Demonstration complete!")
    client.close()


if __name__ == "__main__":
    main()