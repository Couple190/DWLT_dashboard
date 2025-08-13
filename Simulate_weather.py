import time
import requests
import random
from datetime import datetime, timezone

# ==============
# CONSTANTS
# ==============
ORION_URL = "http://localhost:1026/v2/entities"
SUBSCRIPTION_URL = "http://localhost:1026/v2/subscriptions"
QL_URL = "http://quantumleap:8668/v2/notify"
API_KEY = "9eac022db3a9ccaaa0201cfbbaf47741"
UPDATE_INTERVAL = 300  # 5 minutes between updates

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# ==============
# SENSOR LOCATIONS 
# ==============
LOCATIONS = [
     {
        "id": "Sensor:Weather:FacultyCS",
        "name": "Faculty of Computer Science",
        "lat": 52.13878,
        "lon": 11.64533
    },
    {
        "id": "Sensor:Weather:ScienceHub",
        "name": "Science Harbor",
        "lat": 52.14175,
        "lon": 11.65640
    },
    {
        "id": "Sensor:Weather:UniMensa",
        "name": "University Mensa",
        "lat": 52.13966,
        "lon": 11.64761
    },
    {
        "id": "Sensor:Weather:Library",
        "name": "University Library",
        "lat": 52.13888,
        "lon": 11.64707
    },
    {
        "id": "Sensor:Weather:WelcomeCenter",
        "name": "OVGU Welcome Center",
        "lat": 52.14031,
        "lon": 11.64039
    },
    {
        "id": "Sensor:Weather:NorthPark",
        "name": "North Park",
        "lat": 52.14276,
        "lon": 11.64513
    },
    {
        "id": "Sensor:Weather:GeschwisterPark",
        "name": "Geschwister-Scholl-Park",
        "lat": 52.14020,
        "lon": 11.63655
    }
]

# ============== 
# PARKING SPOTS 
# ==============
PARKING_SPOTS = [
    {
        "id": "ParkingSpot:ScienceHarbor",
        "name": "Parking A - Science Harbor",
        "lat": 52.1412,
        "lon": 11.6558
    },
    {
        "id": "ParkingSpot:FacultyCS",
        "name": "Parking B - Faculty CS",
        "lat": 52.13878,
        "lon": 11.64533
    },
    {
        "id": "ParkingSpot:NorthPark",
        "name": "Parking C - North Park",
        "lat": 52.1431,
        "lon": 11.6457
    }
]

#  --------------------------
# FUNCTIONS
#  --------------------------

def update_parking(spot):
    """Simulate and update parking availability with free/total spaces"""
    total_spaces = random.randint(00, 15)  # total capacity
    free_spaces = random.randint(0, total_spaces)  # available spaces

    payload = {
        "totalSpaces": {
            "type": "Integer",
            "value": total_spaces
        },
        "freeSpaces": {
            "type": "Integer",
            "value": free_spaces
        },
        "location": {
            "type": "geo:point",
            "value": f"{spot['lat']}, {spot['lon']}"
        },
        "timestamp": {
            "type": "DateTime",
            "value": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        }
    }

    try:
        update_url = f"{ORION_URL}/{spot['id']}/attrs"
        update_res = requests.patch(update_url, headers=HEADERS, json=payload, timeout=10)

        if update_res.status_code == 404:
            # Create if doesn't exist
            payload["id"] = spot["id"]
            payload["type"] = "ParkingSpot"
            create_res = requests.post(ORION_URL, headers=HEADERS, json=payload, timeout=10)
            create_res.raise_for_status()
            print(f"[CREATED] {spot['id']} with {free_spaces}/{total_spaces} spaces free")
        else:
            update_res.raise_for_status()
            print(f"[UPDATED] {spot['id']} - {free_spaces}/{total_spaces} free")

        return True
    except Exception as e:
        print(f"[ERROR] Parking update failed: {str(e)}")
        return False

        return False



def get_weather(lat, lon):
    """Fetch weather data with proper timestamp"""
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        data = res.json()
        return {
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        }
    except Exception as e:
        print(f"[ERROR] Weather API failed for {lat},{lon}: {e}")
        return None

def update_orion(sensor):
    """Update or create entity in Orion with proper payload structure"""
    weather = get_weather(sensor["lat"], sensor["lon"])
    if not weather:
        return False

    # Basic payload structure
    payload = {
        "temperature": {
            "value": weather["temperature"],
            "type": "Float"
        },
        "humidity": {
            "value": weather["humidity"],
            "type": "Float"
        },
        "TimeInstant": {
            "value": weather["timestamp"],
            "type": "DateTime"
        },
        "location": {
            "value": f"{sensor['lat']}, {sensor['lon']}",
            "type": "geo:point"
        }
    }

    try:
        # First try to update existing entity
        update_url = f"{ORION_URL}/{sensor['id']}/attrs"
        update_res = requests.patch(update_url, headers=HEADERS, json=payload, timeout=10)
        
        if update_res.status_code == 404:
            # Entity doesn't exist, create it
            payload["id"] = sensor["id"]
            payload["type"] = "WeatherObserved"
            create_res = requests.post(ORION_URL, headers=HEADERS, json=payload, timeout=10)
            create_res.raise_for_status()
            print(f"[CREATED] {sensor['id']}")
        else:
            update_res.raise_for_status()
            print(f"[UPDATED] {sensor['id']}")

        print(f"  - Temp: {weather['temperature']}°C, Humidity: {weather['humidity']}% at {weather['timestamp']}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to update {sensor['id']}: {str(e)}")
        print(f"  - Payload sent: {payload}")
        if 'update_res' in locals():
            print(f"  - Orion response: {update_res.text}")
        return False

def create_subscription():
    """Create subscription with proper notification format"""
    sub_payload = {
        "description": "Weather data subscription",
        "subject": {
            "entities": [{"idPattern": ".*", "type": "WeatherObserved"}],
            "condition": {"attrs": ["temperature", "humidity"]}
        },
        "notification": {
            "http": {"url": QL_URL},
            "attrs": ["temperature", "humidity", "location", "TimeInstant"],
            "metadata": ["dateCreated", "dateModified"]
        },
        "expires": "2040-01-01T00:00:00.000Z"
    }

    try:
        res = requests.post(SUBSCRIPTION_URL, headers=HEADERS, json=sub_payload, timeout=10)
        res.raise_for_status()
        print("[SUCCESS] Subscription created")
        return True
    except Exception as e:
        print(f"[ERROR] Subscription failed: {str(e)}")
        return False

if __name__ == "__main__":
    print("Starting weather monitoring service...")
    
    # Create subscription once
    create_subscription()

    # Main update loop
    while True:
        start_time = time.time()
        print(f"\n=== Update cycle started at {datetime.now(timezone.utc).isoformat()} ===")
        
        success_count = 0
        # Update weather sensors
        for sensor in LOCATIONS:
            if update_orion(sensor):
                success_count += 1
            time.sleep(0.5)

        # Update parking sensors
        for spot in PARKING_SPOTS:
            update_parking(spot)
            time.sleep(0.5)

        elapsed = time.time() - start_time
        sleep_time = max(UPDATE_INTERVAL - elapsed, 0)
        print(f"=== Cycle complete: {success_count}/{len(LOCATIONS)} successful | Next update in {sleep_time:.1f}s ===")
        time.sleep(sleep_time)
        
        