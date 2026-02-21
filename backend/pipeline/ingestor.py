import requests
import json
from typing import List, Dict, Any


BASE_URL = "https://gamma-api.polymarket.com"


def fetch_all_active_events(limit: int = 100) -> List[Dict[str, Any]]:
    all_events = []
    offset = 0
    
    while True:
        url = f"{BASE_URL}/events"
        params = {
            "active": True,
            "closed": False,
            "limit": limit,
            "offset": offset
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        events = data if isinstance(data, list) else data.get("data", [])
        if not events:
            break
            
        all_events.extend(events)
        offset += limit
        
        if len(events) < limit:
            break
    
    return all_events


def fetch_all_markets(limit: int = 100) -> List[Dict[str, Any]]:
    all_markets = []
    offset = 0
    
    while True:
        url = f"{BASE_URL}/markets"
        params = {
            "active": True,
            "closed": False,
            "limit": limit,
            "offset": offset
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        markets = data if isinstance(data, list) else data.get("data", [])
        if not markets:
            break
            
        all_markets.extend(markets)
        offset += limit
        
        if len(markets) < limit:
            break
    
    return all_markets


def build_dataset() -> Dict[str, Any]:
    events = fetch_all_active_events()
    markets = fetch_all_markets()
    
    return {
        "events": events,
        "markets": markets,
        "total_events": len(events),
        "total_markets": len(markets)
    }


def save_dataset(dataset: Dict[str, Any], filepath: str = "polymarket_dataset.json"):
    with open(filepath, "w") as f:
        json.dump(dataset, f, indent=2)


if __name__ == "__main__":
    dataset = build_dataset()
    save_dataset(dataset)
    print(f"Dataset created: {dataset['total_events']} events, {dataset['total_markets']} markets")
