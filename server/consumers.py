import json

def create_delivery(state, event):
    data = json.loads(event.data)
    return {
        "id": event.delivery_id,
        "budget": int(data["budget"]),
        "notes": data["notes"],
        "state": "ready"
    }
    
def start_delivery(state, event):
    return state | {
        "status": "active"
    }
    
def pickup_products(state, event):
    data = json.loads(event.data)
    new_budget = int(state["budget"]) - int(data["purchase_price"]) * int(data["quantity"])
    
    return state | {
        "budget": new_budget,
        "purchase_price": int(data["purchase_price"]),
        "quantity": int(data["quantity"]),
        "status": "collected"
    }
    
def deliver_products(state, event):
    data = json.loads(event.data)
    new_budget = int(state["budget"]) + int(data["purchase_price"]) * int(data["quantity"])
    new_quantity = state["quantity"] - int(data["quantity"])

    return state | {
        "budget": new_budget,
        "purchase_price": int(data["purchase_price"]),
        "quantity": new_quantity,
        "status": "completed"
    }
    
#Used to call functions dynamically instead of static function calls
CONSUMERS = {
    "CREATE_DELIVERY": create_delivery,
    "START_DELIVERY": start_delivery,
    "PICKUP_PRODUCTS": pickup_products,
    "DELIVER_PRODUCTS": deliver_products
}