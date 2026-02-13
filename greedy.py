from collections import defaultdict
from clickhouse_driver import Client
from collections import deque

def move_closest(start, needed_amount, product, inventory, graph):
    queue = deque([start])
    visited = set()
    while queue:
        node = queue.popleft()
        if node in visited:
            continue
        visited.add(node)
        for neighbor in graph[node]:
            if neighbor == start:
                continue
            queue.append(neighbor)
            if inventory[neighbor].get(product, 0) > 0:
                amount = min(inventory[neighbor][product], needed_amount)
                inventory[neighbor][product] -= amount
                inventory[node][product] += amount
                return
    raise ValueError("No needed product found! Bad initialization!")

def greedy():
    client = Client(host='localhost', port=9000)
    warehouses = client.execute("SELECT id, product, quantity FROM warehouses ORDER BY id, product")
    requests = client.execute("SELECT id, warehouse_id, product, quantity, penalty_base FROM requests ORDER BY id")
    links = client.execute("SELECT from_warehouse, to_warehouse, distance FROM warehouses_links")

    graph = defaultdict(list)
    for frm, to, dst in links:
        if dst == 1:
            graph[frm].append(to)

    inventory = defaultdict(lambda: defaultdict(int))
    for wid, prod, qty in warehouses:
        inventory[wid][prod] += qty

    penalty_requests = {}
    penalty_accumulator = 0

    requests_iter = iter(requests)
    current_request = next(requests_iter, None)
    requests_num = len(requests)
    steps_cnt = 0
    print("===== GREEDY ALGO START =====")

    while penalty_requests or current_request:
        steps_cnt += 1
        new_penalty_req = None
        is_move_done = False

        if current_request:
            req_id, warehouse_id, product, qty, penalty_base = current_request
            if inventory[warehouse_id].get(product, 0) >= qty:
                inventory[warehouse_id][product] -= qty
            else:
                needed_amount = qty - inventory[warehouse_id].get(product, 0)
                move_closest(warehouse_id, needed_amount, product, inventory, graph)
                is_move_done = True
                if inventory[warehouse_id].get(product, 0) >= qty:
                    inventory[warehouse_id][product] -= qty
                else:
                    penalty_requests[req_id] = {
                        'warehouse_id': warehouse_id,
                        'product': product,
                        'quantity': qty,
                        'penalty': penalty_base,
                    }
                    new_penalty_req = req_id
            current_request = next(requests_iter, None)

        for p_id, p_data in list(penalty_requests.items()):
            if inventory[p_data['warehouse_id']].get(p_data['product'], 0) >= p_data['quantity']:
                inventory[p_data['warehouse_id']][p_data['product']] -= p_data['quantity']
                penalty_accumulator += p_data['penalty']
                del penalty_requests[p_id]
            else:
                if not is_move_done:
                    needed_amount = p_data["quantity"] - inventory[p_data["warehouse_id"]].get(p_data['product'], 0)
                    move_closest(p_data["warehouse_id"], needed_amount, p_data['product'], inventory, graph)
                    is_move_done = True
                    if inventory[p_data['warehouse_id']].get(p_data['product'], 0) >= p_data['quantity']:
                        inventory[p_data['warehouse_id']][p_data['product']] -= p_data['quantity']
                        penalty_accumulator += p_data['penalty']
                        del penalty_requests[p_id]
                        continue
                if p_id != new_penalty_req:
                    penalty_requests[p_id]['penalty'] += 1

        if steps_cnt % 100 == 0:
            print(f"Step: {steps_cnt}")
            requests_left = requests_num - steps_cnt
            if requests_left < 0:
                requests_left = 0
            print(f"main requests left: {requests_left}")
            print(f"Request count in penalty: {len(penalty_requests)}")
            print(f"Штраф: {penalty_accumulator}")

    print("\nВсе товары доставлены!\n")
    print(f"Шагов: {steps_cnt}")
    print(f"Штраф: {penalty_accumulator}")
    for w_id, prod in inventory.items():
        for p_id, qty in prod.items():
            if qty > 0:
                print(f"Осталось {p_id}: {qty} на складе {w_id}")

if __name__ == "__main__":
    greedy()
