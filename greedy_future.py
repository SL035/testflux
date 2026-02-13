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


def move_if_can(start, max_path, needed_amount, product, inventory, graph):
    i = 1
    queue = deque([start])  
    visited = set()
    while i <= max_path and queue:
        node = queue.popleft()
        if node in visited:
            continue
        visited.add(node)

        for neighbor in graph[node]:
            if neighbor == start:
                continue
            queue.append(neighbor)
            if inventory[neighbor].get(product, 0) >= needed_amount:
                inventory[neighbor][product] -= needed_amount
                inventory[node][product] += needed_amount
                return True
        i += 1
    return False


def greedy_future():
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

    # === ОБРАБОТКА ОЧЕРЕДИ ЗАЯВОК ===
    requests_num = len(requests)
    steps_cnt = 0

    print("===== GREEDY FUTURE ALGO START =====")

    for i, current_request in enumerate(requests):
        steps_cnt += 1
        new_penalty_req = None
        is_move_done = False
        # === 1. Обрабатываем текущую заявку (если есть) ===
    
        req_id, warehouse_id, product, qty, penalty_base = current_request
        # Проверяем, можно ли выполнить заявку на месте
        if inventory[warehouse_id].get(product, 0) >= qty:
            inventory[warehouse_id][product] -= qty
        else:
            # Проверяем можно ли выполнить после 1 перемещения, и если да, то выполняем
            needed_amount = qty - inventory[warehouse_id].get(product, 0)
            is_moved = move_if_can(warehouse_id, 1, needed_amount, product, inventory, graph)
            if is_moved:
                is_move_done = True
                inventory[warehouse_id][product] -= qty
            else:
                # Иначе отправлем в штрафные
                penalty_requests[req_id] = {
                    'warehouse_id': warehouse_id,
                    'product': product,
                    'quantity': qty,
                    'penalty': penalty_base,
                }
                new_penalty_req = req_id
        
        if not is_move_done:
            # === 2. Смотрим в будущее ===
            for j, next_request in enumerate(requests[i+1:]):
                next_req_id, next_warehouse_id, next_product, next_qty, next_penalty_base = next_request

                # Если и так можем выполнить заявку, то смотрим следующую
                if inventory[next_warehouse_id].get(next_product, 0) >= next_qty:
                    continue

                # Проверяем можно ли выполнить после j+2 перемещений, и если да, то перемещаем на 1
                needed_amount = next_qty - inventory[next_warehouse_id].get(next_product, 0)
                is_moved = move_if_can(next_warehouse_id, j+2, needed_amount, next_product, inventory, graph)

                if is_moved:
                    is_move_done = True
                    break

        # === 3. Обрабатываем штрафные заявки жадно ===
        for p_id, p_data in list(penalty_requests.items()):
            # Проверяем, можно ли выполнить штрафную заявку на месте
            if inventory[p_data['warehouse_id']].get(p_data['product'], 0) >= p_data['quantity']:
                inventory[p_data['warehouse_id']][p_data['product']] -= p_data['quantity']
                penalty_accumulator += p_data['penalty']
                del penalty_requests[p_id]
            else:
                # Если еще не двигали: двигаем ближайший товар на 1 и заного пытаемся выполнить штрафную заявку
                if not is_move_done:
                    needed_amount = p_data["quantity"] - inventory[p_data["warehouse_id"]].get(p_data['product'], 0)
                    move_closest(p_data["warehouse_id"], needed_amount, p_data['product'], inventory, graph)
                    is_move_done = True
                    if inventory[p_data['warehouse_id']].get(p_data['product'], 0) >= p_data['quantity']:
                        inventory[p_data['warehouse_id']][p_data['product']] -= p_data['quantity']
                        penalty_accumulator += p_data['penalty']
                        del penalty_requests[p_id]
                        continue
                # Иначе увеличиваем штраф
                if p_id != new_penalty_req:
                    penalty_requests[p_id]['penalty'] += 1

        if steps_cnt % 100 == 0:
            print(f"Step: {steps_cnt}")
            requests_left = requests_num - steps_cnt
            print(f"Main requests left: {requests_left if requests_left > 0 else 0}")
            print(f"Request count in penalty: {len(penalty_requests)}")
            print(f"Штраф: {penalty_accumulator}\n")

    print("\n<<<< No main requests left. Go to penalties!>>>>")

    # === 4. Обрабатываем оставшиеся штрафные заявки жадно ===
    while penalty_requests:
        steps_cnt += 1
        is_move_done = False
        for p_id, p_data in list(penalty_requests.items()):
            # Проверяем, можно ли выполнить штрафную заявку на месте
            if inventory[p_data['warehouse_id']].get(p_data['product'], 0) >= p_data['quantity']:
                inventory[p_data['warehouse_id']][p_data['product']] -= p_data['quantity']
                penalty_accumulator += p_data['penalty']
                del penalty_requests[p_id]
            else:
                # Если еще не двигали: двигаем ближайший товар на 1 и заного пытаемся выполнить штрафную заявку
                if not is_move_done:
                    needed_amount = p_data["quantity"] - inventory[p_data["warehouse_id"]].get(p_data['product'], 0)
                    move_closest(p_data["warehouse_id"], needed_amount, p_data['product'], inventory, graph)
                    is_move_done = True
                    if inventory[p_data['warehouse_id']].get(p_data['product'], 0) >= p_data['quantity']:
                        inventory[p_data['warehouse_id']][p_data['product']] -= p_data['quantity']
                        penalty_accumulator += p_data['penalty']
                        del penalty_requests[p_id]
                        continue
                # Иначе увеличиваем штраф
                penalty_requests[p_id]['penalty'] += 1

        if steps_cnt % 100 == 0:
            print(f"Step: {steps_cnt}")
            print(f"Request count in penalty: {len(penalty_requests)}")
            print(f"Штраф: {penalty_accumulator}\n")

    print("\n\nВсе товары доставлены!\n")
    print(f"Шагов: {steps_cnt}")
    print(f"Штраф: {penalty_accumulator}")

    for w_id, prod in inventory.items():
        for p_id, qty in prod.items():
            if qty > 0:
                print(f"Осталось {p_id}: {qty} на складе {w_id}")


if __name__ == "__main__":
    greedy_future()