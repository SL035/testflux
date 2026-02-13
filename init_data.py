import random
from collections import defaultdict, deque
from clickhouse_driver import Client

# ИСПРАВЛЕНО: убран user='default' — в версии 23.8 не требуется
client = Client(host='localhost', port=9000)

# ==================================
# ======== CLEAR TABLES ============
# ==================================
client.execute('DROP TABLE IF EXISTS warehouses;')
client.execute('DROP TABLE IF EXISTS requests;')
client.execute('DROP TABLE IF EXISTS warehouses_links;')
client.execute('DROP TABLE IF EXISTS penalty;')
client.execute('DROP TABLE IF EXISTS temp_init_values;')
client.execute('DROP TABLE IF EXISTS temp_warehouses;')
client.execute('DROP TABLE IF EXISTS temp_products;')
client.execute('DROP TABLE IF EXISTS temp_product_totals;')

# ==================================
# ======= INIT WAREHOUSES ==========
# ==================================

# ИСПРАВЛЕНО: CREATE TEMPORARY TABLE (без OR REPLACE!)
client.execute("""
CREATE TEMPORARY TABLE temp_init_values AS
SELECT
    toUInt8(rand64() % 7 + 3) AS n_warehouses,  /* [3..9] складов */
    toUInt8(rand64() % 90 + 10) AS n_products    /* [10..99] продуктов */
;
""")

client.execute("""
CREATE TABLE warehouses (
    id UInt8,
    product UInt8,
    quantity UInt32
) ENGINE = MergeTree()
ORDER BY (id, product);
""")

# ИСПРАВЛЕНО: уменьшено количество строк с 10_000_000 до 1_000_000 для Codespaces
# ИСПРАВЛЕНО: заменён устаревший RAND() на rand64() (актуально для версии 23.8+)
client.execute("""
INSERT INTO warehouses
WITH
    (SELECT n_warehouses FROM temp_init_values) AS n_warehouses,
    (SELECT n_products FROM temp_init_values) AS n_products
SELECT
    toUInt8(floor(cbrt(rand64() % (n_warehouses * n_warehouses * n_warehouses))) + 1) AS id,
    toUInt8(floor(sqrt(rand64() % (n_products * n_products))) + 1) AS product,
    toUInt32(COUNT()) AS quantity
FROM numbers(1_000_000)  /* Уменьшено для скорости в Codespaces */
GROUP BY id, product;
""")

# ИСПРАВЛЕНО: все временные таблицы без OR REPLACE
client.execute("""
CREATE TEMPORARY TABLE temp_warehouses AS
SELECT DISTINCT id AS warehouse_id FROM warehouses;
""")

client.execute("""
CREATE TEMPORARY TABLE temp_products AS
SELECT DISTINCT product FROM warehouses;
""")

client.execute("""
CREATE TEMPORARY TABLE temp_product_totals AS
SELECT
    product,
    sum(quantity) AS total_quantity
FROM warehouses
GROUP BY product;
""")

# ИСПРАВЛЕНО: извлечение значений из результата (было списком кортежей)
warehouses_num = client.execute("SELECT COUNT() FROM temp_warehouses")[0][0]
products_num = client.execute("SELECT COUNT() FROM temp_products")[0][0]

counts_by_warehouses = client.execute("""
    SELECT id, sum(quantity) AS init_products_count
    FROM warehouses
    GROUP BY id
    ORDER BY id;
""")

counts_by_products = client.execute("SELECT * FROM temp_product_totals ORDER BY product;")

print("==== INFO ====")
print(f"<<< warehouses count: {warehouses_num}")
print(f"<<< products count: {products_num}")
print(f"<<< init items count in warehouses:")
for row in counts_by_warehouses:
    print(f"  {row}")
print(f"<<< init items count by product:")
for row in counts_by_products:
    print(f"  {row}")
print("=========================")

# ==================================
# ======== INIT REQUESTS ===========
# ==================================
client.execute("""
CREATE TABLE requests (
    id UInt32,
    warehouse_id UInt8,
    product UInt8,
    quantity UInt32,
    penalty_base UInt16
) ENGINE = MergeTree()
ORDER BY id;
""")

product_totals = client.execute("""
    SELECT product, total_quantity
    FROM temp_product_totals
""")

all_requests = []
for product, total_qty in product_totals:
    num_requests_for_product = random.randint(1, total_qty)
    remaining = total_qty
    for i in range(num_requests_for_product - 1):
        qty = random.randint(1, remaining)
        all_requests.append((product, qty))
        remaining -= qty
        if remaining <= 0:
            break
    if remaining > 0:
        all_requests.append((product, remaining))

random.shuffle(all_requests)
warehouse_ids = [row[0] for row in client.execute("SELECT DISTINCT id FROM warehouses")]
rows_to_insert = []
for i, (product, qty) in enumerate(all_requests):
    warehouse_id = random.choice(warehouse_ids)
    penalty_base = random.randint(1, 500)
    rows_to_insert.append((i, warehouse_id, product, qty, penalty_base))

client.execute(
    "INSERT INTO requests (id, warehouse_id, product, quantity, penalty_base) VALUES",
    rows_to_insert
)

requests_num = client.execute("SELECT COUNT() FROM requests")[0][0]
print(f"\nRequests count: {requests_num}")

# ==================================
# ==== INIT WAREHOUSES LINKS =======
# ==================================
client.execute("""
CREATE TABLE warehouses_links (
    from_warehouse UInt8,
    to_warehouse UInt8,
    distance UInt8
) ENGINE = MergeTree()
ORDER BY (from_warehouse, to_warehouse);
""")

edges = defaultdict(set)
random.shuffle(warehouse_ids)
for a, b in zip(warehouse_ids[:-1], warehouse_ids[1:]):
    edges[a].add(b)
    edges[b].add(a)

print(f"\nMain links:")
for node in sorted(edges):
    print(f"  {node} -> {sorted(edges[node])}")

min_extra_edges = len(warehouse_ids) // 2
max_extra_edges = (len(warehouse_ids) * (len(warehouse_ids) - 1) // 2) - (len(warehouse_ids) - 1)
extra_edges_count = random.randint(min_extra_edges, max_extra_edges)
print(f"extra_links count: {extra_edges_count}")

added_extra_edges = 0
while added_extra_edges < extra_edges_count:
    a, b = random.sample(warehouse_ids, 2)
    if b not in edges[a]:
        edges[a].add(b)
        edges[b].add(a)
        added_extra_edges += 1

print(f"\nDirect links:")
for node in sorted(edges):
    print(f"  {node} -> {sorted(edges[node])}")

links_to_insert = []
for start in warehouse_ids:
    queue = deque([(start, 0)])
    visited = {start: 0}
    while queue:
        node, dist = queue.popleft()
        for neighbor in edges[node]:
            if neighbor not in visited:
                visited[neighbor] = dist + 1
                queue.append((neighbor, dist + 1))
    for target, dist in visited.items():
        if start != target:
            links_to_insert.append((start, target, dist))

print("\nAll links (sample):")
for i, link in enumerate(links_to_insert[:20]):
    print(f"  {link}")
if len(links_to_insert) > 20:
    print(f"  ... and {len(links_to_insert) - 20} more")

client.execute(
    "INSERT INTO warehouses_links (from_warehouse, to_warehouse, distance) VALUES",
    links_to_insert
)

print("\n==== INIT FINISHED ====")