import random

from collections import defaultdict, deque

from clickhouse_driver import Client


client = Client(host='localhost', port=9000, user='default')

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

###
###  INIT
###

client.execute(
    """
    CREATE TABLE warehouses (
        id UInt8,
        product UInt8,
        quantity UInt32
    ) ENGINE = MergeTree()
    ORDER BY (id, product);
    """
)
client.execute(
    "INSERT INTO warehouses (id, product, quantity) VALUES",
    [
        (2, 1, 30), # Склад 2: 30 ед. товара 1
        (3, 1, 20), # Склад 3: 20 ед. товара 1
        (3, 2, 30), # Склад 3: 30 ед. товара 2
        (4, 1, 80), # Склад 4: 80 ед. товара 1
        (5, 2, 20), # Склад 5: 20 ед. товара 2
    ]
)

client.execute(
    """
    CREATE TABLE requests (
        id UInt32,
        warehouse_id UInt8,
        product UInt8,
        quantity UInt32,
        penalty_base UInt16
    ) ENGINE = MergeTree()
    ORDER BY id;
    """
)
client.execute(
    "INSERT INTO requests (id, warehouse_id, product, quantity, penalty_base) VALUES",
    [
        (1, 1, 1, 100, 1), # Запрос 1: склад 1 ← 100 ед. товара 1 (штраф = 1, ВЫСОКИЙ ПРИОРИТЕТ)
        (2, 5, 1, 30, 2), # Запрос 2: склад 5 ← 30 ед. товара 1 (штраф = 2)
        (3, 1, 2, 40, 3), # Запрос 3: склад 1 ← 40 ед. товара 2 (штраф = 3)
    ]
)

client.execute(
    """
    CREATE TABLE warehouses_links (
        from_warehouse UInt8,
        to_warehouse UInt8,
        distance UInt8
    ) ENGINE = MergeTree()
    ORDER BY (from_warehouse, to_warehouse);
    """
)
client.execute(
    "INSERT INTO warehouses_links (from_warehouse, to_warehouse, distance) VALUES",
    [
        (1, 2, 1),
        (1, 3, 2),
        (1, 4, 3),
        (1, 5, 4),
        (2, 1, 1),
        (2, 3, 1),
        (2, 4, 2),
        (2, 5, 3),
        (3, 1, 2),
        (3, 2, 1),
        (3, 4, 1),
        (3, 5, 2),
        (4, 1, 3),
        (4, 2, 2),
        (4, 3, 1),
        (4, 5, 1),
        (5, 1, 4),
        (5, 2, 3),
        (5, 3, 2),
        (5, 4, 1),
    ]
)