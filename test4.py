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
        (1, 1, 100), # Склад 1: 100 ед. ТОВАРА 1
        (2, 2, 100), # Склад 2: 100 ед. ТОВАРА 2
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
        (1, 1, 2, 150, 1), # Запрос 1: склад 1 ← 150 ед. товара 2 (базовый штраф = 1)
        (2, 2, 1, 100, 2), # Запрос 2: склад 2 ← 100 ед. товара 1 (базовый штраф = 2)
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
        (2, 1, 1),
    ]
)