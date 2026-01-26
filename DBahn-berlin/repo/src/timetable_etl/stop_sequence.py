
# stop order is computed per train run as for the same line stops and route lengths can differ
# index = position of the concrete stop wihtin the train run (train_id), ordered by planned arrival

from .sql import load_sql

def compute_stop_sequence(conn) -> None:

    with conn.cursor() as cur:
        cur.execute(load_sql("schema/create_stop_sequence.sql"))
    conn.commit()

