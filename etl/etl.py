from datetime import timedelta


def transform_data(src_conn, dst_conn):
    def _get_min_date():
        with src_conn.cursor() as c:
            src_query = """
                    SELECT dt
                    FROM transactions
                    ORDER BY dt asc 
                    LIMIT 1
                """

            c.execute(src_query)
            result = c.fetchone()
            return result[0]

    current_date = _get_min_date()

    with src_conn:
        while True:
            with src_conn.cursor() as c:
                src_query = f"""
                        SELECT t.id, t.dt, t.idoper, t.move, t.amount, ot.name 
                        FROM transactions t
                        JOIN operation_types ot ON t.idoper = ot.id
                        WHERE t.dt >= "{current_date}" and t.dt < "{current_date + timedelta(hours=1)}"
                    """

                c.execute(src_query)
                data = c.fetchall()
                if not data:
                    break
                with dst_conn.cursor() as c2:
                    c2.executemany(
                        'INSERT INTO transactions_denormalized (id, dt, idoper, move, amount, name_oper) VALUES (%s, %s, %s, %s, %s, %s)', data)
                #print(f'Загружено {len(data)} транзакций с {current_date} по {current_date + timedelta(hours=1)}')
                current_date += timedelta(hours=1)
    dst_conn.close()
