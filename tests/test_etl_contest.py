import pymysql
from .helpers import ping_container
# import sys
# sys.path.insert(0, '../..')
from etl.etl import transform_data


def test_container_is_alive(mysql_source_image):
    assert ping_container(mysql_source_image)


def test_containers_assets_is_ready(mysql_source_image,
                                    mysql_destination_image):
    src_conn = pymysql.connect(**mysql_source_image,
                               cursorclass=pymysql.cursors.DictCursor)

    with src_conn:
        with src_conn.cursor() as c:
            src_query = """
                SELECT
                    COUNT(*) AS total
                FROM transactions t
                    JOIN operation_types ot ON t.idoper = ot.id
            """

            c.execute(src_query)
            src_result = c.fetchone()

    dst_conn = pymysql.connect(**mysql_destination_image,
                               cursorclass=pymysql.cursors.DictCursor)

    with dst_conn:
        with dst_conn.cursor() as c:
            dst_query = """
                SELECT
                    COUNT(*) AS total
                FROM transactions_denormalized t
            """

            c.execute(dst_query)
            dst_result = c.fetchone()

    assert src_result['total'] > 0
    assert dst_result['total'] == 0


def test_data_transfer(mysql_source_image,
                       mysql_destination_image):
    """

    :param mysql_source_image: Контейнер mysql-источника с исходными данными
    :param mysql_destination_image: Контейнер mysql-назначения
    :return:
    """
    src_conn = pymysql.connect(**mysql_source_image)
    dst_conn = pymysql.connect(**mysql_destination_image)

    transform_data(src_conn, dst_conn)

    src_conn = pymysql.connect(**mysql_source_image,
                               cursorclass=pymysql.cursors.DictCursor)
    dst_conn = pymysql.connect(**mysql_destination_image,
                               cursorclass=pymysql.cursors.DictCursor)

    with src_conn:
        with src_conn.cursor() as c:
            src_query = """
                    SELECT
                        t.id, t.dt, t.idoper, t.move, t.amount, ot.name name_oper
                    FROM transactions t
                        JOIN operation_types ot ON t.idoper = ot.id
                """

            c.execute(src_query)
            src_result = c.fetchall()

    with dst_conn:
        with dst_conn.cursor() as c:
            src_query = """
                    SELECT *
                    FROM transactions_denormalized
                 """

            c.execute(src_query)
            dst_result = c.fetchall()

    assert src_result == dst_result
