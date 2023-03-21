import json
import logging
import hashlib


class DbMysqlConnect:
    def __init__(self, redis_cache, pool):
        self.redis_cache = redis_cache
        self.pool = pool

    def execute_query(self, sql, args=None):
        logging.info("execute query: %s | %s", sql, args)
        #	execute query: select content_id, category_list from content_category_list where content_id = %s | 18355
        # sql: cau lệnh sql,  args: content_id

        # Lấy key trong redis xem tồn tại chưa
        # sql_key có dạng wakarec:05de292bbabd550461e417504d832f19,
        # trong đó phần sau của wakarec là 1 str mã hóa dc tạo ra từ câu lệnh sql và content_id
        # (select ... from ... where ...)
        sql_key = "wakarec:{}".format(hashlib.md5("{}|{}".format(sql, args).encode()).hexdigest())

        # Lấy value từ key đó trong redis
        sql_cache = self.redis_cache.get(sql_key)

        # Nếu không tồn tại cache trong redis, thì ms chạy lệnh sql
        if not sql_cache:
            conn = self.pool.get_connection()
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, args)
                    result = cursor.fetchall()

            # và cache vào redis (cache), key là sql_key, value là ds item-category "[[39491, \"342,343,407\"]]"
            # 3600*2 là thời gian hết hạn lưu trong redis, ~ 2 tiếng
            self.redis_cache.set(sql_key, json.dumps(result), 3600 * 2)
        # Nếu tồn tại cache (ds category cho item) rồi thì lấy luôn ds này
        else:
            result = json.loads(sql_cache)
        return result

    # Hàm upsert để chạy query sql dạng insert into
    def upsert(self, sql, args=None):
        conn = self.pool.get_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, args)
            conn.commit()