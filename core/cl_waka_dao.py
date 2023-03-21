import logging
from core.cl_mysql_connect import DbMysqlConnect
import base64

class WakaDao:
    def __init__(self, redis_cache, pool):
        # class DbMysqlConnect từ cl_mysql_connect.py
        # Gồm các hàm execute_query (lấy ds category trong redis nếu có sẵn cache,
        # k thì ms chạy lệnh sql và thêm kq vào redis cache)
        # Và hàm upsert (~ insert into)
        self.mysql_connect = DbMysqlConnect(redis_cache, pool)

    # chạy query lấy list category cho item dùng hàm execute_query trong class DbMysqlConnect
    def get_categories_by_content_id(self, content_id):
        try:
            sql = "select content_id, category_list from content_category_list where content_id = %s"
            res_cat = self.mysql_connect.execute_query(sql, content_id)
            if res_cat:
                return res_cat[0][1] # lay' category_list cho item
        except Exception as e:
            logging.exception(e)

    # chạy query lấy list category cho user dùng hàm execute_query trong class DbMysqlConnect
    def get_categories_by_user_id(self, user_id):
        try:
            sql = "select user_id, category_list from user_category_list where user_id = %s"
            res_cat = self.mysql_connect.execute_query(sql, user_id)
            if res_cat:
                return res_cat[0][1] # lấy category_list cho user
        except Exception as e:
            logging.exception(e)

    # chạy query write log vào bảng rec_access_log, dùng hàm upsert trong class DbMysqlConnect
    def write_log(self, item_id, user_id, request_body, request_headers, response_body, ip, time_process, model_id):
        try:
            sql = "insert into rec_access_log (item_id,user_id,request_body,request_headers,response_body,ip,time_process,endpoint,model) " \
                  "values(%s,%s,%s,%s,%s,%s,%s,'',%s)"
            self.mysql_connect.upsert(sql, (item_id, user_id, request_body, request_headers, response_body, ip, time_process, model_id))
        except Exception as e:
            logging.exception(e)


