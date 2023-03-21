import json
import random
import logging
import time
from fastapi import (
    FastAPI,
    Body,
    BackgroundTasks
)
from starlette.requests import Request
from config import config
from core.redis_cache import RedisCache
from core.cl_waka_dao import WakaDao
import predictionio
import pymysqlpool

app = FastAPI()

# REDIS_SERVICE = 'vgdata', SENTINEL_CONFIGS =
# [("172.25.0.109", 50000), ("172.25.0.102", 50000), ("172.25.0.110", 50000)]
# redis_cache dc. tao. tu` class RediCache trong redis_cache.py,
# chua' cac' ham` thao tac' vs Redis db vd nhu get(key), append,... muc. dich' de? cache data truy van' cho nhanh

redis_cache = RedisCache(config.REDIS_SERVICE, config.SENTINEL_CONFIGS, db=5)

# pool chua' ket' noi' den' mysql dwh
pool = pymysqlpool.ConnectionPool(size=2, maxsize=3, pre_create_num=2, **config.MYSQL_SERVER)

# 2 Engine chua' 2 thuat. toan' UR va` itemSim
ur_client = predictionio.EngineClient(url="http://localhost:8012")
itemSim_client = predictionio.EngineClient(url="http://localhost:8011")

# Logging cho API
logger = logging.getLogger()
logger.setLevel("INFO")
logger.handlers[0].setFormatter(logging.Formatter(
    '%(asctime)s - %(levelname)s -%(filename)s:%(funcName)s:%(lineno)d\t%(message)s'
))


@app.post("/queries.json")
async def book_rec(request: Request, background_tasks: BackgroundTasks, v: dict = Body(...)):
    try:
        start_time = time.time()
        # Recommend cho item
        if "item" in v and "user" not in v:
            logging.info("---Start item_rec: %s", v) #{'num': 4, 'item': 27781} type dict

            request_body = json.dumps(v) #{'num': 4, 'item': 27781} type str

            # Số lượng book trong ds Recomemnd trả về, k đề cập gì thì lấy 10 cuốn
            num = int(v.get("num", 10))

            itemid = str(v.get("item", "")) # Lấy itemid và logging lại ttin
            logging.info("itemid: %s", itemid)

            model_id = "ur" # Universal Recommend

            # class WakaDao trong cl_waka_dao.py, 2 tham số là redis_cache và pool
            # Gồm các câu lệnh sql lấy category của item (get_categories_by_content_id)
            # cũng như write các log vào bảng rec_access_log trong mysql (write_log)
            waka_dao = WakaDao(redis_cache, pool)

            # Lấy ds category của item
            rowCategoriesItem = waka_dao.get_categories_by_content_id(itemid)
            logging.info("cats by content_id: %s", rowCategoriesItem)

            # Lúc lấy kết quả recommend sẽ lấy thật nhiều book để suffer random => bỏ
            # v['num'] = num * 3

            if not rowCategoriesItem: # Nếu k có ds category cho book, thì query gửi lên sẽ k có mục fields
                jres_ur = ur_client.send_query(v)
            else: # Nếu có ds category cho book thì gửi kèm mục fields để bias theo category của book đó
                listCategoryItem = rowCategoriesItem.split(",")
                v["fields"] = [{"name": "category", "values": listCategoryItem, "bias": 3}]
                jres_ur = ur_client.send_query(v)
            logging.info(jres_ur)

            # Nếu ur Engine trả về kq mặc định (score = 0), thì dùng Engine itemSim
            if jres_ur['itemScores'][0]['score'] == 0:
                itemSimQuery = {
                    "items": itemid,
                    "num": num
                }
                jres_itemSim = itemSim_client.send_query(itemSimQuery)
                if jres_itemSim and jres_itemSim['itemScores']:
                    model_id = "sim"
                    logging.info("Change to itemSim: %s", jres_itemSim)
                    jres_ur = jres_itemSim

        # Recommend cho user
        elif "user" in v and "item" not in v:

            logging.info("---Start user_rec: %s", v)  # {'num': 4, 'user': 599299} type dict

            request_body = json.dumps(v)  # {'num': 4, 'user': 599299} type str

            # Số lượng book trong ds Recomemnd trả về, k đề cập gì thì lấy 10 cuốn
            num = int(v.get("num", 10))

            userid = str(v.get("user", ""))  # Lấy userid và logging lại ttin
            logging.info("userid: %s", userid)

            model_id = "ur"  # Universal Recommend

            # class WakaDao trong cl_waka_dao.py, 2 tham số là redis_cache và pool
            # Gồm các câu lệnh sql lấy category của user (get_categories_by_user_id)
            # cũng như write các log vào bảng rec_access_log trong mysql (write_log)
            waka_dao = WakaDao(redis_cache, pool)

            # Lấy ds category của user
            rowCategoriesUser = waka_dao.get_categories_by_user_id(userid)
            logging.info("cats by user_id: %s", rowCategoriesUser)

            # Lúc lấy kết quả recommend sẽ lấy thật nhiều book để suffer random
            v['num'] = num * 3

            if not rowCategoriesUser:  # Nếu k có ds category cho user, thì query gửi lên sẽ k có mục fields
                jres_ur = ur_client.send_query(v)
            else:  # Nếu có ds category cho user thì gửi kèm mục fields để bias theo category mà user đó xem nhiều
                listCategoryUser = rowCategoriesUser.split(",")
                v["fields"] = [{"name": "category", "values": listCategoryUser, "bias": 3}]
                jres_ur = ur_client.send_query(v)
            logging.info('v: %s', v)
            logging.info(jres_ur)

        # Recommend cho user kết hợp với item (contextual recommend)
        else:
            logging.info("---Start user_item_rec: %s", v)  # {'num': 4, 'user': 599299, 'item': 2323} type dict

            request_body = json.dumps(v)  # {'num': 4, 'user': 599299, 'item': 2323} type str

            # Số lượng book trong ds Recomemnd trả về, k đề cập gì thì lấy 10 cuốn
            num = int(v.get("num", 10))

            userid = str(v.get("user", ""))  # Lấy userid và logging lại ttin
            itemid = str(v.get("item", ""))  # Lấy itemid và logging lại ttin
            logging.info("userid: %s; itemid: %s" % (userid, itemid))

            model_id = "ur"  # Universal Recommend

            # class WakaDao trong cl_waka_dao.py, 2 tham số là redis_cache và pool
            # Gồm các câu lệnh sql lấy category của user và item (get_categories_by_user_id/item_id)
            # cũng như write các log vào bảng rec_access_log trong mysql (write_log)
            waka_dao = WakaDao(redis_cache, pool)

            # Trong TH recommend mix user-item, lấy ds category của cả user và item
            # để bias tùy theo tình huống
            rowCategoriesUser = waka_dao.get_categories_by_user_id(userid)
            rowCategoriesItem = waka_dao.get_categories_by_content_id(itemid)
            logging.info("cats by user_id: %s", rowCategoriesUser)
            logging.info("cats by item_id: %s", rowCategoriesItem)

            # Lúc lấy kết quả recommend sẽ lấy thật nhiều book để suffer random
            v['num'] = num * 3

            # Nếu k có ds category cho user và có ds category cho item, thì lấy ds category cho item
            if not rowCategoriesUser and rowCategoriesItem:
                # Có thể thêm userBias để thiên về trải nghiệm cá nhân, itemBias để thiên về item
                # v['userBias'] = 1.5
                listCategoryItem = rowCategoriesItem.split(",")
                v["fields"] = [{"name": "category", "values": listCategoryItem, "bias": 3}]
                jres_ur = ur_client.send_query(v)
            elif rowCategoriesUser:  # Nếu có ds category cho user thì gửi kèm mục fields để bias theo category mà user đó xem nhiều
                listCategoryUser = rowCategoriesUser.split(",")
                v["fields"] = [{"name": "category", "values": listCategoryUser, "bias": 3}]
                jres_ur = ur_client.send_query(v)
            else: # Nếu k có cả 2 ds category cho item và user, thì gửi query bthg, k bias
                jres_ur = ur_client.send_query(v)

            logging.info('v: %s', v)
            logging.info(jres_ur)


        # Chỉ lấy những item có score != 0
        itemScoresOriginal = jres_ur['itemScores']
        itemScores = list(filter(lambda x: x['score'] != 0, itemScoresOriginal))


        # Lấy ngẫu nhiên n cuốn sách trong ds kq recommend (gồm n*3 cuốn sách) => Bỏ cái này đi
        # itemScores = random.choices(itemScoresFilter, k=num)
        # logging.info("itemScores random.choices: %s" % itemScores)

        # Tính thời gian xử lý 1 request
        time_process = (time.time() - start_time) * 1000
        # Them 1 background_tasks là "write_log" cho fastAPI, đi kèm với các tham số
        background_tasks.add_task(write_log, str(v.get("item", "")), str(v.get("user", "")), request_body, request.headers.get("user-agent", ""),
                                  json.dumps(itemScores), request.client.host, time_process, model_id)

        return {"itemScores": itemScores}
        # [{'item': '34181', 'score': 0.0}, {'item': '30688', 'score': 0.0}, {'item': '35000', 'score': 0.0}]
    except:
        return {"itemScores": []}

# Backgroud_tasks write_log, dùng hàm write_log trong waka_dao class để viết dl vào bảng rec_access_log trong mysql
async def write_log(item_id, user_id, request_body, request_headers, response_body, ip, time_process, model_id):
    waka_dao = WakaDao(redis_cache, pool)
    waka_dao.write_log(item_id, user_id, request_body, request_headers, response_body, ip, time_process, model_id)


