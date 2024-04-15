import os
from enum import Enum

import peewee
from peewee import *
from datetime import datetime
from loguru import logger
from utils import format
#from playhouse.shortcuts import model_to_dict, dict_to_model
#import json

#db = SqliteDatabase('../downloaded.db')
db = SqliteDatabase(os.path.join(os.path.abspath("."), "downloaded.db"))
class UnknownField(object):
    def __init__(self, *_, **__): pass

class BaseModel(Model):
    class Meta:
        database = db

class MsgStatusDB(Enum):
    """Download status"""

    Downloaded = 1
    Downloading = 2
    SkipDownload = 3
    MissingDownload = 4


class Downloaded(BaseModel):
    id = AutoField(primary_key=True, column_name='ID', null=True)
    chat_id = IntegerField(column_name='CHAT_ID', null=True)
    message_id = IntegerField(column_name='MESSAGE_ID', null=True)
    filename = TextField(column_name='FILENAME', null=True)
    caption = TextField(column_name='CAPTION', null=True)
    title = TextField(column_name='TITLE', null=True)
    mime_type = TextField(column_name='MIME_TYPE', null=True)
    media_size = IntegerField(column_name='MEDIA_SIZE', null=True)
    media_duration = IntegerField(column_name='MEDIA_DURATION', null=True)
    media_addtime = TextField(column_name='MEDIA_ADDTIME', null=True)
    chat_username = TextField(column_name='CHAT_USERNAME')
    chat_title = TextField(column_name='CHAT_TITLE', null=True)
    addtime = TextField(column_name='ADDTIME', null=True)
    status = IntegerField(column_name='STATUS', null=True) #

    class Meta:
        table_name = 'Downloaded'

    def create_table(table):
        u"""
        如果table不存在，新建table
        """
        if not table.table_exists():
            table.create_table()


    def getMsg(self, chat_username: str, message_id: int, status = 1, chat_id: int =None):
        if db.autoconnect == False:
            db.connect()
        if chat_username:
            try:
                downloaded = Downloaded.get(Downloaded.chat_username==chat_username, Downloaded.message_id==message_id, Downloaded.status == status)
                if downloaded:
                    return downloaded  # 说明存在此条数据
            except peewee.DoesNotExist:
                return None
        elif chat_id:
            try:
                downloaded = Downloaded.get(Downloaded.chat_id==chat_id, Downloaded.message_id==message_id, Downloaded.status == status)
                if downloaded:
                    return downloaded  # 说明存在此条数据
            except peewee.DoesNotExist:
                return None
        return None # 0为不存在

    def get2Down(self, chat_username: str):
        if db.autoconnect == False:
            db.connect()
        if chat_username:
            try:
                downloaded = Downloaded.select(Downloaded.message_id).where(Downloaded.chat_username == chat_username, Downloaded.status ==2)
                if downloaded:
                    return downloaded  # 说明存在此条数据
            except peewee.DoesNotExist:
                return None
        return None # 0为不存在

    def getStatus(self, chat_id: int, message_id: int, chat_username = None ):
        if db.autoconnect == False:
            db.connect()
        if chat_id:
            try:
                downloaded = Downloaded.get(chat_id=chat_id, message_id=message_id)
                if downloaded:
                    return downloaded.status# 说明存在此条数据
            except peewee.DoesNotExist:
                if chat_username:
                    try:
                        downloaded = Downloaded.get(chat_username=chat_username, message_id=message_id)
                        if downloaded:
                            return downloaded.status  # 说明存在此条数据
                    except peewee.DoesNotExist:
                        return 0
        return 0 #0为不存在 1未已完成 2为下载中 3为跳过未系在 4为下载后丢失

    def msg_insert_to_db(self, dictit :dict):
        if db.autoconnect == False:
            db.connect()
        try:
            downloaded = Downloaded.get(chat_id=dictit['chat_id'], message_id=dictit['message_id'])
            if downloaded:
                return False# 说明存在此条数据，无法插入
        except peewee.DoesNotExist:
            pass
        try:
            # 出错说明不存在此条数据，需写入
            downloaded = Downloaded()
            downloaded.chat_id = dictit['chat_id']
            downloaded.message_id = dictit['message_id']
            downloaded.filename = dictit['filename']
            downloaded.caption = dictit['caption']
            downloaded.title = dictit['title']
            downloaded.mime_type = dictit['mime_type']
            downloaded.media_size = dictit['media_size']
            downloaded.media_duration = dictit['media_duration']
            downloaded.media_addtime = dictit['media_addtime']
            if not dictit['chat_username']:
                dictit['chat_username'] = ''
            downloaded.chat_username = dictit['chat_username']
            downloaded.chat_title = dictit['chat_title']
            downloaded.addtime = datetime.now().strftime("%Y-%m-%d %H:%M")
            downloaded.status = dictit['status']
            downloaded.save()
            db.close()
            return True
        except Exception as e:
            logger.error(
                f"[{e}].",
                exc_info=True,
            )
            db.close()
            return False

    def msg_update_to_db(self, dictit :dict):
        if db.autoconnect == False:
            db.connect()
        try:
            downloaded = Downloaded.get(chat_id=dictit['chat_id'], message_id=dictit['message_id'])
            if downloaded:
                downloaded.chat_id = dictit['chat_id']
                downloaded.message_id = dictit['message_id']
                downloaded.filename = dictit['filename']
                downloaded.caption = dictit['caption']
                downloaded.title = dictit['title']
                downloaded.mime_type = dictit['mime_type']
                downloaded.media_size = dictit['media_size']
                downloaded.media_duration = dictit['media_duration']
                downloaded.media_addtime = dictit['media_addtime']
                if not dictit['chat_username']:
                    dictit['chat_username'] = ''
                downloaded.chat_username = dictit['chat_username']
                downloaded.chat_title = dictit['chat_title']
                downloaded.addtime = datetime.now().strftime("%Y-%m-%d %H:%M")
                downloaded.status = dictit['status']
                downloaded.save()
                db.close()
                return True
        except peewee.DoesNotExist:
            return False
        try:
            # 出错说明不存在此条数据，需写入
            downloaded = Downloaded()

            return True
        except Exception as e:
            logger.error(
                f"[{e}].",
                exc_info=True,
            )
            db.close()
            return False

    def get_all_message_id(self):
        if db.autoconnect == False:
            db.connect()
        try:
            downloaded = Downloaded.select().where(Downloaded.status == 1)
            db.close()
            return downloaded
        except peewee.DoesNotExist:
            db.close()
            return False


    def file_similar_rate(self, mime_type :str, media_size :int, filename :str, title: str):
        similar = 0
        if db.autoconnect == False:
            db.connect()
        try:
            downloaded = Downloaded.select().where(Downloaded.mime_type == mime_type, Downloaded.media_size == media_size, Downloaded.status==1)
            for record in downloaded:
                filename = filename.split(".")[0]
                filename_db = record.filename.split(".")[0]
                title_db = record.title
                for namea in [filename_db, title_db]:
                    for nameb in [filename, title]:
                        sim_num = format.string_similar(namea, nameb)
                        if sim_num == 1.0:
                            return 1.0
                        if similar < sim_num:
                            similar = sim_num
            db.close()
            return similar
        except peewee.DoesNotExist:
            db.close()
            return False
        except:
            db.close()
            return False

    def get_similar_files(self, chat_id: int, message_id: int, mime_type :str, media_size :int, filename :str, title: str, similar_min: float):
        similar_file_list = []
        if db.autoconnect == False:
            db.connect()
        try:
            status_acc= [1,2] #完成下载或正在下载
            downloaded = Downloaded.select().where(Downloaded.mime_type == mime_type , Downloaded.media_size == media_size, Downloaded.status.in_(status_acc) )
            for record in downloaded:
                if record.chat_id == chat_id and record.message_id == message_id: # 是自己
                    if record.status == 1:  # 如果是已完成状态 直接加入列表
                        similar_file_list.append(record)
                    else:
                        continue # 如果是未完成状态 则跳过
                else: # 不是是自己 判断是否相似
                    filename = filename.split(".")[0]
                    filename_db = record.filename.split(".")[0]
                    title_db = record.title
                    similar = 0
                    for namea in [filename_db, title_db]:
                        for nameb in [filename, title]:
                            sim_num = format.string_similar(namea, nameb)
                            if similar < sim_num:
                                similar = sim_num
                    if similar >= similar_min:  # 高于相似度阈值
                        similar_file_list.append(record)

            db.close()
            return similar_file_list
        except peewee.DoesNotExist:
            db.close()
            return None
        except Exception as e:
            logger.error(
                f"[{e}].",
                exc_info=True,
            )
            db.close()
            return None

    def get_last_read_message_id(self, chat_username: str):
        last_read_message_id = 1
        chat_username_qry = chat_username
        if db.autoconnect == False:
            db.connect()
        try:
            select_str = Downloaded.select(fn.Max(Downloaded.message_id)).where(
                Downloaded.chat_username == chat_username, Downloaded.status == 1)
            last_read_message_id = select_str.scalar()
            #print(f"==========={last_read_message_id}=============")
            db.close()
            if last_read_message_id:
                return last_read_message_id
            else:
                return 1
        except peewee.DoesNotExist:
            logger.error(f"{chat_username}error")
            db.close()
        except Exception as e:
            logger.error(
                f"[{chat_username}{e}].",
                exc_info=True,
            )

    def load_retry_msg_from_db(self):
        if db.autoconnect == False:
            db.connect()
        try:
            retry_ids = Downloaded.select(Downloaded.chat_id,Downloaded.chat_username,
                                          fn.GROUP_CONCAT(Downloaded.message_id).alias('retry_ids')).where(
                Downloaded.status == 2).group_by(
                Downloaded.chat_id, Downloaded.chat_username)

            dicts = []
            if retry_ids:
                for retry in retry_ids:
                    retryIds_str = str(retry.retry_ids).split(',')
                    retryIds = []
                    for retryId in retryIds_str:
                        retryIds.append(int(retryId))
                    dictit = {
                        'chat_id': retry.chat_id,
                        'chat_username' : retry.chat_username,
                        'ids_to_retry': set(retryIds)
                    }
                    dicts.append(dictit)
            db.close()
            return dicts
        except peewee.DoesNotExist:
            return None
        except Exception as e:
            logger.error(
                f"[{e}].",
                exc_info=True,
            )
            db.close()
            return None

    def retry_msg_insert_to_db(self, retry_chat_username :str, retry_msg_ids: []):
        if not retry_msg_ids:
            return False
        if db.autoconnect == False:
            db.connect()
        for msg_id in retry_msg_ids:
            try:
                downloaded = Downloaded.get(Downloaded.chat_username==retry_chat_username, Downloaded.message_id==int(msg_id))
                downloaded.status = 2
                downloaded.save()
                db.close()
                return True
            except peewee.DoesNotExist:
                continue
            except Exception as e:
                logger.error(
                    f"[{e}].",
                    exc_info=True,
                )
                db.close()
                return False


# class RetryIds(BaseModel):
#     id = AutoField(primary_key=True, null=True)
#     chat_username = CharField(max_length=200, null=False)
#     message_ids = TextField(null=False)
#
#     class Meta:
#         table_name = 'RetryIds'
#
#     def create_table(table):
#         u"""
#         如果table不存在，新建table
#         """
#         if not table.table_exists():
#             table.create_table()
#
#
#     def retry_msg_insert_to_db(self, retry_chat_id :str, retry_msg_ids: []):
#         if not retry_msg_ids:
#             return False
#         if db.autoconnect == False:
#             db.connect()
#         try:
#             RetryIds.delete().where(RetryIds.chat_username == retry_chat_id).execute()
#         except peewee.DoesNotExist:
#             pass
#         try:
#             # 出错说明不存在此条数据，需写入
#             retry_ids = RetryIds()
#             retry_ids.chat_username = retry_chat_id
#             retry_ids.message_ids = str(list(set(retry_msg_ids)))
#             retry_ids.save()
#             db.close()
#             return True
#         except Exception as e:
#             logger.error(
#                 f"[{e}].",
#                 exc_info=True,
#             )
#             db.close()
#             return False
#
#     def load_retry_msg_from_db(self):
#         if db.autoconnect == False:
#             db.connect()
#         try:
#             # 出错说明不存在此条数据，需写入
#             retry_ids = RetryIds().select()
#             dicts = []
#             for retry in retry_ids:
#                 retryIds_str = retry.message_ids.strip('[').strip(']').split(',')
#                 retryIds = []
#                 for retryId in retryIds_str:
#                     retryIds.append(int(retryId))
#                 dictit = {
#                     'chat_id': retry.chat_username,
#                     'ids_to_retry': set(retryIds)
#                 }
#                 dicts.append(dictit)
#             db.close()
#             return dicts
#         except peewee.DoesNotExist:
#             return None
#         except Exception as e:
#             logger.error(
#                 f"[{e}].",
#                 exc_info=True,
#             )
#             db.close()
#             return None

class SqliteSequence(BaseModel):
    name = BareField(null=True)
    seq = BareField(null=True)

    class Meta:
        table_name = 'sqlite_sequence'
        primary_key = False
