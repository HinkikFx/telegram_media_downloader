import peewee
from peewee import *
from datetime import datetime
from loguru import logger
from utils import format
from playhouse.shortcuts import model_to_dict, dict_to_model
import json

db = SqliteDatabase('./downloaded.db')

class UnknownField(object):
    def __init__(self, *_, **__): pass

class BaseModel(Model):
    class Meta:
        database = db


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
    status = IntegerField(column_name='STATUS', null=True)

    class Meta:
        table_name = 'Downloaded'

    def create_table(table):
        u"""
        如果table不存在，新建table
        """
        if not table.table_exists():
            table.create_table()

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
        return 0 #0为不存在

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


class RetryIds(BaseModel):
    id = AutoField(primary_key=True, null=True)
    chat_username = CharField(max_length=200, null=False)
    message_ids = TextField(null=False)

    class Meta:
        table_name = 'RetryIds'

    def create_table(table):
        u"""
        如果table不存在，新建table
        """
        if not table.table_exists():
            table.create_table()


    def retry_msg_insert_to_db(self, retry_chat_id :str, retry_msg_ids: []):
        if db.autoconnect == False:
            db.connect()
        try:
            RetryIds.delete().where(RetryIds.chat_username == retry_chat_id).execute()
        except peewee.DoesNotExist:
            pass
        try:
            # 出错说明不存在此条数据，需写入
            retry_ids = RetryIds()
            retry_ids.chat_username = retry_chat_id
            retry_ids.message_ids = str(list(set(retry_msg_ids)))
            retry_ids.save()
            db.close()
            return True
        except Exception as e:
            logger.error(
                f"[{e}].",
                exc_info=True,
            )
            db.close()
            return False

    def load_retry_msg_from_db(self):
        if db.autoconnect == False:
            db.connect()
        try:
            # 出错说明不存在此条数据，需写入
            retry_ids = RetryIds().select()
            dicts = []
            for retry in retry_ids:
                retryIds_str = retry.message_ids.strip('[').strip(']').split(',')
                retryIds = []
                for retryId in retryIds_str:
                    retryIds.append(int(retryId))
                dictit = {
                    'chat_id': retry.chat_username,
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

class SqliteSequence(BaseModel):
    name = BareField(null=True)
    seq = BareField(null=True)

    class Meta:
        table_name = 'sqlite_sequence'
        primary_key = False
