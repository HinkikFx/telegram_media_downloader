import peewee
from peewee import *
from datetime import datetime
from loguru import logger
from utils import format

db = SqliteDatabase('./downloaded.db')

class UnknownField(object):
    def __init__(self, *_, **__): pass

class BaseModel(Model):
    class Meta:
        database = db

class Downloaded(BaseModel):
    id = AutoField(column_name='ID', null=True)
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

    def addto_localDB(self, dictit :dict , status :int = 1):
        try:
            if db.autoconnect == False:
                db.connect()
            try:
                downloaded = Downloaded.get(chat_id=dictit['chat_id'], message_id=dictit['message_id'])
                if downloaded:
                    if downloaded.status == status:
                        db.close()
                        return  # 成功则说明存在此条数据，无需再写入
                    else:
                        downloaded.status = status
                        downloaded.save()
                        db.close()
                        return  # 成功则说明存在此条数据，无需再写入
            except peewee.DoesNotExist:
                pass

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
            downloaded.status = status
            downloaded.save()
            db.close()
        except Exception as e:
            logger.error(
                f"[{e}].",
                exc_info=True,
            )
            db.close()

    def search_by_ids(self, chat_id :int, message_id :int, status : int = 1):
        try:
            if db.autoconnect == False:
                db.connect()
            downloaded = Downloaded.get(chat_id = chat_id, message_id = message_id, status = status)
            if downloaded:
                # 存在，直接读取
                dictit = {
                    'chat_id': downloaded.chat_id,
                    'message_id': downloaded.message_id,
                    'filename': downloaded.filename,
                    'caption': downloaded.caption,
                    'title': downloaded.title,
                    'mime_type': downloaded.mime_type,
                    'media_size': downloaded.media_size,
                    'media_duration': downloaded.media_duration,
                    'media_addtime': downloaded.media_addtime,
                    'chat_username': downloaded.chat_username,
                    'chat_title': downloaded.chat_title,
                    'addtime': downloaded.addtime,
                    'status': downloaded.status
                }
                db.close()
                return dictit
            else:
                db.close()
                return None
        except peewee.DoesNotExist:
            db.close()
            return False
        except Exception as e:
            logger.error(
                f"[{e}].",
                exc_info=True,
            )
            db.close()
            return None

    def search_ids_by_status(self, chat_id :int, status :int):
        ids = []
        try:
            if db.autoconnect == False:
                db.connect()
            downloaded = Downloaded.select(Downloaded.message_id).where(Downloaded.chat_id == chat_id, Downloaded.status == status)
            if downloaded:
                # 存在，直接
                for record in downloaded:
                    ids.append(record.message_id)
                db.close()
                return ids
            else:
                db.close()
                return None
        except peewee.DoesNotExist:
            db.close()
            return False
        except Exception as e:
            logger.error(
                f"[{e}].",
                exc_info=True,
            )
            db.close()
            return None

    def search_to_continue_msgs_all(self, status :int = 2):
        try:
            if db.autoconnect == False:
                db.connect()
            downloaded = Downloaded.select(Downloaded.chat_id, Downloaded.chat_username, Downloaded.message_id).where(Downloaded.status == status)
            if downloaded:
                db.close()
                return downloaded
            else:
                db.close()
                return None
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
    def search_to_continue_chats(self, status :int):
        try:
            if db.autoconnect == False:
                db.connect()
            downloaded = Downloaded.select(Downloaded.chat_id, Downloaded.chat_username).distinct().where(Downloaded.status == status)
            if downloaded:
                db.close()
                return downloaded
            else:
                db.close()
                return None
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

    def search_to_continue_mgs_by_chatid(self, chat_id: int, status :int):
        try:
            if db.autoconnect == False:
                db.connect()
            downloaded = Downloaded.select(Downloaded.message_id).where(Downloaded.chat_id == chat_id, Downloaded.status == status)
            if downloaded:
                db.close()
                return downloaded
            else:
                db.close()
                return None
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

    def is_exist_by_ids(self, chat_id :int, message_id :int, status: int = 1):
        try:
            if db.autoconnect == False:
                db.connect()
            if Downloaded.get(chat_id = chat_id, message_id = message_id, status = status):
                db.close()
                return True
            else:
                db.close()
                return False
        except peewee.DoesNotExist:
            db.close()
            return False
        except Exception as e:
            logger.error(
                f"[{e}].",
                exc_info=True,
            )
            db.close()
            return False




    def exist_filename_similar(self, mime_type :str, media_size :int, filename :str, title: str, status: int = 1):
        try:
            similar = 0
            if db.autoconnect == False:
                db.connect()
            downloaded = Downloaded.select().where(Downloaded.mime_type == mime_type, Downloaded.media_size == media_size, Downloaded.status==status)
            if filename and '.' in filename:
                filename = filename.split(".")[0]#todo 去掉 message 前缀 写一个函数来处理
            if not downloaded:
                db.close()
                return 0
            for record in downloaded:
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
            return 0
        except Exception as e:
            logger.error(
                f"[{e}{filename}].",
                exc_info=True,
            )
            db.close()
            return False


    def max_by_ids(self, chat_id :int):
        max_id = 1
        try:
            if db.autoconnect == False:
                db.connect()
            max_id = Downloaded.select(fn.Max(Downloaded.message_id)).scalar()
        except peewee.DoesNotExist:
            db.close()
            return False
        except Exception as e:
            logger.error(
                f"[{e}].",
                exc_info=True,
            )
        finally:
            db.close()
        return max_id


class SqliteSequence(BaseModel):
    name = BareField(null=True)
    seq = BareField(null=True)

    class Meta:
        table_name = 'sqlite_sequence'
        primary_key = False
