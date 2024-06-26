"""Downloads media from telegram."""
import asyncio
import logging
import os
import shutil
import time
import re
from enum import Enum
from typing import List, Optional

import pyrogram
from loguru import logger
from rich.logging import RichHandler

from typing import AsyncGenerator, Optional, Union

from tqdm.asyncio import tqdm

from module.app import Application, ChatDownloadConfig, DownloadStatus, TaskNode
from module.bot import start_download_bot, stop_download_bot
from module.download_stat import update_download_status, update_download_status_simple
from module.get_chat_history_v2 import get_chat_history_v2
from module.language import _t
from module.pyrogram_extension import (
    HookClient,
    fetch_message,
    record_download_status,
    report_bot_download_status,
    set_max_concurrent_transmissions,
    set_meta_data,
    upload_telegram_chat,
)
from module.web import init_web

from utils.format import (

    validate_title,
    process_string,
    find_files_with_prefix,

    find_missing_files,
    merge_files_cat,
    merge_files_write,
    merge_files_shutil,
    get_folder_files_size
)
from utils.log import LogFilter
from utils.meta import print_meta
from utils.meta_data import MetaData

from module import sqlmodel


logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt='%m-%d %H:%M:%S',
    handlers=[RichHandler()]
)

CONFIG_NAME = "config.yaml"
DATA_FILE_NAME = "data.yaml"
APPLICATION_NAME = "media_downloader"
app = Application(CONFIG_NAME, DATA_FILE_NAME, APPLICATION_NAME)

queue_maxsize = 100
queue: asyncio.Queue = asyncio.Queue(maxsize = queue_maxsize)

RETRY_TIME_OUT = 3

CHUNK_MIN = 10

similar_set = 0.90
sizerange_min = 0.01


logging.getLogger("pyrogram.session.session").addFilter(LogFilter())
logging.getLogger("pyrogram.client").addFilter(LogFilter())

logging.getLogger("pyrogram").setLevel(logging.WARNING)

db = sqlmodel.Downloaded()


def check_download_finish(media_size: int, download_path: str, ui_file_name: str, chunk_count: int):
    files_count, total_size = get_folder_files_size(download_path)

    if files_count != chunk_count or total_size != media_size:
        return False
    return True


def merge_chunkfile(folder_path: str, output_file: str, chunk_count: int, file_size: int, method: str):
    directory, _ = os.path.split(output_file)
    os.makedirs(directory, exist_ok=True)
    file_list = os.listdir(folder_path)
    if chunk_count != len(file_list):
        return False
    if method == 'cat':
        merge_files_cat(folder_path, output_file)
    elif method == 'write':
        merge_files_write(folder_path, output_file)
    elif method == 'shutil':
        merge_files_shutil(folder_path, output_file)
    if _is_exist(output_file) and os.path.getsize(output_file) == file_size:
        return True
    else:
        os.remove(output_file)
        return False

def _check_timeout(retry: int, _: int):

    if retry >= 4:
        return True
    return False

def _is_exist(file_path: str) -> bool:
    return not os.path.isdir(file_path) and os.path.exists(file_path)

class Msg_db_Status(Enum):

    DB_No_Exist = 0 # 数据库中不存在
    DB_Exist = 1 # 数据库中存在
    DB_Downloading = 2  # 数据库中下载中
    DB_Aka_Exist = 3 # 数据库中等价内容存在
    DB_Aka_Downloading = 4  # 数据库中等价内容下载中
    DB_Passed = 5  # 数据库中等价内容存在

class Msg_file_Status(Enum):

    File_No_Exist = 0 # 文件系统中不存在
    File_Exist = 1 # 文件系统中存在
    File_Aka_Exist = 2  # 文件系统中等价文件存在

def _get_msg_db_status(msg_dict: dict) :

    msg_chat_id = msg_dict.get('chat_id')
    try:
        msg_db_status = db.getStatus(msg_chat_id, msg_dict.get('message_id'))
    except Exception as e:
        logger.error(
            f"[{e}].",
            exc_info=True,
        )

    if msg_db_status == 0: #数据库里没这条数据
        db_files = db.get_similar_files(msg_dict,similar_set,sizerange_min,[1,2]) #看看是否有等价内容数据 4因为依附于1 暂时不管
        if db_files and len(db_files) >= 1:
            for db_file in db_files:
                if db_file.status == 1 or db_file.status == 4: #等价存在
                    msg_db_status = Msg_db_Status.DB_Aka_Exist
                    break
                elif db_file.status == 2: #等价下载中
                    msg_db_status = Msg_db_Status.DB_Aka_Downloading
                    break
        else:
            msg_db_status = Msg_db_Status.DB_No_Exist
    elif msg_db_status == 1:
        msg_db_status = Msg_db_Status.DB_Exist
    elif msg_db_status == 2:
        msg_db_status = Msg_db_Status.DB_Downloading
    elif msg_db_status == 4:
        msg_db_status = Msg_db_Status.DB_Aka_Exist
    elif msg_db_status == 3:
        msg_db_status = Msg_db_Status.DB_Passed


    return msg_db_status

def _get_msg_file_status(msg_dict: dict) :

    if _is_exist(msg_dict.get('file_fullname')):
        msg_file_status = Msg_file_Status.File_Exist #文件存在
    else:
        file_dir = os.path.dirname(msg_dict.get('file_fullname'))
        filename_pre = f"[{msg_dict.get('message_id')}]"
        files = find_files_with_prefix(file_dir, filename_pre)
        if files and len(files)>=1:
            msg_file_status = Msg_file_Status.File_Exist #文件存在
        else:
            msg_file_status = Msg_file_Status.File_No_Exist #文件不存在

    return msg_file_status



# pylint: disable = R0912
def _get_media_meta(
    message: pyrogram.types.Message
) -> dict:

    media_dict = {}
    msg_time = ''

    try:
        if message.chat.id < 0:
            msg_real_chat_id = 0 - message.chat.id - 1000000000000
        else:
            msg_real_chat_id = message.chat.id

        msg_real_chat_username = message.chat.username
        msg_real_message_id = message.id
        msg_real_chat_title = validate_title(message.chat.title)

        msg_from = False  # 是否转发的信息
        if message.forward_from_chat and message.forward_from_chat.id and message.forward_from_message_id:
            msg_from_chat_id = 0 - message.forward_from_chat.id - 1000000000000
            msg_from_chat_username = message.forward_from_chat.username
            msg_from_message_id = message.forward_from_message_id
            msg_from_chat_title = validate_title(message.forward_from_chat.title)
            msg_from = True

        if message.date:
            msg_time = message.date.strftime("%Y-%m-%d %H:%M")

        msg_caption = process_string(validate_title(getattr(message, "caption", '')))
        msg_media_group_id = getattr(message, "media_group_id", None)

        if msg_caption:
            app.set_caption_name(msg_real_message_id, msg_media_group_id, msg_caption)
        else:
            msg_caption = app.get_caption_name(msg_real_message_id, msg_media_group_id)

        if message.audio and message.audio != '':
            msg_type = 'audio'
            msg_filename = message.audio.file_name
            msg_duration = message.audio.duration
            msg_size = message.audio.file_size
        elif message.video and message.video != '':
            msg_type = 'video'
            msg_filename = message.video.file_name
            msg_duration = message.video.duration
            msg_size = message.video.file_size
        elif message.photo and message.photo != '':
            msg_type = 'photo'
            if not msg_real_chat_username:
                msg_filename = f"[{msg_real_chat_id}]{msg_real_message_id}.jpg"
            else:
                msg_filename = f"[{msg_real_chat_username}]{msg_real_message_id}.jpg"
            msg_duration = 0
            msg_size = message.photo.file_size
        elif message.document and message.document != '':
            msg_type = 'document'
            msg_filename = message.document.file_name
            msg_duration = 0
            msg_size = message.document.file_size
        else:
            logger.info(
                f"无需处理的媒体类型: ",
                exc_info=True,
            )
            return None

        if msg_filename and msg_filename != '':
            msg_file_onlyname, msg_file_ext = os.path.splitext(msg_filename)
        else:
            msg_file_onlyname = 'No_Name'
            msg_file_ext = '.unknown'

        msg_file_ext = msg_file_ext.replace('.','')

        if msg_caption and msg_caption != '' and msg_filename and (
                'telegram' in msg_filename.lower() or re.sub(r'[._\-\s]', '',
                                                             msg_file_onlyname).isdigit()):
            msg_filename = app.get_file_name(msg_real_message_id,
                                             f"{msg_caption}{msg_file_ext}",
                                             msg_caption)
        else:
            msg_filename = validate_title(app.get_file_name(msg_real_message_id, msg_filename, msg_caption))

        msg_title = process_string(msg_file_onlyname)

        if not msg_real_chat_username or msg_real_chat_username == '':
            subdir = validate_title(f"[{msg_real_chat_id}]{msg_real_chat_id}")
        else:
            subdir = validate_title(f"[{msg_real_chat_id}]{msg_real_chat_username}")

        subdir = os.path.join(subdir, str(msg_real_message_id // 100 * 100).zfill(6))

        file_save_url = os.path.join(app.get_file_save_path(msg_type, msg_real_chat_title, msg_time), subdir, msg_filename)
        temp_save_url = os.path.join(app.temp_save_path, subdir, msg_filename)

        if not msg_filename or 'None' in file_save_url:
            if not msg_real_chat_username:
                logger.error(f"[{msg_real_chat_id}]{msg_real_message_id}: ", exc_info=True, )
            else:
                logger.error(f"[{msg_real_chat_username}]{msg_real_message_id}: ",exc_info=True,)

        if msg_from: #
            media_dict = {
                'chat_id': msg_real_chat_id,
                'message_id': msg_real_message_id,
                'filename': msg_filename,
                'caption': msg_caption,
                'title': msg_title,
                'mime_type': msg_file_ext,
                'media_size': msg_size,
                'media_duration': msg_duration,
                'media_addtime': msg_time,
                'chat_username': msg_real_chat_username,
                'chat_title': msg_real_chat_title,
                'file_fullname': file_save_url,
                'temp_file_fullname': temp_save_url,
                'msg_from': msg_from,
                'msg_from_chat_id': msg_from_chat_id,
                'msg_from_chat_username': msg_from_chat_username,
                'msg_from_message_id': msg_from_message_id,
                'msg_from_chat_title': msg_from_chat_title,
                'msg_type': msg_type,
                'msg_link': message.link
            }
        else:
            media_dict = {
                'chat_id': msg_real_chat_id,
                'message_id': msg_real_message_id,
                'filename': msg_filename,
                'caption': msg_caption,
                'title': msg_title,
                'mime_type': msg_file_ext,
                'media_size': msg_size,
                'media_duration': msg_duration,
                'media_addtime': msg_time,
                'chat_username': msg_real_chat_username,
                'chat_title': msg_real_chat_title,
                'file_fullname': file_save_url,
                'temp_file_fullname': temp_save_url,
                'msg_type': msg_type,
                'msg_link': message.link
            }
    except Exception as e:
        logger.error(
            f"Message[{message.id}]: "
            f"{_t('some info is missed')}:\n[{e}].",
            exc_info=True,
        )

    return media_dict



async def add_download_task(
    message: pyrogram.types.Message,
    msg_dict: dict,
    node: TaskNode,
):
    if message.empty:
        return False

    if not msg_dict or msg_dict == None:
        msg_dict = _get_media_meta(message)

    if not msg_dict.get('chat_username') or msg_dict.get('chat_username') == '':
        show_chat_username = str(msg_dict.get('chat_id'))
    else:
        show_chat_username = msg_dict.get('chat_username')

    node.download_status[message.id] = DownloadStatus.Downloading
    await queue.put((message, node))
    # 写入数据库 记录进入下载队列
    msg_dict['status'] = 2  # 2 下载中
    insert_into_db(msg_dict)
    logger.info(f"加入队列[{show_chat_username}]{msg_dict.get('filename')}   当前队列长：{queue.qsize()}")
    node.total_task += 1

    return True

async def download_task(
    client: pyrogram.Client, message: pyrogram.types.Message, node: TaskNode
):

    download_status, file_name = await download_media(client, message, node)

    if not node.bot:
        app.set_download_id(node, message.id, download_status)

    node.download_status[message.id] = download_status

    file_size = os.path.getsize(file_name) if file_name else 0

    await upload_telegram_chat(
        client,
        node.upload_user if node.upload_user else client,
        app,
        node,
        message,
        download_status,
        file_name,
    )

    # rclone upload
    if (
        not node.upload_telegram_chat_id
        and download_status is DownloadStatus.SuccessDownload
    ):
        if await app.upload_file(file_name):
            node.upload_success_count += 1

    await report_bot_download_status(
        node.bot,
        node,
        download_status,
        file_size,
    )


# pylint: disable = R0915,R0914
def insert_into_db(media_dict:dict):
    try:
        db_status = db.getStatus(chat_id=media_dict.get('chat_id'), message_id=media_dict.get('message_id'))
        if db_status == 0: #不存在记录则插入
            db.msg_insert_to_db(media_dict)
        else: #存在记录则更新
            db.msg_update_to_db(media_dict)

    except Exception as e:
        # pylint: disable = C0301
        logger.error(
            f"[{e}].",
            exc_info=True,
        )


def save_chunk_to_file(chunk, file_path, file_name):
    if not os.path.exists(file_path):
        os.makedirs(file_path, exist_ok=True)

    file_url = os.path.join(file_path, file_name)
    chunk_size = len(chunk)
    if not _is_exist(file_url) or os.path.getsize(file_url) < chunk_size:
        with open(file_url, "wb") as f:
            f.write(chunk)
    if _is_exist(file_url) and os.path.getsize(file_url) == chunk_size:
        return True
    else:
        return False


@record_download_status
async def download_media(
    client: pyrogram.client.Client,
    message: pyrogram.types.Message,
    node: TaskNode,
):
    msg_dict = _get_media_meta(message)

    if _get_msg_file_status(msg_dict) == Msg_file_Status.File_Exist:
        await update_download_status(msg_dict.get('media_size'), msg_dict.get('media_size'), message.id, msg_dict.get('filename'),
                                     time.time(),
                                     node, client)
        msg_dict['status'] = 1
        insert_into_db(msg_dict)

        return DownloadStatus.SuccessDownload, msg_dict.get('filename')

    task_start_time: float = time.time()
    _media = None

    media_dict = _get_media_meta(message)
    message_id = media_dict.get('message_id')
    _media = media_dict
    file_name = media_dict.get('file_fullname')
    temp_file_name = media_dict.get('temp_file_fullname')
    media_size = media_dict.get('media_size')
    _type = media_dict.get('msg_type')

    if media_dict.get('chat_username'):
        show_chat_username = media_dict.get('chat_username')
    else:
        show_chat_username = str(media_dict.get('chat_id'))

    ui_file_name = file_name.split('/')[-1]

    if app.hide_file_name:
        ui_file_name = f"****{os.path.splitext(file_name.split('/')[0])}"


    for retry in range(3):
        try:
            temp_file_path = os.path.dirname(temp_file_name)
            chunk_dir = f"{temp_file_path}/{message_id}_chunk"
            # logger.info(f"开始下载[{show_chat_username}]{media_dict.get('filename')}...")

            if media_size < 1024 * 1024 * CHUNK_MIN: # 小于CHUNK_MIN M的就用单一文件下载
                chunk_count = 1
                chunk_filename = os.path.join(chunk_dir, "00000000")
                if os.path.exists(chunk_dir):
                    shutil.rmtree(chunk_dir)

                os.makedirs(chunk_dir, exist_ok=True)
                try:
                    await client.download_media(
                        message,
                        file_name=chunk_filename,
                        progress=update_download_status,
                        progress_args=(
                            message_id,
                            ui_file_name,
                            task_start_time,
                            node,
                            client,
                        ),
                    )
                except pyrogram.errors.exceptions.bad_request_400.BadRequest:
                    logger.warning(
                        f"[{show_chat_username}]{message_id}: {_t('file reference expired, refetching')}..."
                    )
                    await asyncio.sleep(RETRY_TIME_OUT)
                    message = await fetch_message(client, message)
                    if _check_timeout(retry, message_id):
                        # pylint: disable = C0301
                        logger.error(
                            f"[{show_chat_username}]{message_id}]: "
                            f"{_t('file reference expired for 3 retries, download skipped.')}"
                        )
                except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
                    await asyncio.sleep(wait_err.value)
                    logger.warning(f"[{show_chat_username}]: FlowWait ", message_id, wait_err.value)
                    _check_timeout(retry, message_id)
                except Exception as e:
                    logger.exception(f"{e}")
                    pass
            else: #大文件 采用分快下载模式
                if not os.path.exists(chunk_dir):
                    os.makedirs(chunk_dir, exist_ok=True)
                else:
                    temp_file = os.path.join(chunk_dir, '00000000.temp')
                    temp_file_end = os.path.join(chunk_dir, '00000000')
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                    if os.path.exists(temp_file_end) and os.path.getsize(temp_file_end) > 1024 * 1024:
                        os.remove(temp_file_end)
                chunk_count = int(media_size / 1024 / 1024) + 1
                chunks_to_down = find_missing_files(chunk_dir, chunk_count)
                if chunks_to_down and len(chunks_to_down) >= 1:  # 至少有一批
                    for start_id, end_id in chunks_to_down:  # 遍历缺失的文件批次
                        down_byte = int(start_id) * 1024 * 1024
                        chunk_it = start_id
                        try:
                            async for chunk in client.stream_media(message, offset=start_id,
                                                                   limit=end_id - start_id + 1):
                                chunk_filename = f"{str(int(chunk_it)).zfill(8)}"
                                chunk_it += 1
                                down_byte += len(chunk)
                                save_chunk_to_file(chunk, chunk_dir, chunk_filename)
                                await update_download_status(down_byte, media_size, message_id, ui_file_name,
                                                             task_start_time,
                                                             node, client)
                                await asyncio.sleep(RETRY_TIME_OUT)
                        except pyrogram.errors.exceptions.bad_request_400.BadRequest:
                            logger.warning(
                                f"[{show_chat_username}]{message_id}: {_t('file reference expired, refetching')}..."
                            )
                            await asyncio.sleep(RETRY_TIME_OUT)
                            message = await fetch_message(client, message)
                            if _check_timeout(retry, message_id):
                                # pylint: disable = C0301
                                logger.error(
                                    f"[{show_chat_username}]{message_id}]: "
                                    f"{_t('file reference expired for 3 retries, download skipped.')}"
                                )
                        except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
                            await asyncio.sleep(wait_err.value)
                            logger.warning(f"[{show_chat_username}]: FlowWait ", message_id, wait_err.value)
                            _check_timeout(retry, message_id)
                        except Exception as e:
                            logger.exception(f"{e}")
                            pass

            #判断一下是否下载完成
            if chunk_dir and os.path.exists(chunk_dir): #chunk_dir存在
                if check_download_finish(media_size, chunk_dir, ui_file_name, chunk_count): # 大小数量一致
                    try:
                        if merge_chunkfile(folder_path=chunk_dir, output_file=file_name, chunk_count=chunk_count,
                                        file_size=media_size, method='shutil'):
                            # await asyncio.sleep(RETRY_TIME_OUT)
                            if _is_exist(file_name) and os.path.getsize(file_name) == media_size:
                                shutil.rmtree(chunk_dir)

                            media_dict['status'] = 1
                            insert_into_db(media_dict)

                            logger.success(f"完成下载{file_name}...剩余：{queue.qsize()}")

                            return DownloadStatus.SuccessDownload, file_name
                    except Exception as e:
                        logger.exception(f"{e}")
                        pass
            else:
                pass
        except pyrogram.errors.exceptions.bad_request_400.BadRequest:
            logger.warning(
                f"[{show_chat_username}]{message_id}: {_t('file reference expired, refetching')}..."
            )
            await asyncio.sleep(RETRY_TIME_OUT)
            message = await fetch_message(client, message)
            if _check_timeout(retry, message_id):
                # pylint: disable = C0301
                logger.error(
                    f"[{show_chat_username}]{message_id}]: "
                    f"{_t('file reference expired for 3 retries, download skipped.')}"
                )
        except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
            await asyncio.sleep(wait_err.value)
            logger.warning(f"[{show_chat_username}]: FlowWait ", message_id, wait_err.value)
            _check_timeout(retry, message_id)

        except TypeError:
            # pylint: disable = C0301
            logger.warning(
                f"{_t('Timeout Error occurred when downloading Message')}[{show_chat_username}]{message_id}, "
                f"{_t('retrying after')} {RETRY_TIME_OUT} {_t('seconds')}"
            )
            await asyncio.sleep(RETRY_TIME_OUT)
            if _check_timeout(retry, message_id):
                logger.error(
                    f"[{show_chat_username}]{message_id}: {_t('Timing out after 3 reties, download skipped.')}"
                )
        except Exception as e:
            # pylint: disable = C0301
            logger.error(
                f"[{show_chat_username}]{message_id}: "
                f"{_t('could not be downloaded due to following exception')}:\n[{e}].",
                exc_info=True,
            )
            break

    return DownloadStatus.FailedDownload, None


def _load_config():
    """Load config"""
    app.load_config()


def _check_config() -> bool:
    """Check config"""
    print_meta(logger)
    try:
        _load_config()
        logger.add(
            os.path.join(app.log_file_path, "tdl.log"),
            rotation="10 MB",
            retention="10 days",
            level=app.log_level,
        )
    except Exception as e:
        logger.exception(f"load config error: {e}")
        return False

    return True


async def worker(client: pyrogram.client.Client):
    """Work for download task"""
    while app.is_running:
        try:
            item = await queue.get()
            message = item[0]
            node: TaskNode = item[1]

            if node.is_stop_transmission:
                continue

            if node.client:
                await download_task(node.client, message, node)
            else:
                await download_task(client, message, node)
        except Exception as e:
            logger.exception(f"{e}")

def need_skip_message(message, chat_download_config):
    try:
        # Case 1 不是媒体类型就跳过
        if not (message.audio or message.video or message.photo or message.document):
            return True

        # Case 2 不符合config文件条件就跳过 受 media_types file_formats 以及filter 控制
        meta_data = MetaData()
        set_meta_data(meta_data, message, '')
        if meta_data.file_extension and meta_data.file_extension != '' and (
        not 'all' in app.file_formats[meta_data.media_type]) and (
                not meta_data.file_extension.replace('.', '').lower() in app.file_formats[meta_data.media_type]):
            return True
        if not app.exec_filter(chat_download_config, meta_data):
            return True

        return False

    except Exception as e:
        logger.exception(f"{e}")


async def download_chat_task(
    client: pyrogram.Client,
    chat_download_config: ChatDownloadConfig,
    node: TaskNode,
):
    try:

        if str(node.chat_id).isdigit():
            real_chat_id = 0 - node.chat_id - 1000000000000
        else:
            real_chat_id = node.chat_id

        chat_download_config.node = node

        if chat_download_config.ids_to_retry:
            retry_ids = list(chat_download_config.ids_to_retry)
            logger.info(f"[{node.chat_id}]{_t('Downloading files failed during last run')}...")
            downloading_messages = Optional[AsyncGenerator["types.Message", None]]
            batch_size = 200
            for i in range(0, len(retry_ids), batch_size):
                batch_files = retry_ids[i:i + batch_size]
                try:
                    downloading_messages = await client.get_messages(  # type: ignore
                        chat_id=real_chat_id, message_ids=batch_files
                    )
                except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
                    await asyncio.sleep(wait_err.value)
                except Exception as e:
                    logger.exception(f"{e}")

                if downloading_messages and len(downloading_messages) > 0:
                    try:
                         for message in downloading_messages:
                            if need_skip_message(message, chat_download_config) : # 不在下载范围内
                                node.download_status[message.id] = DownloadStatus.SkipDownload
                                await upload_telegram_chat(
                                    client,
                                    node.upload_user,
                                    app,
                                    node,
                                    message,
                                    DownloadStatus.SkipDownload,
                                )
                                continue
                            else:
                                # msg_dict = _get_media_meta(message)
                                # await add_download_task(message, msg_dict, node)

                                msg_dict = _get_media_meta(message)
                                msg_db_status = _get_msg_db_status(msg_dict)
                                msg_file_status = _get_msg_file_status(msg_dict)
                                if msg_db_status == Msg_db_Status.DB_Exist:
                                    if msg_file_status == Msg_file_Status.File_Exist or msg_file_status == Msg_file_Status.File_Aka_Exist:
                                        # 数据库有 文件有
                                        continue
                                    else:
                                        # 数据库有 文件没了 重新下载
                                        await add_download_task(message, msg_dict, node)
                                elif msg_db_status == Msg_db_Status.DB_Aka_Exist:
                                    # 数据库等价有 等价文件有没有暂时不管
                                    # TODO 等价文件有没有问题解决
                                    msg_dict['status'] = 4
                                    insert_into_db(msg_dict)  # 写入数据库一条等价信息
                                    continue
                                elif msg_db_status == Msg_db_Status.DB_Aka_Downloading:
                                    # 数据库有其他等价文件在下载
                                    continue
                                elif msg_db_status == Msg_db_Status.DB_No_Exist or msg_db_status == Msg_db_Status.DB_Downloading:
                                    if msg_file_status == Msg_file_Status.File_Exist:
                                        # 数据库没有 文件有
                                        msg_dict['status'] = 1  # 补写入数据库
                                        insert_into_db(msg_dict)
                                        continue
                                    elif msg_file_status == Msg_file_Status.File_Aka_Exist:
                                        # 数据库没有 等价文件有
                                        msg_dict['status'] = 4  # 补写入数据库
                                        insert_into_db(msg_dict)
                                        continue
                                    else:
                                        # 数据库没有 文件也没 下载
                                        await add_download_task(message, msg_dict, node)
                                elif msg_db_status == Msg_db_Status.DB_Passed:
                                    continue

                            await asyncio.sleep(RETRY_TIME_OUT)
                    except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
                        await asyncio.sleep(wait_err.value)
                    except Exception as e:
                        logger.exception(f"{e}")

                await asyncio.sleep(RETRY_TIME_OUT)


        """Download all task"""
        messages_iter = get_chat_history_v2(
            client,
            real_chat_id,
            limit=node.limit,
            max_id=node.end_offset_id,
            offset_id=chat_download_config.last_read_message_id,
            reverse=True,
        )
        message_line = tqdm(messages_iter)

        async for message in message_line:  # type: ignore
            message_line.set_description("[%s]" % node.chat_id)

            if need_skip_message(message, chat_download_config):  # 不在下载范围内
                node.download_status[message.id] = DownloadStatus.SkipDownload
                await upload_telegram_chat(
                    client,
                    node.upload_user,
                    app,
                    node,
                    message,
                    DownloadStatus.SkipDownload,
                )
                continue
            else:
                msg_dict = _get_media_meta(message)
                msg_db_status = _get_msg_db_status(msg_dict)
                msg_file_status = _get_msg_file_status(msg_dict)
                if msg_db_status == Msg_db_Status.DB_Exist:
                    if msg_file_status == Msg_file_Status.File_Exist or msg_file_status == Msg_file_Status.File_Aka_Exist:
                        # 数据库有 文件有
                        continue
                    else:
                        # 数据库有 文件没了 重新下载
                        await add_download_task(message, msg_dict, node)
                elif msg_db_status == Msg_db_Status.DB_Aka_Exist:
                    # 数据库等价有 等价文件有没有暂时不管
                    # TODO 等价文件有没有问题解决
                    msg_dict['status'] = 4
                    insert_into_db(msg_dict)  # 写入数据库一条等价信息
                    continue
                elif msg_db_status == Msg_db_Status.DB_Downloading or msg_db_status == Msg_db_Status.DB_Aka_Downloading:
                    # 数据库有其他等价文件在下载
                    continue
                elif msg_db_status == Msg_db_Status.DB_No_Exist:
                    if msg_file_status == Msg_file_Status.File_Exist or msg_file_status == Msg_file_Status.File_Aka_Exist:
                        # 数据库没有 文件有
                        msg_dict['status'] = 1  # 补写入数据库
                        insert_into_db(msg_dict)
                        continue
                    else:
                        # 数据库没有 文件也没 下载
                        await add_download_task(message, msg_dict, node)
                elif msg_db_status == Msg_db_Status.DB_Passed:
                    continue

        chat_download_config.need_check = True
        chat_download_config.total_task = node.total_task
        node.is_running = True
    except Exception as e:
        logger.exception(f"{e}")


async def download_all_chat(client: pyrogram.Client):
    """Download All chat"""
    logger.info(f"开始读取全部Chat...")
    for key, value in app.chat_download_config.items():
        value.node = TaskNode(chat_id=key)
        try:
            await download_chat_task(client, value, value.node)
        except Exception as e:
            logger.warning(f"Download {key} error: {e}")
        finally:
            value.need_check = True
            logger.info(f"{_t('update config')}......")
            app.update_config()
    logger.info(f"读取全部Chat完毕...")



async def run_until_all_task_finish():
    """Normal download"""
    while True:
        finish: bool = True
        for _, value in app.chat_download_config.items():
            if not value.need_check or value.total_task != value.finish_task:
                finish = False

        if (not app.bot_token and finish) or app.restart_program:
            break

        await asyncio.sleep(RETRY_TIME_OUT)


def _exec_loop():
    """Exec loop"""

    app.loop.run_until_complete(run_until_all_task_finish())


async def start_server(client: pyrogram.Client):
    """
    Start the server using the provided client.
    """
    await client.start()


async def stop_server(client: pyrogram.Client):
    """
    Stop the server using the provided client.
    """
    await client.stop()

def main_download(client, tasks):
    task0 = app.loop.create_task(download_all_chat(client))
    tasks.append(task0)

    for _ in range(app.max_download_task):
        task = app.loop.create_task(worker(client))
        tasks.append(task)

    if app.bot_token:
        app.loop.run_until_complete(start_download_bot(app, client, add_download_task, download_chat_task))
    _exec_loop()

def main_clean(client, tasks):
    app.is_running = False
    if app.bot_token:
        app.loop.run_until_complete(stop_download_bot())
    app.loop.run_until_complete(stop_server(client))
    for task in tasks:
        task.cancel()
    logger.info(_t("Stopped!"))
    logger.info(f"{_t('update config')}......")
    app.update_config()
    logger.success(
        f"{_t('Updated last read message_id to config file')},"
        f"{_t('total download')} {app.total_download_task}, "
        f"{_t('total upload file')} "
        f"{app.cloud_drive_config.total_upload_success_file_count}"
    )

def main():
    """Main function of the downloader."""
    tasks = []
    client = HookClient(
        "media_downloader",
        api_id=app.api_id,
        api_hash=app.api_hash,
        proxy=app.proxy,
        workdir=app.session_file_path,
        start_timeout=app.start_timeout,
    )
    try:
        app.pre_run()
        init_web(app)
        set_max_concurrent_transmissions(client, app.max_concurrent_transmissions)
        app.loop.run_until_complete(start_server(client))
        logger.success(_t("Successfully started (Press Ctrl+C to stop)"))
        main_download(client, tasks)

    except KeyboardInterrupt:
        logger.info(_t("KeyboardInterrupt"))
    except Exception as e:
        logger.exception("{}", e)
    finally:
        main_clean(client, tasks)

if __name__ == "__main__":
    if _check_config():
        main()

