"""Downloads media from telegram."""
import asyncio
import logging
import os
import shutil
import time
import re
from typing import List, Optional

import sys

import pyrogram
from loguru import logger
from rich.logging import RichHandler
from tqdm import tqdm

from module.app import Application, ChatDownloadConfig, DownloadStatus, TaskNode
from module.bot import start_download_bot, stop_download_bot
from module.download_stat import update_download_status, update_download_status_simple
from module.get_chat_history_v2 import get_chat_history_v2
from module.language import _t
from module.pyrogram_extension import (
    HookClient,
    fetch_message,
    get_extension,
    record_download_status,
    report_bot_download_status,
    set_max_concurrent_transmissions,
    set_meta_data,
    upload_telegram_chat,
)
from module.web import init_web

from utils.format import (
    truncate_filename,
    validate_title,
    validate_title_clean,
    process_string,
    find_files_with_prefix,
    is_exist_files_with_prefix,
    guess_media_type,
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

from shutil import move


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

queue_maxsize = 0
queue: asyncio.Queue = asyncio.Queue(maxsize = queue_maxsize)

RETRY_TIME_OUT = 3

CHUNK_MIN = 10

similar_set = 0.92

logging.getLogger("pyrogram.session.session").addFilter(LogFilter())
logging.getLogger("pyrogram.client").addFilter(LogFilter())

logging.getLogger("pyrogram").setLevel(logging.WARNING)

db = sqlmodel.Downloaded()

CHECK_DB = False
CHECK_DUPLICATE = False

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


def _move_to_download_path(temp_download_path: str, download_path: str):
    """Move file to download path

    Parameters
    ----------
    temp_download_path: str
        Temporary download path

    download_path: str
        Download path

    """
    #TODO 需要处理一下channel 随便改名问题
    directory, _ = os.path.split(download_path)
    os.makedirs(directory, exist_ok=True)
    shutil.move(temp_download_path, download_path)


def _check_timeout(retry: int, _: int):
    """Check if message download timeout, then add message id into failed_ids

    Parameters
    ----------
    retry: int
        Retry download message times

    """
    if retry >= 4:
        return True
    return False


def _can_download(_type: str, file_formats: dict, file_format: Optional[str]) -> bool:
    """
    Check if the given file format can be downloaded.

    Parameters
    ----------
    _type: str
        Type of media object.
    file_formats: dict
        Dictionary containing the list of file_formats
        to be downloaded for `audio`, `document` & `video`
        media types
    file_format: str
        Format of the current file to be downloaded.

    Returns
    -------
    bool
        True if the file format can be downloaded else False.
    """
    if _type in ["audio", "document", "video"]:
        allowed_formats: list = file_formats[_type]
        if not file_format in allowed_formats and allowed_formats[0] != "all":
            return False
    return True


def get_exist_files(msgdict: dict) -> list:
    file_path = msgdict.get('file_fullname')
    if _is_exist(file_path):
        return file_path

def get_exist_files_aks_by_path(file_path) -> list:
    file_dir, filename = os.path.split(file_path)
    filename_pre = re.findall(r"\[.+?\]", filename)[0]
    aka_files = find_files_with_prefix(file_dir, filename_pre)
    if file_path in aka_files:
        aka_files.remove(file_path)
    return aka_files

def get_exist_files_aka(msgdict: dict) -> list:
    file_path = msgdict.get('file_fullname')
    return get_exist_files_aks_by_path(file_path)

def get_exist_files_forward(msgdict: dict) -> list:
    forword_files = []
    if msgdict.get('msg_from'):  # 判读转发源文件是否存在
        _, from_file_path, _ = get_media_info_str(msgdict.get('msg_from_chat_username'),
                                                  msgdict.get('msg_from_chat_id'),
                                                  msgdict.get('msg_from_message_id'),
                                                  re.sub(r"^\[\d+\]", "", msgdict.get('filename')),
                                                  msgdict.get('caption'),
                                                  msgdict.get('file_format'))  # 生成个名字
        if from_file_path:
            if _is_exist(from_file_path):
                forword_files.append(from_file_path)
            forword_aka_files = get_exist_files_aks_by_path(from_file_path)
            if forword_aka_files:
                forword_files.extend(forword_aka_files)
            return forword_files
        else:
            print('怎么回事！没有生成路径！')
    else:
        return None

def get_exist_files_db(msgdict: dict):
    return db.get_similar_files(msgdict.get('chat_id'), msgdict.get('message_id'), msgdict.get('mime_type'),msgdict.get('media_size'),msgdict.get('filename'), msgdict.get('title'),similar_set)

def is_exist_files_db(msgdict: dict):
    return db.getStatus(msgdict.get('chat_id'), msgdict.get('message_id'))

def _is_exist(file_path: str) -> bool:
    """
    Check if a file exists and it is not a directory.

    Parameters
    ----------
    file_path: str
        Absolute path of the file to be checked.

    Returns
    -------
    bool
        True if the file exists else False.
    """
    return not os.path.isdir(file_path) and os.path.exists(file_path)

# pylint: disable = R0912
def get_media_info_str(msg_chat_username: str, msg_chat_id: int, msg_message_id: int, msg_filename: str, msg_caption: str, media_type: str):

    if msg_filename and msg_filename != '':
        msg_file_onlyname, msg_file_format = os.path.splitext(msg_filename)
    else:
        msg_file_onlyname = 'No_Name'
        msg_file_format = '.unknown'

    if msg_caption and msg_caption != '' and msg_filename and (
            'telegram' in msg_filename.lower() or re.sub(r'[._\-\s]', '',
                                                         msg_file_onlyname).isdigit()):
        msg_filename = app.get_file_name(msg_message_id,
                                         f"{msg_caption}{msg_file_format}",
                                         msg_caption)
    else:
        msg_filename = validate_title(app.get_file_name(msg_message_id, msg_filename, msg_caption))

        # 处理存储用message衍生信息
    # if msg_chat_username == 'libraryforalls':
    #     print('debug')

    if not msg_chat_username:
        dirname = validate_title(f"[{str(msg_chat_id)}]{msg_chat_id}")
    else:
        dirname = validate_title(f"[{str(msg_chat_id)}]{msg_chat_username}")

    file_save_path = os.path.join(app.get_file_save_path(media_type, dirname, ''),
                                  str(int(msg_message_id) // 100 * 100).zfill(6))
    temp_save_path = os.path.join(app.temp_save_path, dirname,
                                  str(int(msg_message_id) // 100 * 100).zfill(6))

    file_save_url = os.path.join(file_save_path, truncate_filename(msg_filename))
    temp_save_url = os.path.join(temp_save_path, truncate_filename(msg_filename))

    return msg_filename, file_save_url, temp_save_url

def _get_media_meta(
    message: pyrogram.types.Message
) -> dict:
    """Extract file name and file id from media object.

    Parameters
    ----------
    message : pyrogram.types.Message

    Returns
    -------
    media_dict : dict
    """

    #待返回dict初始化
    media_dict = {}
    msg_time = ''
    # msg_date = ''
    try:
        # TODO 下面有bug 当是转发信息时 如果config里没有转发源 则后面会出错 先暂时不记录原始chat_id
        msg_from = False  # 是否转发的信息
        msg_real_chat_id = 0 - message.chat.id - 1000000000000
        msg_real_chat_username = message.chat.username
        msg_real_message_id = message.id
        msg_real_chat_title = message.chat.title

        # if msg_real_chat_username == 'libraryforalls':
        #     print('debug')

        if message.forward_from_chat and message.forward_from_chat.id and message.forward_from_message_id:
            msg_from_chat_id = 0 - message.forward_from_chat.id - 1000000000000
            msg_from_chat_username = message.forward_from_chat.username
            msg_from_message_id = message.forward_from_message_id
            msg_from_chat_title = validate_title(message.forward_from_chat.title)
            msg_from = True

        if message.date:
            msg_time = message.date.strftime("%Y-%m-%d %H:%M")
            # msg_date = message.date.strftime(app.date_format)

        msg_caption = getattr(message, "caption", '')
        msg_media_group_id = getattr(message, "media_group_id", None)

        if msg_caption:
            msg_caption = validate_title(msg_caption)
            app.set_caption_name(msg_real_message_id, msg_media_group_id, msg_caption)
        else:
            msg_caption = app.get_caption_name(msg_real_message_id, msg_media_group_id)

        if message.audio and message.audio != '':
            msg_type = 'audio'
            msg_filename = message.audio.file_name
            msg_duration = message.audio.duration
            msg_size = message.audio.file_size
            media_obj = message.audio
            msg_file_format = os.path.splitext(message.audio.file_name)[-1][1:]
            msg_title = ''
        elif message.video and message.video != '':
            msg_type = 'video'
            msg_filename = message.video.file_name
            msg_duration = message.video.duration
            msg_size = message.video.file_size
            media_obj = message.video
            msg_file_format = os.path.splitext(message.video.file_name)[-1][1:]
            msg_title = ''
        elif message.photo and message.photo != '':
            msg_type = 'photo'
            if not msg_real_chat_username:
                msg_filename = f"[{msg_real_chat_id}]{msg_real_message_id}.jpg"
            else:
                msg_filename = f"[{msg_real_chat_username}]{msg_real_message_id}.jpg"
            msg_duration = 0
            msg_size = message.photo.file_size
            media_obj = message.photo
            msg_file_format = os.path.splitext(msg_filename)[-1][1:]
            msg_title = ''
        elif message.document and message.document != '':
            msg_type = 'document'
            msg_filename = message.document.file_name
            msg_duration = 0
            msg_size = message.document.file_size
            media_obj = message.document
            msg_file_format = os.path.splitext(message.document.file_name)[-1][1:]
            msg_title = ''
        else:
            logger.info(
                f"无需处理的媒体类型: ",
                exc_info=True,
            )
            return None

        msg_filename, file_save_url, temp_save_url = get_media_info_str(msg_real_chat_username, msg_real_chat_id, msg_real_message_id, msg_filename, msg_caption, msg_type)

        if not msg_title or msg_title == '':
            msg_title = validate_title_clean(process_string(os.path.splitext(msg_filename)[0]))

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
                'mime_type': msg_file_format,
                'media_size': msg_size,
                'media_duration': msg_duration,
                'media_addtime': msg_time,
                'chat_username': msg_real_chat_username,
                'chat_title': msg_real_chat_title,
                'file_fullname': file_save_url,
                'temp_file_fullname': temp_save_url,
                'file_format': msg_type,
                'msg_from': msg_from,
                'msg_from_chat_id': msg_from_chat_id,
                'msg_from_chat_username': msg_from_chat_username,
                'msg_from_message_id': msg_from_message_id,
                'msg_from_chat_title': msg_from_chat_title,
            }
        else:
            media_dict = {
                'chat_id': msg_real_chat_id,
                'message_id': msg_real_message_id,
                'filename': msg_filename,
                'caption': msg_caption,
                'title': msg_title,
                'mime_type': msg_file_format,
                'media_size': msg_size,
                'media_duration': msg_duration,
                'media_addtime': msg_time,
                'chat_username': msg_real_chat_username,
                'chat_title': msg_real_chat_title,
                'file_fullname': file_save_url,
                'temp_file_fullname': temp_save_url,
                'file_format': msg_type,
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
    node: TaskNode,
):

    """Add Download task"""
    if message.empty:
        return False


    media_dict = _get_media_meta(message)
    file_path = media_dict.get('file_fullname')
    media_size = media_dict.get('media_size')

    if media_dict.get('chat_username') is None:
        show_chat_username = str(media_dict.get('chat_id'))
    else:
        show_chat_username = media_dict.get('chat_username')

    # if show_chat_username == 'libraryforalls':
    #     print('debug')


    # 改写逻辑 已存在就不进下载队列
    if get_exist_files(media_dict):# 文件严格存在
        if os.path.getsize(file_path) == media_size:  # 判读文件是否同大小
            # 严格存在 记录为已下载
            node.download_status[message.id] = DownloadStatus.SuccessDownload
            node.total_task += 1
            # 写入数据库 记录已完成下载
            media_dict['status'] = 1
            insert_into_db(media_dict)
            # 更新bot 展示状态
            await report_bot_download_status(
                node.bot,
                node,
                DownloadStatus.SuccessDownload,
                media_size,
            )
            logger.info(
                f"[{show_chat_username}]{media_dict.get('filename')}已存在[path]。 当前Chat已处理：{node.total_task}")

            return True

    aka_files = get_exist_files_aka(media_dict)
    if aka_files:# 广义文件存在
        for file_name_local in aka_files:
            if os.path.getsize(file_name_local) != media_size: # 广义本地文件不一样大
                os.remove(file_name_local) # 删掉
                logger.info(
                    f"[{show_chat_username}]{media_dict.get('filename')}大小不一致，已删除。 当前Chat已处理：{node.total_task}")
            else: # 广义本地文件一样大
                if file_name_local != file_path:  # 文件名不一致则按新规则命名
                    move(file_name_local, file_path)
                    logger.info(
                        f"[{show_chat_username}]:{os.path.split(file_name_local)[1]}被重命名为[{show_chat_username}]:{os.path.split(file_path)[1]} "
                    )

        #上面循环处理完 应该最多只剩一个本地文件并且名字应该符合命名标准 大小也一样大
        if _is_exist(file_path):
            # 广义存在 记录为已下载
            node.download_status[message.id] = DownloadStatus.SuccessDownload
            node.total_task += 1
            # 写入数据库 记录已完成下载
            media_dict['status'] = 1
            insert_into_db(media_dict)
            # 更新bot 展示状态
            await report_bot_download_status(
                node.bot,
                node,
                DownloadStatus.SuccessDownload,
                media_size,
            )
            logger.info(
                f"[{show_chat_username}]{media_dict.get('filename')}已存在[aka]。 当前Chat已处理：{node.total_task}")
            return True


    forward_files = get_exist_files_forward(media_dict)
    if forward_files:  # 不存在相同文件 但如果是转发msg 所以有可能在其他channel里已经下载了 判断一下
        for file_name_local in forward_files:
            if os.path.getsize(file_name_local) == media_size:  # 源channel中文件一样大
                # 源channel中文件存在 记录为已下载
                node.download_status[message.id] = DownloadStatus.SuccessDownload
                node.total_task += 1
                # 写入数据库 记录已完成下载
                media_dict['status'] = 1
                insert_into_db(media_dict)
                # 更新bot 展示状态
                await report_bot_download_status(
                    node.bot,
                    node,
                    DownloadStatus.SuccessDownload,
                    media_size,
                )
                logger.info(
                    f"[{show_chat_username}]{media_dict.get('filename')}已存在[forward]。 当前Chat已处理：{node.total_task}")
                return True


    db_files = get_exist_files_db(media_dict)
    if db_files:  # 不存在相同文件 但有可能在其他channel里已经下载了 用db来判断相似度
        # 严格文件 广义文件 源channel中文件均不存在 看相似文件在db中是否可能存在
        for db_file in db_files:
            if db_file.chat_username:
                indisk = check_msg_files_indb(chat_username = db_file.chat_username, message_id = db_file.message_id, chat_id= None)
            else:
                indisk = check_msg_files_indb(None, message_id=db_file.message_id, chat_id= db_file.chat_id)

            if indisk:
                # 有相似 且文件还存在
                node.download_status[message.id] = DownloadStatus.SuccessDownload
                node.total_task += 1
                # 写入数据库 记录已完成下载
                media_dict['status'] = 1
                insert_into_db(media_dict)
                # 更新bot 展示状态
                await report_bot_download_status(
                    node.bot,
                    node,
                    DownloadStatus.SuccessDownload,
                    media_size,
                )
                logger.info(
                    f"[{show_chat_username}]{media_dict.get('filename')}已存在[db]。 当前Chat已处理：{node.total_task}")
                return True



        # 以上情况都没发生 则说明真没有 那就加入队列下载吧
    node.download_status[message.id] = DownloadStatus.Downloading
    await queue.put((message, node))
    await update_download_status_simple(0, media_size, message.id, media_dict.get('filename'), time.time(),show_chat_username)
    # 写入数据库 记录进入下载队列
    media_dict['status'] = 2  # 2 下载中
    insert_into_db(media_dict)
    logger.info(f"加入队列[{show_chat_username}]{media_dict.get('filename')}   当前队列长：{queue.qsize()}")
    node.total_task += 1

    return True

async def download_task(
    client: pyrogram.Client, message: pyrogram.types.Message, node: TaskNode
):
    """Download and Forward media"""

    download_status, file_name = await download_media(
        client, message, app.media_types, app.file_formats, node
    )

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
    # TODO: 写入数据库 记录已完成下载
    try:
        db_status = db.getStatus(chat_id=media_dict.get('chat_id'), message_id=media_dict.get('message_id'))
        if db_status == 0:
            db.msg_insert_to_db(media_dict)
        elif db_status == 1:
            1
        elif db_status == 2:
            db.msg_update_to_db(media_dict)
        elif db_status == 3:
            db.msg_update_to_db(media_dict)
        # if media_dict.get('msg_from_chat_id') and media_dict.get('msg_from_message_id'): #是转发来的 则把转发源也记录到数据库中
        #     db_from_status = db.getStatus(chat_id=media_dict.get('msg_from_chat_id'), message_id=media_dict.get('msg_from_message_id'))
        #     media_dict['chat_id'] = media_dict.get('msg_from_chat_id')
        #     media_dict['message_id'] = media_dict.get('msg_from_message_id')
        #     if db_from_status == 0:
        #         db.msg_insert_to_db(media_dict)
        #     elif db_from_status == 1:
        #         1
        #     elif db_from_status == 2:
        #         db.msg_update_to_db(media_dict)
        #     elif db_from_status == 3:
        #         db.msg_update_to_db(media_dict)

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
    media_types: List[str],
    file_formats: dict,
    node: TaskNode,
):
    """
    Download media from Telegram.

    Each of the files to download are retried 3 times with a
    delay of 5 seconds each.

    Parameters
    ----------
    client: pyrogram.client.Client
        Client to interact with Telegram APIs.
    message: pyrogram.types.Message
        Message object retrieved from telegram.
    media_types: list
        List of strings of media types to be downloaded.
        Ex : `["audio", "photo"]`
        Supported formats:
            * audio
            * document
            * photo
            * video
            * voice
    file_formats: dict
        Dictionary containing the list of file_formats
        to be downloaded for `audio`, `document` & `video`
        media types.

    Returns
    -------
    int
        Current message id.
    """

    # pylint: disable = R0912

    file_name: str = ""
    ui_file_name: str = ""
    temp_file_name: str = ""
    task_start_time: float = time.time()
    media_dict = {}
    media_size = 0
    _media = None
    message = await fetch_message(client, message)
    message_id = message.id

    if message.empty or not (message.audio or message.video or message.photo or message.document):
        # logger.info(f"不是媒体类型 自动跳过[{message.chat.username}]{message.id}")
        return DownloadStatus.SkipDownload, None

    try:
        media_dict = _get_media_meta(message)
        message_id = media_dict.get('message_id')
        _media = media_dict
        file_name = media_dict.get('file_fullname')
        temp_file_name = media_dict.get('temp_file_fullname')
        file_format = media_dict.get('mime_type')
        media_size = media_dict.get('media_size')
        _type = media_dict.get('file_format')

        if media_dict.get('chat_username'):
            show_chat_username = media_dict.get('chat_username')
        else:
            show_chat_username = str(media_dict.get('chat_id'))

        # if show_chat_username == 'libraryforalls':
        #     print('debug')

        ui_file_name = file_name.split('/')[-1]

        if _media is None:
            return DownloadStatus.SkipDownload, None

        if app.hide_file_name:
            ui_file_name = f"****{os.path.splitext(file_name.split('/')[0])}"

        if _can_download(_type, file_formats, file_format):#判读是否是可下载的类型
            if get_exist_files(media_dict):  # 文件严格存在
                if os.path.getsize(file_name) == media_size:  # 判读文件是否同大小
                    # 严格存在 记录为已下载
                    node.download_status[message.id] = DownloadStatus.SuccessDownload
                    node.total_task += 1
                    # 写入数据库 记录已完成下载
                    media_dict['status'] = 1
                    insert_into_db(media_dict)
                    # 更新bot 展示状态
                    await report_bot_download_status(
                        node.bot,
                        node,
                        DownloadStatus.SuccessDownload,
                        media_size,
                    )
                    return DownloadStatus.SuccessDownload, None

            aka_files = get_exist_files_aka(media_dict)
            if aka_files:  # 广义文件存在
                for file_name_local in aka_files:
                    if os.path.getsize(file_name_local) != media_size:  # 广义本地文件不一样大
                        os.remove(file_name_local)  # 删掉
                    else:  # 广义本地文件一样大
                        if file_name_local != file_name:  # 文件名不一致则按新规则命名
                            move(file_name_local, file_name)

                # 上面循环处理完 应该最多只剩一个本地文件并且名字应该符合命名标准 大小也一样大
                if _is_exist(file_name):
                    # 广义存在 记录为已下载
                    node.download_status[message.id] = DownloadStatus.SuccessDownload
                    node.total_task += 1
                    # 写入数据库 记录已完成下载
                    media_dict['status'] = 1
                    insert_into_db(media_dict)
                    # 更新bot 展示状态
                    await report_bot_download_status(
                        node.bot,
                        node,
                        DownloadStatus.SuccessDownload,
                        media_size,
                    )
                    return DownloadStatus.SuccessDownload, None

            forward_files = get_exist_files_forward(media_dict)
            if forward_files:  # 不存在相同文件 但如果是转发msg 所以有可能在其他channel里已经下载了 判断一下
                for file_name_local in forward_files:
                    if os.path.getsize(file_name_local) == media_size:  # 源channel中文件一样大
                        # 源channel中文件存在 记录为已下载
                        node.download_status[message.id] = DownloadStatus.SuccessDownload
                        node.total_task += 1
                        # 写入数据库 记录已完成下载
                        media_dict['status'] = 1
                        insert_into_db(media_dict)
                        # 更新bot 展示状态
                        await report_bot_download_status(
                            node.bot,
                            node,
                            DownloadStatus.SuccessDownload,
                            media_size,
                        )
                        return DownloadStatus.SuccessDownload, None

            db_files = get_exist_files_db(media_dict)
            if db_files:  # 不存在相同文件 但有可能在其他channel里已经下载了 用db来判断相似度
                # 严格文件 广义文件 源channel中文件均不存在 看相似文件在db中是否可能存在
                for db_file in db_files:
                    if db_file.chat_username:
                        indisk = check_msg_files_indb(chat_username=db_file.chat_username,
                                                      message_id=db_file.message_id, chat_id=None)
                    else:
                        indisk = check_msg_files_indb(None, message_id=db_file.message_id, chat_id=db_file.chat_id)

                    if indisk:
                        # 有相似 且文件还存在
                        node.download_status[message.id] = DownloadStatus.SuccessDownload
                        node.total_task += 1
                        # 写入数据库 记录已完成下载
                        media_dict['status'] = 1
                        insert_into_db(media_dict)
                        # 更新bot 展示状态
                        await report_bot_download_status(
                            node.bot,
                            node,
                            DownloadStatus.SuccessDownload,
                            media_size,
                        )
                        return DownloadStatus.SuccessDownload, None

            pass
        else:
            return DownloadStatus.SkipDownload, None

        # break
    except Exception as e:
        logger.error(
            f"Message[{message_id}]: "
            f"{_t('could not be downloaded due to following exception')}:\n[{e}].",
            exc_info=True,
        )
        return DownloadStatus.FailedDownload, None


    for retry in range(3):
        # await update_download_status(0, media_size, message_id, ui_file_name,
        #                              task_start_time,
        #                              node, client)
        # logger.info(
        #     f"[{message.chat.username}]{message.id}{ui_file_name}开始下载进程...")
        try:

            temp_file_path = os.path.dirname(temp_file_name)
            chunk_dir = f"{temp_file_path}/{message_id}_chunk"
            logger.info(f"开始下载[{show_chat_username}]{message_id}{media_dict.get('filename')}...")

            if media_size < 1024 * 1024 * CHUNK_MIN: # 小于CHUNK_MIN M的就用单一文件下载
                chunk_count = 1
                chunk_filename = os.path.join(chunk_dir, "00000000")
                if os.path.exists(chunk_dir):
                    shutil.rmtree(chunk_dir)

                os.makedirs(chunk_dir, exist_ok=True)

                temp_download_path = await client.download_media(
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
                        except Exception as e:
                            logger.exception(f"{e}")
                            pass

            #判断一下是否下载完成
            if chunk_dir and os.path.exists(chunk_dir): #chunk_dir存在
                if check_download_finish(media_size, chunk_dir, ui_file_name, chunk_count): # 大小数量一致
                    try:
                        if merge_chunkfile(folder_path=chunk_dir, output_file=file_name, chunk_count=chunk_count,
                                        file_size=media_size, method='shutil'):
                            # await asyncio.sleep(0.5)
                            if _is_exist(file_name) and os.path.getsize(file_name) == media_size:
                                shutil.rmtree(chunk_dir)

                            media_dict['status'] = 1
                            insert_into_db(media_dict)

                            logger.success(f"完成下载[{show_chat_username}]{message_id}{media_dict.get('filename')}...")

                            return DownloadStatus.SuccessDownload, file_name
                    except Exception as e:
                        logger.exception(f"{e}")
                        pass
                else:
                    # await queue.put((message, node))
                    # logger.info(f"下载的文件大小出问题了...重新放入队列试试")
                    pass
            else:
                # await queue.put((message, node))
                # logger.info(f"下载的文件夹不见了...")
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
        #TODO 增加守护进程 当队列为空时 跟踪指定频道 有新消息则加入队列
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




async def download_chat_task(
    client: pyrogram.Client,
    chat_download_config: ChatDownloadConfig,
    node: TaskNode,
):
    try:
        #增加 chat_id 数字 和 chat_id username的判断

        if str(node.chat_id).isdigit():
            node.chat_id = 0 - node.chat_id - 1000000000000

        if node.chat_id == 'libraryforalls':
            print('debug')

        chat_download_config.node = node
        if isinstance(node.chat_id, int):
            logger.info(f"开始入列：[{0 - node.chat_id -1000000000000}]")
        else:
            logger.info(f"开始入列：[{node.chat_id}]")

        if chat_download_config.ids_to_retry:
            retry_ids = list(chat_download_config.ids_to_retry)
            logger.info(f"{_t('Downloading files failed during last run')}...")
            downloading_messages = []
            if len(retry_ids) > 200:# retry文件过多的话 就按200/批次处理文件
                batch_size = 200
                for i in range(0, len(retry_ids), batch_size):
                    batch_files = retry_ids[i:i + batch_size]
                    for fetch_retyr in range(3):
                        try:
                            downloading_messages: list = await client.get_messages(  # type: ignore
                                chat_id=node.chat_id, message_ids=batch_files
                            )
                            break
                        except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
                            await asyncio.sleep(wait_err.value)
                        except Exception as e:
                            logger.exception(f"{e}")
                    if downloading_messages and len(downloading_messages) > 0:
                        for message in downloading_messages:
                            if not (message.audio or message.video or message.photo or message.document):
                                continue
                            await add_download_task(message, node)

                    await asyncio.sleep(1)
            else:
                downloading_messages: list = await client.get_messages(  # type: ignore
                    chat_id=node.chat_id, message_ids=retry_ids
                )
                if downloading_messages and len(downloading_messages)>0:
                    for message in downloading_messages:
                        if not (message.audio or message.video or message.photo or message.document):
                            continue
                        await add_download_task(message, node)

        """Download all task"""
        messages_iter = get_chat_history_v2(
            client,
            node.chat_id,
            limit=node.limit,
            max_id=node.end_offset_id,
            offset_id=chat_download_config.last_read_message_id,
            reverse=True,
        )

        async for message in messages_iter:  # type: ignore
            meta_data = MetaData()

            if not(message.audio or message.video or message.photo or message.document):
                continue

            caption = message.caption
            if caption:
                caption = validate_title(caption)
                app.set_caption_name(node.chat_id, message.media_group_id, caption)
            else:
                caption = app.get_caption_name(node.chat_id, message.media_group_id)

            set_meta_data(meta_data, message, caption)

            #TODO 下面的貌似没用 考虑后面增加跳过指定文件的功能
            if app.need_skip_message(chat_download_config, message.id):
                continue

            if app.exec_filter(chat_download_config, meta_data):
                await add_download_task(message, node)
            else:
                node.download_status[message.id] = DownloadStatus.SkipDownload
                await upload_telegram_chat(
                    client,
                    node.upload_user,
                    app,
                    node,
                    message,
                    DownloadStatus.SkipDownload,
                )

        chat_download_config.need_check = True
        chat_download_config.total_task = node.total_task
        node.is_running = True
        if isinstance(node.chat_id, int):
            logger.info(f"入列完毕：[{0 - node.chat_id -1000000000000}]")
        else:
            logger.info(f"入列完毕：[{node.chat_id}]")
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
    logger.info(f"读取全部Chat完毕...")

def check_duplicate():
    return

def check_msg_files_indb(chat_username: str, message_id: int, chat_id: int = None):
    if chat_username is None and chat_id is None:
        return False
    if chat_username is None:
        msg = db.getMsg(None, message_id, 1, chat_id)
    else:
        msg = db.getMsg(chat_username, message_id, 1, None)

    if msg is None:
        return False


    msg_chat_id = msg.chat_id
    msg_message_id = msg.message_id
    msg_mime_type = msg.mime_type
    media_type = guess_media_type(msg_mime_type)
    file_name_pre= f"{[msg_message_id]}"
    if msg.chat_username and  msg.chat_username != '':
        file_save_path = os.path.join(app.get_file_save_path(media_type, validate_title(f"[{str(msg_chat_id)}]{msg.chat_username}"), ''),
                                  str(int(msg_message_id) // 100 * 100).zfill(6))
    else:
        file_save_path = os.path.join(
            app.get_file_save_path(media_type, validate_title(f"[{str(msg_chat_id)}]{str(msg_chat_id)}"), ''),
            str(int(msg_message_id) // 100 * 100).zfill(6))
    if not is_exist_files_with_prefix(file_save_path, file_name_pre):
        msg.status = 4  # 1完成 2 下载中 3 跳过 4 丢失
        msg.save()
        return False
    else:
        return True



async def run_until_all_task_finish():
    """Normal download"""
    while True:
        finish: bool = True
        for _, value in app.chat_download_config.items():
            if not value.need_check or value.total_task != value.finish_task:
                finish = False

        if (not app.bot_token and finish) or app.restart_program:
            break

        await asyncio.sleep(1)


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

def restart_program():
    print("程序重启...")
    p = sys.executable
    os.execl(p, p, *sys.argv)


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

        # if CHECK_DB:
        #     check_db_files()
        #
        # if CHECK_DUPLICATE:
        #     check_duplicate()

        app.loop.create_task(download_all_chat(client))

        for _ in range(app.max_download_task):
            task = app.loop.create_task(worker(client))
            tasks.append(task)

        if app.bot_token:
            app.loop.run_until_complete(
                start_download_bot(app, client, add_download_task, download_chat_task)
            )
        _exec_loop()
    except KeyboardInterrupt:
        logger.info(_t("KeyboardInterrupt"))
    except Exception as e:
        logger.exception("{}", e)
    finally:
        app.is_running = False
        try:
            if app.bot_token:
                app.loop.run_until_complete(stop_download_bot())
        except:
            pass
        try:
            app.loop.run_until_complete(stop_server(client))
        except:
            pass
        try:
            for task in tasks:
                task.cancel()
        except:
            pass
        finally:
            logger.info(_t("Stopped!"))
            logger.info(f"{_t('update config')}......")
            app.update_config()
            logger.success(
                 f"{_t('Updated last read message_id to config file')},"
                 f"{_t('total download')} {app.total_download_task}, "
                 f"{_t('total upload file')} "
                 f"{app.cloud_drive_config.total_upload_success_file_count}"
            )


if __name__ == "__main__":
    if _check_config():
        main()

