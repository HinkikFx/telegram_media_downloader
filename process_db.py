import asyncio
import json
import os
import re
import sys
import threading
from pathlib import Path
from time import sleep
from tqdm import tqdm
from module import sqlmodel
from playhouse.shortcuts import model_to_dict
from utils.format import validate_title, guess_media_type, move_file, is_exist_files_with_prefix, process_string

db = sqlmodel.Downloaded()

similar_set = 0.90
sizerange_min = 0.005

if sys.platform.startswith('linux'):
    root_dir = "/mnt/onedrive/"
else:
    root_dir = "/Users/wuyun/Downloads/mnt/alist-local"


def has_japanese_or_korean_chars(a: str) -> bool:
    # Match any character in the Unicode block for Japanese or Korean characters
    result = bool(re.search(r'[\u3040-\u30ff\uac00-\ud7a3]', a.replace('の', '的')) or re.search(
        r'(日南|真琴|天知遥|秋水|养猪妹|利香|幽灵妹|柚木)', a))
    return result

def get_files_in_dir(dir_path):
    file_list = []
    for root, dirs, files in os.walk(dir_path):
        for file_name in files:
           file_list.append(os.path.join(root, file_name))
    return file_list


def get_msg_info_from_file(file_dir: str, file_name: str):
    chat_id = int(re.search(r'\d+', os.path.basename(os.path.dirname(file_dir))).group())
    msg_id = int(re.search(r'\d+', file_name).group())
    return chat_id,msg_id

def worker_file2db(file_paths):
    for path in file_paths:
        update_db_from_filedir(path)

def get_subfolders(a):
    try:
        subfolders = [f for f in os.listdir(a) if os.path.isdir(os.path.join(a, f))]
        return subfolders
    except OSError:
        return "无法访问目录或目录不存在。"
def file2db_main():
    asmr_dirs = db.get_subfolders(os.join(root_dir, 'docu/telegram/Asmr'))
    book_dirs = db.get_subfolders(os.join(root_dir, 'docu/telegram/Books'))
    all_dirs = asmr_dirs + book_dirs
    try:
        num_threads = 5
        chunk_size = len(all_dirs) // num_threads
        threads = []
        for i in range(num_threads):
            # for msg in tqdm(all_msg):
            start = i * chunk_size
            end = (i + 1) * chunk_size if i < num_threads - 1 else len(all_dirs)
            thread = threading.Thread(target=worker_file2db, args=(all_dirs[start:end],))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    except KeyboardInterrupt:
        print("KeyboardInterrupt")
    except Exception as e:
        print(e)

def update_db_from_filedir(dir_path):
    for root, dirs, files in tqdm(os.walk(dir_path)):
        for file_name in files:
            chat_id,msg_id = get_msg_info_from_file(root, file_name)

            # 处理同id多个文件的历史问题
            files = find_files_with_prefix(root, file_name)
            for file in files:
                if file == file_name:
                    pass
                else:
                    del_file(file)

            # 处理文件和数据库对应有误的问题
            msg = db.getStatus(chat_id, chat_id)
            if msg.ststus == 0: #文件存在 但数据库不存在 写一个2标志位 下次运行可以补写信息入库
                msg_dict = {
                    'chat_id': chat_id,
                    'message_id': msg_id,
                    'filename': file_name,
                    'caption': '',
                    'title': '',
                    'mime_type': '',
                    'media_size': '',
                    'media_duration': '',
                    'media_addtime': '',
                    'chat_username': '',
                    'chat_title': '',
                    'addtime': '',
                    'msg_type': '',
                    'msg_link': '',
                    'status': 2
                }
                db.msg_insert_to_db(msg_dict)
                print(f"[{chat_id}]msg_id 加入待下载队列！")
            elif msg.ststus == 1: #文件存在 数据库也存在
                pass
            elif msg.ststus == 2 or msg.ststus == 3 or msg.ststus == 4:  # 文件存在 但数据库记录错误
                msg.status = 1
                db.msg_update_to_db(model_to_dict(msg))
                print(f"{msg.filename} 文件在 修改数据库！")

def del_file(file_path):
    file_path = Path(file_path)
    for i in range(3):
        try:
            if file_path.exists():
                file_path.unlink()
            sleep(1)
            if not file_path.exists():
                return True
        except Exception as e:
            print (e)
            pass



def find_files_with_prefix(folder_path, prefix):
    file_list = []
    if os.path.exists(folder_path):
        with os.scandir(folder_path) as entries:
            for entry in entries:
                if entry.is_file() and entry.name.startswith(prefix):
                    file_list.append(entry.path)
        return file_list
    else:
        return []

def save_list_to_json(list, json_file):
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(list, f, indent=4, ensure_ascii=False)

def get_aka_msg(msg_dict):
    file_ext = os.path.splitext(msg_dict.get('filename'))[-1]
    file_type = guess_media_type(file_ext)
    if file_type == 'video':
        subdir = os.path.join(root_dir,'cav1/telegram')
    elif file_type == 'audio':
        subdir = os.path.join(root_dir,'docu/telegram/Asmr')
    elif file_type == 'photo':
        subdir = os.path.join(root_dir,'images2/telegram')
    elif file_type == 'document':
        subdir = os.path.join(root_dir,'docu/telegram/Books')
    else:
        subdir = os.path.join(root_dir,'upload/telegram')

    if not msg_dict.get('chat_username'):
        file_dir = os.path.join(subdir,
                                validate_title(f"[{str(msg_dict.get('chat_id'))}]{msg_dict.get('chat_id')}"),
                                str(int(msg_dict.get('message_id')) // 100 * 100).zfill(6))
    else:
        file_dir = os.path.join(subdir,
                                validate_title(f"[{str(msg_dict.get('chat_id'))}]{msg_dict.get('chat_username')}"),
                                str(int(msg_dict.get('message_id')) // 100 * 100).zfill(6))

    folder_path = file_dir
    prefix = f"[{msg_dict.get('message_id')}]"

    return folder_path, prefix


def get_save_dir(file_ext):
    file_type = guess_media_type(file_ext)
    if file_type == 'video':
        save_dir = os.path.join(root_dir,'cav1/telegram')
    elif file_type == 'audio':
        save_dir = os.path.join(root_dir,'docu/telegram/Asmr')
    elif file_type == 'photo':
        save_dir = os.path.join(root_dir,'images2/telegram')
    elif file_type == 'document':
        save_dir = os.path.join(root_dir,'docu/telegram/Books')
    else:
        save_dir = os.path.join(root_dir,'upload/telegram')

    return save_dir

def get_aka_file_dir(msg_dict):
    file_ext = os.path.splitext(msg_dict.get('filename'))[-1]
    subdir = get_save_dir(file_ext)

    if not msg_dict.get('chat_username'):
        file_dir = os.path.join(subdir,
                                validate_title(f"[{str(msg_dict.get('chat_id'))}]{msg_dict.get('chat_id')}"),
                                str(int(msg_dict.get('message_id')) // 100 * 100).zfill(6))
    else:
        file_dir = os.path.join(subdir,
                                validate_title(f"[{str(msg_dict.get('chat_id'))}]{msg_dict.get('chat_username')}"),
                                str(int(msg_dict.get('message_id')) // 100 * 100).zfill(6))

    return file_dir

def get_aka_file_path(msg_dict):
    file_dir = get_aka_file_dir(msg_dict)
    matched_file = os.path.join(file_dir, f"{msg_dict.get('filename')}")

    return matched_file

def load_last_id_json(json_file):
    if os.path.exists(json_file):
        # Read existing data from the file
        with open(json_file, 'r', encoding='utf-8') as f:
            last_id = json.load(f)
        return last_id

def save_last_id_json(json_file, last_id):
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(last_id, f, indent=4, ensure_ascii=False)

def update_json_by_dict(json_file, data_append):
    if os.path.exists(json_file):
        # Read existing data from the file
        with open(json_file, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
    else:
        # Create an empty dictionary if the file doesn't exist
        existing_data = []
    existing_data.append(data_append)
    # Write the updated data back to the file
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, indent=4, ensure_ascii=False)

def update_status():
    last_id = load_last_id_json('./temp/last_update_id.json')
    if not last_id or last_id == '':
        last_id = 0
    all_msg = db.get_all_message_from(last_id)
    file2del = []
    try:
        for msg in tqdm(all_msg):  # 遍历文件夹
            last_id = msg.id
            folder_path, prefix = get_aka_msg(model_to_dict(msg))
            files_disk = find_files_with_prefix(folder_path, prefix)
            if msg.status == 1:
                if files_disk and len(files_disk) >= 1: #文件存在
                    pass
                else:
                    msg.status = 2
                    db.msg_update_to_db(model_to_dict(msg))
                    print(f"{msg.filename} 文件丢了 重新下载吧！")
            elif msg.status == 2:
                if files_disk and len(files_disk) >= 1: #文件存在
                    msg.status = 1
                    db.msg_update_to_db(model_to_dict(msg))
                    print(f"{msg.filename} 文件下完了 不要再下载了！")
                else:
                    pass
            elif msg.status == 3:
                if files_disk and len(files_disk) >= 1: #文件存咋
                    msg.status = 1
                    db.msg_update_to_db(model_to_dict(msg))
                    file2del.append([msg.chat_id, msg.id])
                    print(f"{msg.filename} 文件下完了 但是没让下啊 需要删除掉！")
                else:
                    pass
            elif msg.status == 4:
                if files_disk and len(files_disk) >= 1: #文件存在
                    msg.status = 1
                    db.msg_update_to_db(model_to_dict(msg))
                    file2del.append([msg.chat_id, msg.message_id])
                    print(f"{msg.filename} 文件下完了 但是没让下啊 需要删除掉！")
                else:
                    pass

    except KeyboardInterrupt:
        print("KeyboardInterrupt")
    except Exception as e:
        print(e)
    finally:
        print(f"last_id = {last_id}")
        save_last_id_json('./temp/last_update_id.json', last_id)
        save_list_to_json('./temp/2del_ids.json', file2del)

def worker(msg_list):
    for msg in tqdm(msg_list):
        check_msg(msg)


def check_msg(msg:dict):
    if db.getStatusById(msg.id) != 1:
        print(f"{msg.filename} is dealed. passed......")
        return
    folder_path, prefix = get_aka_msg(model_to_dict(msg))
    file_paths = find_files_with_prefix(folder_path, prefix)
    if not file_paths or len(file_paths) == 0:
        # 文件丢了 重新下载吧
        msg.status = 2
        db.msg_update_to_db(model_to_dict(msg))
        print(f"{msg.filename} 文件丢了 重新下载吧！")
        return

    similar_files_db = db.get_similar_files(model_to_dict(msg), similar_set, sizerange_min)  # 找到他的相似文件
    if len(similar_files_db) >= 20:
        print('debug')
    for similar_file_db in similar_files_db:
        folder_path, prefix = get_aka_msg(model_to_dict(similar_file_db))
        similar_files_disk = find_files_with_prefix(folder_path, prefix)
        for similar_file_disk in similar_files_disk:
            from_path = os.path.dirname(similar_file_disk)
            save_filename = os.path.basename(similar_file_disk)
            file_ext = os.path.splitext(save_filename)[-1]
            subdir = get_save_dir(file_ext)
            to_path = os.path.join(subdir, '2del', os.path.relpath(from_path, subdir))
            move_file(from_path, to_path, save_filename)

            similar_file_db.status = 4
            db.msg_update_to_db(model_to_dict(similar_file_db))
            print(
                f"\n[{msg.chat_id}]{msg.title}=VS=[{similar_file_db.chat_id}]{similar_file_db.title} has been moved to 2del dir......")

def del_dulp():
    last_id = load_last_id_json('./temp/last_id.json')
    if not last_id or last_id == '':
        last_id = 0
    all_msg = db.get_all_finished_message_from(last_id)
    try:
        num_threads = 5
        chunk_size = len(all_msg) // num_threads
        threads = []
        for i in range(num_threads):
        # for msg in tqdm(all_msg):
            start = i * chunk_size
            end = (i + 1) * chunk_size if i < num_threads - 1 else len(all_msg)
            thread = threading.Thread(target=worker, args=(all_msg[start:end],))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    except KeyboardInterrupt:
        print("KeyboardInterrupt")
    except Exception as e:
        print(e)
    finally:
        print(f"last_id = {last_id}")
        save_last_id_json('./temp/last_id.json', last_id)

def main():
    # subdir_asmr = os.path.join(root_dir, 'docu/telegram/Asmr')
    # asmr_ids = get_files_id_in_dir(subdir_asmr)
    # save_list_to_json(asmr_ids, './temp/asmr_ids.json')
    # subdir_book = os.path.join(root_dir, 'docu/telegram/Books')
    # book_ids = get_files_id_in_dir(subdir_book)
    # save_list_to_json(book_ids, './temp/book_ids.json')
    # print("\nupdate_status starting")
    # update_status()
    # print("\nupdate_status ended")
    print("\ndel_dulp starting")
    del_dulp()
    print("\ndel_dulp ended")


if __name__ == "__main__":
    main()