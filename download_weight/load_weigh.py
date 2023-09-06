import os

#from datasets import load_dataset
from huggingface_hub import snapshot_download, login
import shutil
import requests
import time

'''
    如果 huggingface hub 上面的仓库需要登录才可以获取的话，则取消下面的注释。
    运行之后，输入 huggingface 的 Access Tokens 即可。
'''
#login()

def make_dir(dataset_output_dir):
    if not os.path.exists(dataset_output_dir):
        os.makedirs(dataset_output_dir)
    dataset_download_cache = "{}/tmp_cache".format(dataset_output_dir)
    if not os.path.exists(dataset_download_cache):
        os.makedirs(dataset_download_cache)


def download_dataset(repo_id, dataset_output_dir, cache_dir, allow_patterns, max_workers):
    print('开始下载')
    snapshot_download(
            repo_id=repo_id,  
            cache_dir=cache_dir,
            local_dir=dataset_output_dir, 
            local_dir_use_symlinks=True,
            resume_download=True,
            max_workers=max_workers,
            allow_patterns=allow_patterns
    )
    print('下载完成')

def cancel_symlinks_and_remove_cache(dataset_output_dir, cache_dir):
    # 将 dataset_output_dir 软连接的cache文件 直接移动到 dataset_output_dir 
    # 删除 cache_dir
    cache_dirname = os.path.basename(cache_dir)
    for _dir, _, _filenames in os.walk(dataset_output_dir):
        if cache_dirname in _dir:
            continue
        for _filename in _filenames:
            filepath = os.path.join(_dir, _filename)
            try:
                link_filepath = os.readlink(filepath)
            except OSError as e:
                link_filepath = None
            if link_filepath:
                link_filepath = os.path.join(cache_dir, link_filepath.split('{}/'.format(cache_dirname))[-1])
                shutil.move(link_filepath, filepath)
                print('删除软连接， {} 覆盖 {}'.format(link_filepath, filepath))

    shutil.rmtree(cache_dir)
    print('缓存目录 {} 删除成功'.format(cache_dir))


if __name__ == '__main__':
    '''
        仓库名字
    '''
    repo_id = "sentence-transformers/gtr-t5-large"
    '''
        要下载的数据路径。
        * 
            根目录下的所有数据
        data/python/* 
            表示 下载 根目录下python文件夹里面的全部文件
        支持正则匹配
    '''
    allow_patterns = "*"
    '''
        数据保存目录
    '''
    dataset_output_dir = '{}/{}'.format(os.path.dirname(os.path.abspath(__file__)), repo_id)
    cache_dir = "{}/tmp_cache".format(dataset_output_dir)
    '''
        下载进程数
    '''
    max_workers=8

    # 创建对应文件夹
    make_dir(dataset_output_dir)

    while True:
        try:
            # 开始下载，支持断点下载
            download_dataset(repo_id, dataset_output_dir, cache_dir, allow_patterns, max_workers)
        except Exception as e:
            print(e)
            print('下载中断，等待重试')
            time.sleep(3)
        else:
            break


    # 将 dataset_output_dir 软连接的cache文件 直接移动到 dataset_output_dir 
    # 删除 cache_dir
    # cache_dir 删除之后，重新执行该脚本会重新下载数据。
    cancel_symlinks_and_remove_cache(dataset_output_dir, cache_dir)


    '''
        huggingface 数据读取可以参考 https://huggingface.co/docs/datasets/loading#local-and-remote-files

        CSV:
            from datasets import load_dataset
            dataset = load_dataset("csv", data_files="my_file.csv")
        
        JSON:
            from datasets import load_dataset
            dataset = load_dataset("json", data_files="my_file.json")
        
        Parquet:
            from datasets import load_dataset
            dataset = load_dataset("parquet", data_files={'train': 'train.parquet', 'test': 'test.parquet'})     
    '''


   

     




