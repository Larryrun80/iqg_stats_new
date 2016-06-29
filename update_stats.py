import os
import subprocess
import time

import arrow
import paramiko
import yaml


BASE_DIR = os.path.abspath(os.path.dirname(__file__))
STATS_DIR = '{base}/{stats}'.format(base=BASE_DIR, stats='stats/items/')
REMOTE_CONFIG_FILE = '{base}/{config}'.format(
    base=BASE_DIR, config='instance/update_settings.yaml')
BACKUPS_KEPT = 5
BACKUP_FORMAT = 'YYYY-MM-DD_HH:mm:ss'


def print_output(b_info_list):
    for info in b_info_list:
        if info.endswith(b'\n'):
            info = info[:-1]
        print(info.decode())


def get_remote_settings():
    must_settings = ('ssh_settings', 'remote_dir')
    if not os.path.exists(REMOTE_CONFIG_FILE):
        raise RuntimeError('remote config file missing...')

    with open(REMOTE_CONFIG_FILE) as f:
        raw_confs = yaml.load(f)

    for s in must_settings:
        if s not in raw_confs.keys():
            raise RuntimeError('{} not found in config'.format(s))

    return raw_confs


def get_ssh_client(remote_conf):
    client = paramiko.SSHClient()
    client._policy = paramiko.WarningPolicy()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(**remote_conf)
    return client

if __name__ == '__main__':
    # 获取远程配置
    configs = get_remote_settings()
    client = get_ssh_client(configs['ssh_settings'])
    r_dir = configs['remote_dir']
    b_dir = configs['backup_dir']

    # 创建备份
    # 确定备份文件夹名
    stdin, stdout, stderr = client.exec_command(
        'ls {}'.format(r_dir))
    err = stderr.readlines()
    if not err:
        result = [fn[:-1] for fn in stdout.readlines()]
        new_dir = arrow.now('Asia/Shanghai').format(BACKUP_FORMAT)
        while True:
            if new_dir in result:
                new_dir = arrow.now('Asia/Shanghai')\
                               .format(BACKUP_FORMAT)
            else:
                break
    else:
        raise RuntimeError('remote error: {}'.format(err))

    # 如果没有备份文件夹，创建一个
    stdin, stdout, stderr = client.exec_command(
        'ls {}'.format(b_dir))
    if stderr.readlines:
        stdin, stdout, stderr = client.exec_command(
            'mkdir {}'.format(b_dir))

    # 删除最大留存之前的备份
    stdin, stdout, stderr = client.exec_command(
        'ls {}'.format(b_dir))
    err = stderr.readlines()
    if not err:
        backups = []
        rb_dirs = stdout.readlines()
        if len(rb_dirs) > BACKUPS_KEPT:
            for rb_dir in rb_dirs:
                dirname = rb_dir[:-1]
                backups.append(
                    arrow.get(dirname, BACKUP_FORMAT).timestamp)
            backups = sorted(backups)
            to_clear = backups[:len(backups) - BACKUPS_KEPT]

            cmd = 'rm -r'
            for c_dir in to_clear:
                cmd += ' {}{}'.format(b_dir,
                                      arrow.get(c_dir).format(BACKUP_FORMAT))
            stdin, stdout, stderr = client.exec_command(cmd)
            err = stderr.readlines()
            if err:
                raise RuntimeError('remote error: {}'.format(err))
        else:
            print('do not need to clear backups')
    else:
        raise RuntimeError('remote error: {}'.format(err))

    # 备份原配置
    backup_cmds = (
        'mkdir {}{}'.format(b_dir, new_dir),
        'mv {}* {}{}/'.format(r_dir, b_dir, new_dir)
    )

    for cmd in backup_cmds:
        stdin, stdout, stderr = client.exec_command(cmd)
        err = stderr.readlines()
        if err:
            raise RuntimeError('remote error: {}'.format(err))
            break

    # 上传配置文件
    upload_cmd = 'scp -r {local_path} {username}@{remote_addr}:{remote_path}'\
                 ''.format(local_path=STATS_DIR + '*',
                           username=configs['ssh_settings']['username'],
                           remote_addr=configs['ssh_settings']['hostname'],
                           remote_path=configs['remote_dir'])
    p = subprocess.Popen(upload_cmd,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         shell=True)

    while p.poll() is None:  # 检查子进程是否已经结束
        time.sleep(1)

    if p.returncode == 0:
        print_output(p.stdout.readlines())
    else:
        print('error found')
        print_output(p.stderr.readlines())
