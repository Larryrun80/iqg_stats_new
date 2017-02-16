import os
import shlex
import subprocess

import paramiko


class RemoteLogReader():
    """ class for deal remote log data via paramiko
    """
    def __init__(self, log_dir, remote_conf):
        self.log_dir = log_dir

        if not remote_conf:
            raise RuntimeError('remote config missing')
        self.client = paramiko.SSHClient()
        self.client._policy = paramiko.WarningPolicy()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(**remote_conf)

    def get_log_files(self, filename):
        ''' return all files in remote dir
        '''
        result_files = []
        stdin, stdout, stderr =\
            self.client.exec_command(
                'ls data/logs/iqg-api/*/currentAppLogs/{}'.format(filename))

        while True:
            line = stdout.readline()[:-1]
            # end of results
            if line == '':
                break

            result_files.append(line)
        return result_files

    def get_file_by_date(self, file_date):
        ''' return specific date's log files
        '''
        file_name = 'access-{}.log'.format(file_date)

        if self.log_type == 'remote':
            return self.get_remote_files(file_name)
        else:
            return self.get_local_files(file_name)

    def execute_cmd(self, cmd):
        if self.log_type == 'local':
            cmds = cmd.split('|')
            for i, command in enumerate(cmds):
                args = shlex.split(command)
                if i == 0:
                    popen = subprocess.Popen(args,
                                             stdout=subprocess.PIPE)
                else:
                    popen = subprocess.Popen(args,
                                             stdin=popen.stdout,
                                             stdout=subprocess.PIPE)
            stdout = popen.stdout
            result = []
            while True:
                line = stdout.readline().decode()
                if line == '':
                    break
                result.append(line)

        elif self.log_type == 'remote':
            stdout = self.client.exec_command(cmd)[1]
            result = stdout.readlines()

        return result

    def ssh_close(self):
        if hasattr(self, 'client'):
            self.client.close()
        else:
            print('local mission completed')
