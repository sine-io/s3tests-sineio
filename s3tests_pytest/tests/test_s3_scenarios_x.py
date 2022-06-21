
import pytest

from s3tests_pytest.tests import TestBaseClass


class TestAnotherScenarios(TestBaseClass):

    def setup_class(cls) -> None:
        cls.small_file_nums = 20
        # true = 1
        # interval_time = 30
        # # 测试时长节点
        # wait_time = 300
        # # 小对象测试量
        # small_file_nums = 20
        # # 大对象测试量
        # large_file_nums = 10
        # # Bucket_Name = 'test'
        # StorageClass_Type = 'ARCHIVE'
        # # 大对象上传路径
        # large_file_url = "/root/10m.file"
        # small_file_url = "/root/32k.file"
        # big_file_url = "/root/50m.file"
        # large_name = 'large_file_num'
        # small_name = 'small_file_num'
        # big_name = 'big_file_num'
        # day = 1

    def test_para11(self, s3cfg_global_unique):
        res = self.exec_cmd(
            host=s3cfg_global_unique.default_host,
            user=s3cfg_global_unique.ssh_user,
            passwd=s3cfg_global_unique.ssh_passwd,
            port=s3cfg_global_unique.ssh_port,
            command="radosgw-admin user list"
        )
        if not res.stderr:
            print(res.stdout)

    # def test_archive_to_glacier1(self, s3cfg_global_unique):
    #     # 批量转储
    #     n = 1
    #     Bucket_Name = get_new_bucket()
    #     client = get_client()
    #     while n <= (small_file_nums + large_file_nums + 1):
    #         if n <= small_file_nums:
    #             key = small_name + '_' + str(n)
    #             body = open(small_file_url, 'rb').read() + str(n).encode()
    #         elif n <= (small_file_nums + large_file_nums):
    #             key = large_name + '_' + str(n)
    #             body = open(large_file_url, 'rb').read() + str(n).encode()
    #         else:
    #             key = big_name + '_' + str(n)
    #             body = open(big_file_url, 'rb').read() + str(n).encode()
    #         client.put_object(Bucket=Bucket_Name, Key=key, Body=body, StorageClass=StorageClass_Type)
    #         n += 1
    #     time.sleep(interval_time)
    #     start = time.time()
    #     # 确保都已转储成功
    #     n = 1
    #     while n <= (small_file_nums + large_file_nums + 1):
    #         while true:
    #             if n <= small_file_nums:
    #                 key = small_name + '_' + str(n)
    #             elif n <= (small_file_nums + large_file_nums):
    #                 key = large_name + '_' + str(n)
    #             else:
    #                 key = big_name + '_' + str(n)
    #             resp = client.head_object(Bucket=Bucket_Name, Key=key)
    #             end = time.time()
    #             run_time = end - start
    #             try:
    #                 val = resp['ResponseMetadata']['HTTPHeaders']['x-amz-restore']
    #             except:
    #                 if run_time > wait_time:
    #                     raise AssertionError(key + "fail")
    #             else:
    #                 if run_time < wait_time:
    #                     if val == '':
    #                         break
    #                 else:
    #                     raise AssertionError("archive fail")
    #         if n == (small_file_nums + large_file_nums + 1):
    #             break
    #         else:
    #             n += 1
    #     time.sleep(interval_time)
    #     # 设置生命周期
    #     client.put_bucket_lifecycle(
    #         Bucket=Bucket_Name,
    #         LifecycleConfiguration={
    #             'Rules': [
    #                 {
    #                     'Status': 'Enabled',
    #                     'Prefix': '',
    #                     'Transition': {
    #                         'Date': '2021-08-26 00:00:00',
    #                         'StorageClass': 'GLACIER'
    #                     },
    #                 }], })
    #     time.sleep(interval_time)
    #     # 比较
    #     start = time.time()
    #     n = 1
    #     while n <= (small_file_nums + large_file_nums + 1):
    #         end = time.time()
    #         run_time = end - start
    #         if n <= small_file_nums:
    #             key = small_name + '_' + str(n)
    #         elif n <= (small_file_nums + large_file_nums):
    #             key = large_name + '_' + str(n)
    #         else:
    #             key = big_name + '_' + str(n)
    #         resp = client.head_object(Bucket=Bucket_Name, Key=key)
    #         val = resp['ResponseMetadata']['HTTPHeaders']['x-amz-restore']
    #         if val == '':
    #             os.system('radosgw-admin bucket list --bucket=' + Bucket_Name + ' >/root/stat')
    #             content = open('/root/stat', 'rb').read()
    #             name = re.compile('"name": "(.*)"')
    #             merge_flag = re.compile('"merge_flags": (\d+),')
    #             size = re.compile('"size": (\d+),')
    #             storage_class = re.compile('"storage_class": "(\w+)"')
    #             names = name.findall(content.decode())
    #             nums = 0
    #             for name_num in names:
    #                 if name_num == key:
    #                     break
    #                 nums += 1
    #             storage_class_num = storage_class.findall(content.decode())[nums]
    #             merge_flag_num = merge_flag.findall(content.decode())[nums]
    #         if (storage_class_num == 'GLACIER') and (merge_flag_num == '64'):
    #             n += 1
    #         else:
    #             if run_time >= wait_time:
    #                 raise AssertionError("archive fail")

    # def test_archive_to_glacier2():
    #     # 批量转储
    #     n = 1
    #     Bucket_Name = get_new_bucket()
    #     client = get_client()
    #     while n <= (small_file_nums + large_file_nums + 1):
    #         if n <= small_file_nums:
    #             key = small_name + '_' + str(n)
    #             body = open(small_file_url, 'rb').read() + str(n).encode()
    #         elif n <= (small_file_nums + large_file_nums):
    #             key = large_name + '_' + str(n)
    #             body = open(large_file_url, 'rb').read() + str(n).encode()
    #         else:
    #             key = big_name + '_' + str(n)
    #             body = open(big_file_url, 'rb').read() + str(n).encode()
    #         client.put_object(Bucket=Bucket_Name, Key=key, Body=body, StorageClass=StorageClass_Type)
    #         n += 1
    #     # 设置生命周期
    #     client.put_bucket_lifecycle(
    #         Bucket=Bucket_Name,
    #         LifecycleConfiguration={
    #             'Rules': [
    #                 {
    #                     'Status': 'Enabled',
    #                     'Prefix': '',
    #                     'Transition': {
    #                         'Date': '2021-08-26 00:00:00',
    #                         'StorageClass': 'GLACIER'
    #                     },
    #                 }], })
    #     time.sleep(interval_time)
    #     # 比较
    #     start = time.time()
    #     n = 1
    #     while n <= (small_file_nums + large_file_nums + 1):
    #         end = time.time()
    #         run_time = end - start
    #         if n <= small_file_nums:
    #             key = small_name + '_' + str(n)
    #         elif n <= (small_file_nums + large_file_nums):
    #             key = large_name + '_' + str(n)
    #         else:
    #             key = big_name + '_' + str(n)
    #         resp = client.head_object(Bucket=Bucket_Name, Key=key)
    #         val = resp['ResponseMetadata']['HTTPHeaders']['x-amz-restore']
    #         if val == '':
    #             os.system('radosgw-admin bucket list --bucket=' + Bucket_Name + ' >/root/stat')
    #             content = open('/root/stat', 'rb').read()
    #             name = re.compile('"name": "(.*)"')
    #             merge_flag = re.compile('"merge_flags": (\d+),')
    #             size = re.compile('"size": (\d+),')
    #             storage_class = re.compile('"storage_class": "(\w+)"')
    #             names = name.findall(content.decode())
    #             nums = 0
    #             for name_num in names:
    #                 if name_num == key:
    #                     break
    #                 nums += 1
    #             storage_class_num = storage_class.findall(content.decode())[nums]
    #             merge_flag_num = merge_flag.findall(content.decode())[nums]
    #             if (storage_class_num == 'GLACIER') and (merge_flag_num == '64'):
    #                 n += 1
    #         else:
    #             if run_time >= wait_time:
    #                 raise AssertionError("archive fail")
    #
    # def test_small_file_archive():
    #     # 批量转储
    #     n = 1
    #     Bucket_Name = get_new_bucket()
    #     client = get_client()
    #     while n <= small_file_nums:
    #         key = small_name + '_' + str(n)
    #         body = 'Number ' + str(n)
    #         # body = open(small_file_url,'rb').read()+str(n)
    #         client.put_object(Bucket=Bucket_Name, Key=key, Body=body, StorageClass=StorageClass_Type)
    #         n += 1
    #     time.sleep(interval_time)
    #     start = time.time()
    #     # 确保都已转储成功
    #     n = 1
    #     while n <= small_file_nums:
    #         while true:
    #             key = small_name + '_' + str(n)
    #             resp = client.head_object(Bucket=Bucket_Name, Key=key)
    #             end = time.time()
    #             run_time = end - start
    #             try:
    #                 val = resp['ResponseMetadata']['HTTPHeaders']['x-amz-restore']
    #             except:
    #                 if run_time > wait_time:
    #                     raise AssertionError(key + "fail")
    #             else:
    #                 if run_time < wait_time:
    #                     if val == '':
    #                         break
    #                 else:
    #                     raise AssertionError("archive fail")
    #         if n == small_file_nums:
    #             break
    #         else:
    #             n += 1
    #     # 批量取回操作
    #     time.sleep(interval_time)
    #     n = 1
    #     while n <= small_file_nums:
    #         key = small_name + '_' + str(n)
    #         try:
    #             client.restore_object(Bucket=Bucket_Name, Key=key, RestoreRequest={'Days': 20})
    #         except:
    #             if run_time > wait_time:
    #                 raise AssertionError(key + "fail")
    #         else:
    #             if n == small_file_nums:
    #                 break
    #             else:
    #                 n += 1
    #     n = 1
    #     start = time.time()
    #     while n <= small_file_nums:
    #         end = time.time()
    #         run_time = end - start
    #         key = small_name + '_' + str(n)
    #         resp = client.head_object(Bucket=Bucket_Name, Key=key)
    #         val = resp['ResponseMetadata']['HTTPHeaders']['x-amz-restore']
    #         if val[0:23] == 'ongoing-request="false"':
    #             try:
    #                 cont = client.get_object(Bucket=Bucket_Name, Key=key)
    #             except:
    #                 raise AssertionError("restore fail")
    #             else:
    #                 content = cont['Body'].read()
    #                 eq(content, ('Number ' + str(n)).encode())
    #                 if n == small_file_nums:
    #                     break
    #                 else:
    #                     n += 1
    #         else:
    #             if run_time < wait_time:
    #                 client.restore_object(Bucket=Bucket_Name, Key=key, RestoreRequest={'Days': 20})
    #             else:
    #                 raise AssertionError("restore fail")
    #     # 验证批量删除
    #     n = 1
    #     start = time.time()
    #     while n <= small_file_nums:
    #         end = time.time()
    #         run_time = end - start
    #         key = small_name + '_' + str(n)
    #         try:
    #             client.delete_object(Bucket=Bucket_Name, Key=key)
    #         except:
    #             raise AssertionError("delete fail")
    #         else:
    #             try:
    #                 client.restore_object(Bucket=Bucket_Name, Key=key, RestoreRequest={'Days': 5})
    #             except:
    #                 if n == small_file_nums / 2:
    #                     break
    #                 else:
    #                     n += 1
    #             else:
    #                 raise AssertionError("delete fail")
    #     # 验证碎片整理
    #     n += 1
    #     start = time.time()
    #     start_n = n
    #     while n <= small_file_nums:
    #         end = time.time()
    #         run_time = end - start
    #         key = small_name + '_' + str(n)
    #         try:
    #             client.restore_object(Bucket=Bucket_Name, Key=key, RestoreRequest={'Days': 20})
    #         except:
    #             if run_time > wait_time:
    #                 raise AssertionError(key + "fail")
    #         else:
    #             if n == small_file_nums:
    #                 break
    #             else:
    #                 n += 1
    #     n = start_n
    #     start = time.time()
    #     while n <= small_file_nums:
    #         end = time.time()
    #         run_time = end - start
    #         key = small_name + '_' + str(n)
    #         resp = client.head_object(Bucket=Bucket_Name, Key=key)
    #         val = resp['ResponseMetadata']['HTTPHeaders']['x-amz-restore']
    #         if val[0:23] == 'ongoing-request="false"':
    #             try:
    #                 cont = client.get_object(Bucket=Bucket_Name, Key=key)
    #             except:
    #                 raise AssertionError("defrag fail")
    #             else:
    #                 content = cont['Body'].read()
    #                 eq(content, ('Number ' + str(n)).encode())
    #                 if n == small_file_nums:
    #                     break
    #                 else:
    #                     n += 1
    #         else:
    #             if run_time < wait_time:
    #                 client.restore_object(Bucket=Bucket_Name, Key=key, RestoreRequest={'Days': 20})
    #             else:
    #                 raise AssertionError("merge_archive fail")
    #     # 删除剩余对象
    #     n = start_n
    #     start = time.time()
    #     while n <= small_file_nums:
    #         end = time.time()
    #         run_time = end - start
    #         key = small_name + '_' + str(n)
    #         try:
    #             client.delete_object(Bucket=Bucket_Name, Key=key)
    #         except:
    #             raise AssertionError("delete fail")
    #         else:
    #             try:
    #                 client.restore_object(Bucket=Bucket_Name, Key=key, RestoreRequest={'Days': 5})
    #             except:
    #                 if n == small_file_nums:
    #                     break
    #                 else:
    #                     n += 1
    #             else:
    #                 raise AssertionError("delete fail")
    #
    # def test_large_file_archive():
    #     # 批量转储
    #     Bucket_Name = get_new_bucket()
    #     client = get_client()
    #     n = 1
    #     while n <= large_file_nums:
    #         key = large_name + '_' + str(n)
    #         body = open(large_file_url, 'rb').read() + str(n).encode()
    #         client.put_object(Bucket=Bucket_Name, Key=key, Body=body, StorageClass=StorageClass_Type)
    #         n += 1
    #     time.sleep(interval_time)
    #     start = time.time()
    #     # 确保都已转储成功
    #     n = 1
    #     while n <= large_file_nums:
    #         while true:
    #             key = large_name + '_' + str(n)
    #             resp = client.head_object(Bucket=Bucket_Name, Key=key)
    #             end = time.time()
    #             run_time = end - start
    #             try:
    #                 val = resp['ResponseMetadata']['HTTPHeaders']['x-amz-restore']
    #             except:
    #                 if run_time > wait_time:
    #                     raise AssertionError(key + " archive fail")
    #             else:
    #                 if run_time < wait_time:
    #                     if val == '':
    #                         break
    #                 else:
    #                     raise AssertionError("archive fail")
    #         if n == large_file_nums:
    #             break
    #         else:
    #             n += 1
    #     # 批量取回操作
    #     time.sleep(interval_time)
    #     n = 1
    #     while n <= large_file_nums:
    #         key = large_name + '_' + str(n)
    #         try:
    #             client.restore_object(Bucket=Bucket_Name, Key=key, RestoreRequest={'Days': 20})
    #         except:
    #             if run_time > wait_time:
    #                 raise AssertionError(key + " restore fail")
    #         else:
    #             if n == large_file_nums:
    #                 break
    #             else:
    #                 n += 1
    #     n = 1
    #     while n <= large_file_nums:
    #         end = time.time()
    #         run_time = end - start
    #         key = large_name + '_' + str(n)
    #         resp = client.head_object(Bucket=Bucket_Name, Key=key)
    #         val = resp['ResponseMetadata']['HTTPHeaders']['x-amz-restore']
    #         if val[0:23] == 'ongoing-request="false"':
    #             try:
    #                 cont = client.get_object(Bucket=Bucket_Name, Key=key)
    #             except:
    #                 raise AssertionError("restore get fail " + key)
    #             else:
    #                 content = cont['Body'].read()
    #                 eq(content, open(large_file_url, 'rb').read() + str(n).encode())
    #                 if n == large_file_nums:
    #                     break
    #                 else:
    #                     n += 1
    #         else:
    #             if run_time < wait_time:
    #                 client.restore_object(Bucket=Bucket_Name, Key=key, RestoreRequest={'Days': 20})
    #             else:
    #                 raise AssertionError("restore fail")
    #     # 验证批量删除
    #     n = 1
    #     while n <= large_file_nums:
    #         end = time.time()
    #         run_time = end - start
    #         key = large_name + '_' + str(n)
    #         try:
    #             client.delete_object(Bucket=Bucket_Name, Key=key)
    #         except:
    #             raise AssertionError("delete fail" + key)
    #         else:
    #             try:
    #                 client.restore_object(Bucket=Bucket_Name, Key=key, RestoreRequest={'Days': 5})
    #             except:
    #                 if n == large_file_nums:
    #                     break
    #                 else:
    #                     n += 1
    #             else:
    #                 raise AssertionError("delete fail")
    #
    # ('lifecycle1')
    # def test_lifecycle_archive():
    #     Bucket_Name = get_new_bucket()
    #     client = get_client()
    #     # 设置生命周期
    #     client.put_bucket_lifecycle(
    #         Bucket=Bucket_Name,
    #         LifecycleConfiguration={
    #             'Rules': [
    #                 {
    #                     'Status': 'Enabled',
    #                     'Prefix': '',
    #                     'Transition': {
    #                         'Date': '2021-08-26 00:00:00',
    #                         'StorageClass': 'ARCHIVE'
    #                     },
    #                 }], })
    #     # 批量转储
    #     n = 1
    #     while n <= small_file_nums:
    #         key = small_name + '_' + str(n)
    #         body = 'Number ' + str(n)
    #         client.put_object(Bucket=Bucket_Name, Key=key, Body=body)
    #         n += 1
    #     # life_time = (datetime.datetime.now() + datetime.timedelta(days=day)).strftime("%Y-%m-%d %H:%M:%S")
    #     # comand = "date -s" + "\"{}\"".format(life_time)
    #     # os.system(comand)
    #     time.sleep(interval_time)
    #     start = time.time()
    #     # 确保都已转储成功
    #     n = 1
    #     while n <= small_file_nums:
    #         while true:
    #             key = small_name + '_' + str(n)
    #             resp = client.head_object(Bucket=Bucket_Name, Key=key)
    #             end = time.time()
    #             run_time = end - start
    #             try:
    #                 val = resp['ResponseMetadata']['HTTPHeaders']['x-amz-restore']
    #             except:
    #                 if run_time > wait_time:
    #                     raise AssertionError(key + "lifecycle archive fail")
    #             else:
    #                 if run_time < wait_time:
    #                     if val == '':
    #                         break
    #                 else:
    #                     raise AssertionError("lifecycle archive fail")
    #         if n == small_file_nums:
    #             break
    #         else:
    #             n += 1
    #     # life_time = (datetime.datetime.now() + datetime.timedelta(days=-day)).strftime("%Y-%m-%d %H:%M:%S")
    #     # comand = "date -s" + "\"{}\"".format(life_time)
    #     # os.system(comand)
    #     time.sleep(interval_time)
    #     start = time.time()
    #
    # ('archive_tape')
    # def test_archive_tape_file_head():
    #     # 批量上传验证对象的storage_class和状态信息
    #     n = 1
    #     Bucket_Name = get_new_bucket()
    #     client = get_client()
    #     while n <= small_file_nums:
    #         key = small_name + '_' + str(n)
    #         body = 'Number ' + str(n)
    #         client.put_object(Bucket=Bucket_Name, Key=key, Body=body, StorageClass=StorageClass_Type)
    #         n += 1
    #     time.sleep(interval_time)
    #     start = time.time()
    #     n = 1
    #     while n <= small_file_nums:
    #         while true:
    #             key = small_name + '_' + str(n)
    #             resp = client.head_object(Bucket=Bucket_Name, Key=key)
    #             end = time.time()
    #             run_time = end - start
    #             try:
    #                 val = resp['ResponseMetadata']['HTTPHeaders']['x-amz-restore']
    #             except:
    #                 if run_time > wait_time:
    #                     raise AssertionError(key + "fail")
    #             else:
    #                 if run_time < wait_time:
    #                     if val == '':
    #                         break
    #                 else:
    #                     raise AssertionError("archive fail")
    #         if n == small_file_nums:
    #             break
    #         else:
    #             n += 1
    #     os.system('radosgw-admin bucket list --bucket=' + Bucket_Name + ' >/root/stat')
    #     content = open('/root/stat', 'rb').read()
    #     name = re.compile('"name": "(.*)"')
    #     merge_flag = re.compile('"merge_flags": (\d+),')
    #     size = re.compile('"size": (\d+),')
    #     storage_class = re.compile('"storage_class": "(\w+)"')
    #     names = name.findall(content.decode())
    #     n = 1
    #     while n <= small_file_nums:
    #         key = small_name + '_' + str(n)
    #         nums = 0
    #         for name_num in names:
    #             if name_num == key:
    #                 break
    #             nums += 1
    #         storage_class_num = storage_class.findall(content.decode())[nums]
    #         merge_flag_num = merge_flag.findall(content.decode())[nums]
    #         eq(storage_class_num, 'ARCHIVE')
    #         eq(merge_flag_num, '8')
    #         n += 1
    #     # 批量取回对象状态信息验证
    #     n = 1
    #     while n <= small_file_nums:
    #         key = small_name + '_' + str(n)
    #         try:
    #             client.restore_object(Bucket=Bucket_Name, Key=key, RestoreRequest={'Days': 20})
    #         except:
    #             if run_time > wait_time:
    #                 raise AssertionError(key + "fail")
    #         else:
    #             if n == small_file_nums:
    #                 break
    #             else:
    #                 n += 1
    #     n = 1
    #     while n <= small_file_nums:
    #         end = time.time()
    #         run_time = end - start
    #         key = small_name + '_' + str(n)
    #         resp = client.head_object(Bucket=Bucket_Name, Key=key)
    #         val = resp['ResponseMetadata']['HTTPHeaders']['x-amz-restore']
    #         if val[0:23] == 'ongoing-request="false"':
    #             os.system('radosgw-admin bucket list --bucket=' + Bucket_Name + ' >/root/stat')
    #             content = open('/root/stat', 'rb').read()
    #             name = re.compile('"name": "(.*)"')
    #             merge_flag = re.compile('"merge_flags": (\d+),')
    #             size = re.compile('"size": (\d+),')
    #             storage_class = re.compile('"storage_class": "(\w+)"')
    #             names = name.findall(content.decode())
    #             nums = 0
    #             for name_num in names:
    #                 if name_num == key:
    #                     break
    #                 nums += 1
    #             storage_class_num = storage_class.findall(content.decode())[nums]
    #             merge_flag_num = merge_flag.findall(content.decode())[nums]
    #             eq(storage_class_num, 'ARCHIVE')
    #             eq(merge_flag_num, '24')
    #             n += 1
    #         else:
    #             if run_time < wait_time:
    #                 client.restore_object(Bucket=Bucket_Name, Key=key, RestoreRequest={'Days': 20})
    #             else:
    #                 raise AssertionError("restore fail")
    #     n = 1
    #     while n <= small_file_nums:
    #         end = time.time()
    #         run_time = end - start
    #         key = small_name + '_' + str(n)
    #         try:
    #             client.delete_object(Bucket=Bucket_Name, Key=key)
    #         except:
    #             raise AssertionError("delete fail")
    #         else:
    #             try:
    #                 client.restore_object(Bucket=Bucket_Name, Key=key, RestoreRequest={'Days': 5})
    #             except:
    #                 if n == small_file_nums:
    #                     break
    #                 else:
    #                     n += 1
    #             else:
    #                 raise AssertionError("delete fail")
    #
    # # os.system('rm -rf /root/stat')
    #
    # ('archive_glacier')
    # def test_archive_glacier_file_head():
    #     # 批量上传验证对象的storage_class和状态信息
    #     n = 1
    #     Bucket_Name = get_new_bucket()
    #     client = get_client()
    #     while n <= small_file_nums:
    #         key = small_name + '_' + str(n)
    #         body = 'Number ' + str(n)
    #         client.put_object(Bucket=Bucket_Name, Key=key, Body=body, StorageClass='GLACIER')
    #         n += 1
    #     time.sleep(interval_time)
    #     start = time.time()
    #     n = 1
    #     while n <= small_file_nums:
    #         while true:
    #             key = small_name + '_' + str(n)
    #             resp = client.head_object(Bucket=Bucket_Name, Key=key)
    #             end = time.time()
    #             run_time = end - start
    #             try:
    #                 val = resp['ResponseMetadata']['HTTPHeaders']['x-amz-restore']
    #             except:
    #                 if run_time > wait_time:
    #                     raise AssertionError(key + "fail")
    #             else:
    #                 if run_time < wait_time:
    #                     if val == '':
    #                         break
    #                 else:
    #                     raise AssertionError("archive fail")
    #         if n == small_file_nums:
    #             break
    #         else:
    #             n += 1
    #     os.system('radosgw-admin bucket list --bucket=' + Bucket_Name + ' >/root/stat')
    #     content = open('/root/stat', 'rb').read()
    #     name = re.compile('"name": "(.*)"')
    #     merge_flag = re.compile('"merge_flags": (\d+),')
    #     size = re.compile('"size": (\d+),')
    #     storage_class = re.compile('"storage_class": "(\w+)"')
    #     names = name.findall(content.decode())
    #     n = 1
    #     while n <= small_file_nums:
    #         key = small_name + '_' + str(n)
    #         nums = 0
    #         for name_num in names:
    #             if name_num == key:
    #                 break
    #             nums += 1
    #         storage_class_num = storage_class.findall(content.decode())[nums]
    #         merge_flag_num = merge_flag.findall(content.decode())[nums]
    #         eq(storage_class_num, 'ARCHIVE')
    #         eq(merge_flag_num, '8')
    #         n += 1
    #     # 批量取回对象状态信息验证
    #     n = 1
    #     while n <= small_file_nums:
    #         key = small_name + '_' + str(n)
    #         try:
    #             client.restore_object(Bucket=Bucket_Name, Key=key, RestoreRequest={'Days': 20})
    #         except:
    #             if run_time > wait_time:
    #                 raise AssertionError(key + "fail")
    #         else:
    #             if n == small_file_nums:
    #                 break
    #             else:
    #                 n += 1
    #     n = 1
    #     while n <= small_file_nums:
    #         end = time.time()
    #         run_time = end - start
    #         key = small_name + '_' + str(n)
    #         resp = client.head_object(Bucket=Bucket_Name, Key=key)
    #         val = resp['ResponseMetadata']['HTTPHeaders']['x-amz-restore']
    #         if val[0:23] == 'ongoing-request="false"':
    #             os.system('radosgw-admin bucket list --bucket=' + Bucket_Name + ' >/root/stat')
    #             content = open('/root/stat', 'rb').read()
    #             name = re.compile('"name": "(.*)"')
    #             merge_flag = re.compile('"merge_flags": (\d+),')
    #             size = re.compile('"size": (\d+),')
    #             storage_class = re.compile('"storage_class": "(\w+)"')
    #             names = name.findall(content.decode())
    #             nums = 0
    #             for name_num in names:
    #                 if name_num == key:
    #                     break
    #                 nums += 1
    #             storage_class_num = storage_class.findall(content.decode())[nums]
    #             merge_flag_num = merge_flag.findall(content.decode())[nums]
    #             eq(storage_class_num, 'GLACIER')
    #             eq(merge_flag_num, '72')
    #             n += 1
    #         else:
    #             if run_time < wait_time:
    #                 client.restore_object(Bucket=Bucket_Name, Key=key, RestoreRequest={'Days': 20})
    #             else:
    #                 raise AssertionError("restore fail")
    #     n = 1
    #     while n <= small_file_nums:
    #         end = time.time()
    #         run_time = end - start
    #         key = small_name + '_' + str(n)
    #         try:
    #             client.delete_object(Bucket=Bucket_Name, Key=key)
    #         except:
    #             raise AssertionError("delete fail")
    #         else:
    #             try:
    #                 client.restore_object(Bucket=Bucket_Name, Key=key, RestoreRequest={'Days': 5})
    #             except:
    #                 if n == small_file_nums:
    #                     break
    #                 else:
    #                     n += 1
    #             else:
    #                 raise AssertionError("delete fail")
    #     os.system('rm -rf /root/stat')
