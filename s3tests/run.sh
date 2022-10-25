
pytest tests -m "sio and not need_speedup" -n 10 --reruns 2 --reportportal > run-`date +"%Y-%m-%d-%T"`.log 2>&1 &
