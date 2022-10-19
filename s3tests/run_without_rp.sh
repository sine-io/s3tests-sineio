
pytest tests -m "sio and not lifecycle_need_speedup" -n 10 --reruns 2 > run-without-rp-`date`.log 2>&1 &
