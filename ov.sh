./waf configure build -t && ./run_all.py -hh config/ov-cluster.yml -s '4' -c '4' -r '1' -cc config/tpcc.yml -cc config/client_closed.yml -cc config/concurrent_1000.yml -cc config/brq.yml -b tpcc -m ov:ov --allow-client-overlap testing01
#./waf configure build -t && ./run_all.py -hh config/ov-cluster.yml -s '4' -c '4' -r '1' -cc config/rw.yml -cc config/client_closed.yml -cc config/concurrent_1000.yml -cc config/brq.yml -b rw_benchmark -m ov:ov --allow-client-overlap testing01
