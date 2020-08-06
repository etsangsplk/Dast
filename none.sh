./waf configure build -t
./run_all.py -hh config/cluster.yml -s '3' -c '1' -r '1' -z 0.4 -cc config/rw.yml -cc config/client_closed.yml -cc config/concurrent_1.yml -cc config/none.yml -b rw_benchmark -m none:none --allow-client-overlap testing01

