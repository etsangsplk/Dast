./waf && ./run_all.py -hh config/edge-seperate.yml -s '10'  -c '30' -r '3'  -cc config/tpcch.yml -cc config/client_closed.yml -cc config/concurrent_20.yml -cc config/chronos.yml -b tpcc -m chronos:chronos --allow-client-overlap testing10

#./waf && ./run_all.py -hh config/edge-seperate.yml -s '3'  -c '3' -r '3'  -cc config/tpcch.yml -cc config/client_closed.yml -cc config/concurrent_10.yml -cc config/slog.yml -b tpcc -m chronos:chronos --allow-client-overlap testing10
