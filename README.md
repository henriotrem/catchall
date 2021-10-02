# catchall

rm -R ~\go\bin\data
~\go\bin\worker.exe -n 1 -a :11001
~\go\bin\worker.exe -n 2 -a :11002 -j 127.0.0.1:11001
~\go\bin\worker.exe -n 3 -a :11003 -j 127.0.0.1:11001
~\go\bin\worker-api.exe 22001 127.0.0.1:11001,127.0.0.1:11002,127.0.0.1:11003
~\go\bin\redis-cli.exe -p 11001

~\go\bin\go-wrk.exe -c 80 -d 5 -M "PUT" http://127.0.0.1:22001/events/test/delivered

rm -R ~\go\bin\clusters\1\data
~\go\bin\worker.exe -n 1 -a :11001
~\go\bin\worker-api.exe 22001 127.0.0.1:11001
~\go\bin\redis-cli.exe -p 11001

rm -R ~\go\bin\clusters\2\data
~\go\bin\worker.exe -n 1 -a :11002
~\go\bin\worker-api.exe 22002 127.0.0.1:11002
~\go\bin\redis-cli.exe -p 11002

rm -R ~\go\bin\clusters\3\data
~\go\bin\worker.exe -n 1 -a :11003
~\go\bin\worker-api.exe 22003 127.0.0.1:11003
~\go\bin\redis-cli.exe -p 11003
