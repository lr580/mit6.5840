go run mrcoordinator.go pg-*.txt
go run mrworker.go wc.so
cat mr-out-* | sort | more

cd src/main
go build -buildmode=plugin ../mrapps/wc.go
rm mr-out*
go run mrsequential.go wc.so pg*.txt
more mr-out-0

# 每次修改代码一定要重新 build wc.go

# WA: 除此之外其他通过
# *** Starting job count test.
# read unix @->/var/tmp/5840-mr-1000: read: connection reset by peer
# 2025/10/03 19:54:11 dialing:dial-http unix /var/tmp/5840-mr-1000: read unix @->/var/tmp/5840-mr-1000: read: connection reset by peer
# 2025/10/03 19:54:11 dialing:dial unix /var/tmp/5840-mr-1000: connect: connection refused
# 2025/10/03 19:54:11 dialing:dial unix /var/tmp/5840-mr-1000: connect: connection refused
# cat: 'mr-out*': No such file or directory
# test-mr.sh: line 214: [: : integer expression expected
# --- map jobs ran incorrect number of times ( != 8)
# --- job count test: FAIL