#!/bin/bash
# test-raft.sh

TIMES=${1:-3}  # 默认运行 3 次

for i in $(seq 1 $TIMES); do
    echo "========================================="
    echo "Run $i/$TIMES"
    echo "========================================="

    if ! timeout 600 go test -v; then
        echo "FAILED at run $i"
        exit 1
    fi

    echo ""
done

echo "✅ All $TIMES runs passed!"