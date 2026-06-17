#!/bin/bash
# ============================================
# 持续写入数据到 test.cdc_test 表
# 模拟 CDC 源端持续产生变更事件
# 用法: ./scripts/cdc_continuous_write.sh [间隔秒数]
# ============================================

MYSQL_HOST="mysql.in.wegoab.com"
MYSQL_PORT="3306"
MYSQL_USER="root"
MYSQL_PASS="truedian#123"
MYSQL_DB="test"
MYSQL_TABLE="cdc_test"

# 写入间隔(秒)，可通过第一个参数覆盖
INTERVAL=${1:-2}

# MySQL 命令
MYSQL_CMD="mysql -h${MYSQL_HOST} -P${MYSQL_PORT} -u${MYSQL_USER} -p${MYSQL_PASS} ${MYSQL_DB} -e"

# 名字库
NAMES=("Alice" "Bob" "Charlie" "David" "Eve" "Frank" "Grace" "Henry" "Ivy" "Jack"
       "Kate" "Leo" "Mia" "Noah" "Olivia" "Peter" "Quinn" "Rose" "Sam" "Tina")

COUNTER=1

echo "============================================"
echo "CDC 持续写入脚本启动"
echo "数据库: ${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}.${MYSQL_TABLE}"
echo "写入间隔: ${INTERVAL}s"
echo "按 Ctrl+C 停止"
echo "============================================"

# 清空旧数据
echo "清理旧数据..."
${MYSQL_CMD} "TRUNCATE TABLE ${MYSQL_TABLE};" 2>/dev/null

while true; do
    # --- INSERT: 新增随机行 ---
    NAME=${NAMES[$((RANDOM % ${#NAMES[@]}))]}
    EMAIL="$(echo "$NAME" | tr '[:upper:]' '[:lower:]')@example.com"
    AGE=$((18 + RANDOM % 50))
    SCORE=$(awk "BEGIN {printf \"%.2f\", ${RANDOM} * 100 / 32767}")

    ${MYSQL_CMD} "INSERT INTO ${MYSQL_TABLE} (name, email, age, score) VALUES ('${NAME}', '${EMAIL}', ${AGE}, ${SCORE});" 2>/dev/null
    echo "[$(date '+%H:%M:%S')] #${COUNTER} INSERT: name=${NAME} age=${AGE} score=${SCORE}"

    # --- 每 5 次做一次 UPDATE ---
    if [ $((COUNTER % 5)) -eq 0 ]; then
        NEW_AGE=$((18 + RANDOM % 50))
        NEW_SCORE=$(awk "BEGIN {printf \"%.2f\", ${RANDOM} * 100 / 32767}")
        ${MYSQL_CMD} "UPDATE ${MYSQL_TABLE} SET age=${NEW_AGE}, score=${NEW_SCORE} WHERE id % 10 = 0 LIMIT 3;" 2>/dev/null
        echo "[$(date '+%H:%M:%S')] #${COUNTER} UPDATE: set age=${NEW_AGE} score=${NEW_SCORE}"
    fi

    # --- 每 15 次做一次 DELETE + INSERT ---
    if [ $((COUNTER % 15)) -eq 0 ]; then
        ${MYSQL_CMD} "DELETE FROM ${MYSQL_TABLE} WHERE id % 20 = 0 LIMIT 2;" 2>/dev/null
        echo "[$(date '+%H:%M:%S')] #${COUNTER} DELETE: removed oldest rows"
    fi

    COUNTER=$((COUNTER + 1))
    sleep "${INTERVAL}"
done
