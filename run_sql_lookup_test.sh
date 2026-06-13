#!/bin/bash

# SeaTunnel SQL Lookup Plugin Test Script
SEATUNNEL_HOME="/Users/wangzhijun/sourceCode/seatunnel"

echo "========================================="
echo "Starting SeaTunnel SQL Lookup Test"
echo "========================================="
echo ""

# Set Java options
JAVA_OPTS="-Dfile.encoding=UTF-8"
JAVA_OPTS="$JAVA_OPTS -Dlog4j2.isThreadContextMapInheritable=true"

# Build classpath
CLASSPATH=""

# Add all jars from connectors directory (includes our SQL Lookup plugin)
for jar in $SEATUNNEL_HOME/connectors/*.jar; do
    if [ -f "$jar" ]; then
        CLASSPATH="$CLASSPATH:$jar"
        echo "Added connector: $(basename $jar)"
    fi
done

# Add all necessary SeaTunnel jars from target directories
echo ""
echo "Adding SeaTunnel dependencies..."

# Find and add all required jars
find_jars() {
    local pattern=$1
    find $SEATUNNEL_HOME -name "*.jar" -path "*/target/*" | grep -E "$pattern" | while read jar; do
        if [ -f "$jar" ]; then
            echo "$jar"
        fi
    done
}

# Core modules
CORE_MODULES=(
    "seatunnel-api"
    "seatunnel-common"
    "seatunnel-config-shade"
    "seatunnel-plugin-discovery"
    "seatunnel-core-starter"
    "seatunnel-starter"
    "seatunnel-engine-common"
    "seatunnel-engine-core"
    "seatunnel-engine-client"
    "seatunnel-engine-server"
    "seatunnel-transforms-v2"
)

for module in "${CORE_MODULES[@]}"; do
    jars=$(find_jars "$module.*\.jar$" | head -1)
    if [ -n "$jars" ]; then
        CLASSPATH="$CLASSPATH:$jars"
        echo "Added: $(basename $jars)"
    fi
done

# Shade modules (important for Jackson, Guava, etc.)
SHADE_MODULES=(
    "seatunnel-jackson"
    "seatunnel-guava"
    "seatunnel-arrow"
    "seatunnel-hazelcast"
    "seatunnel-hikari"
    "seatunnel-janino"
)

for module in "${SHADE_MODULES[@]}"; do
    jars=$(find_jars "$module.*\.jar$" | head -1)
    if [ -n "$jars" ]; then
        CLASSPATH="$CLASSPATH:$jars"
        echo "Added shade: $(basename $jars)"
    fi
done

# Connector jars
CONNECTOR_JARS=(
    "connector-fake"
    "connector-console"
    "connector-jdbc"
    "connector-cdc-mysql"
    "connector-cdc-base"
)

for connector in "${CONNECTOR_JARS[@]}"; do
    jars=$(find_jars "$connector.*\.jar$" | head -1)
    if [ -n "$jars" ]; then
        CLASSPATH="$CLASSPATH:$jars"
        echo "Added connector: $(basename $jars)"
    fi
done

# Format jars
FORMAT_JARS=(
    "seatunnel-format-json"
    "seatunnel-format-compatible-debezium-json"
)

for format in "${FORMAT_JARS[@]}"; do
    jars=$(find_jars "$format.*\.jar$" | head -1)
    if [ -n "$jars" ]; then
        CLASSPATH="$CLASSPATH:$jars"
        echo "Added format: $(basename $jars)"
    fi
done

# Add examples classes
EXAMPLES_CLASS="$SEATUNNEL_HOME/seatunnel-examples/seatunnel-engine-examples/target/classes"
if [ -d "$EXAMPLES_CLASS" ]; then
    CLASSPATH="$CLASSPATH:$EXAMPLES_CLASS"
    echo "Added examples classes"
fi

echo ""
echo "========================================="
echo "Running SeaTunnel Engine Local Example"
echo "========================================="
echo ""

# Run the example
java $JAVA_OPTS -cp "$CLASSPATH" \
    -Dlog4j2.configurationFile=file:$SEATUNNEL_HOME/config/log4j2_client.properties \
    org.apache.seatunnel.example.engine.SeaTunnelEngineLocalExample

echo ""
echo "========================================="
echo "Execution completed"
echo "========================================="
