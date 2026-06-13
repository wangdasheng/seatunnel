#!/bin/bash

# Set the SeaTunnel home
SEATUNNEL_HOME="/Users/wangzhijun/sourceCode/seatunnel"

# Build classpath with all necessary jars
CLASSPATH=""

# Add connectors directory
for jar in $SEATUNNEL_HOME/connectors/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done

# Add lib directory if exists
if [ -d "$SEATUNNEL_HOME/lib" ]; then
    for jar in $SEATUNNEL_HOME/lib/*.jar; do
        CLASSPATH="$CLASSPATH:$jar"
    done
fi

# Add seatunnel-engine-examples classes
CLASSPATH="$CLASSPATH:$SEATUNNEL_HOME/seatunnel-examples/seatunnel-engine-examples/target/classes"

# Add all seatunnel jars from target directories
for jar in $(find $SEATUNNEL_HOME -name "*.jar" -path "*/target/*" | grep -E "(seatunnel-core|seatunnel-api|seatunnel-common|seatunnel-engine)" | head -50); do
    CLASSPATH="$CLASSPATH:$jar"
done

echo "Classpath: $CLASSPATH"
echo ""
echo "Running SqlLookup test..."
echo ""

# Run the example
java -cp "$CLASSPATH" \
    -Dlog4j2.configurationFile=file:$SEATUNNEL_HOME/config/log4j2.properties \
    org.apache.seatunnel.example.engine.SeaTunnelEngineLocalExample
