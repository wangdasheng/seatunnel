#!/bin/bash

set -e

PLUGIN_JAR="/Users/wangzhijun/IdeaProjects/szwego-etldevops/szwego-seatunnel-plugin-sql-lookup/target/szwego-seatunnel-plugin-sql-lookup-1.0.0.jar"
NFS_SERVER="10.1.12.11"
NFS_PATH="/wangzhijun/seatunnel/connectors"

echo "========================================="
echo "Deploying SQL Lookup Plugin to NFS"
echo "========================================="
echo ""

# Check if plugin jar exists
if [ ! -f "$PLUGIN_JAR" ]; then
    echo "❌ Error: Plugin jar not found at $PLUGIN_JAR"
    exit 1
fi

echo "Plugin jar size: $(ls -lh $PLUGIN_JAR | awk '{print $5}')"
echo ""

# Verify SPI file is in the jar
echo "Verifying SPI configuration..."
if jar tf "$PLUGIN_JAR" | grep -q "META-INF/services/org.apache.seatunnel.api.table.factory.TableTransformFactory"; then
    echo "✓ SPI configuration found in jar"
else
    echo "❌ SPI configuration NOT found in jar!"
    exit 1
fi

echo ""
echo "Copying to NFS server..."
echo "  Server: $NFS_SERVER"
echo "  Path: $NFS_PATH"
echo ""

# Copy to NFS (you may need to adjust this based on your NFS access method)
# Option 1: If NFS is mounted locally
if [ -d "$NFS_PATH" ]; then
    cp "$PLUGIN_JAR" "$NFS_PATH/"
    echo "✓ Copied to NFS (mounted directory)"
else
    # Option 2: Use scp if you have SSH access to NFS server
    echo "NFS path not mounted locally. Using scp..."
    scp "$PLUGIN_JAR" "root@$NFS_SERVER:$NFS_PATH/"
    echo "✓ Copied via SCP"
fi

echo ""
echo "Verifying deployment..."
ls -lh "$NFS_PATH/szwego-seatunnel-plugin-sql-lookup-1.0.0.jar"

echo ""
echo "========================================="
echo "✓ Deployment complete!"
echo "========================================="
echo ""
echo "Next steps:"
echo "1. Restart SeaTunnel pods:"
echo "   kubectl rollout restart deployment/seatunnel-master deployment/seatunnel-worker -n seatunnel"
echo ""
echo "2. Wait for pods to be ready:"
echo "   kubectl wait --for=condition=ready pod -l app=seatunnel-master -n seatunnel --timeout=120s"
echo "   kubectl wait --for=condition=ready pod -l app=seatunnel-worker -n seatunnel --timeout=120s"
echo ""
echo "3. Submit your job again"
