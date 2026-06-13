#!/bin/bash

set -e

NAMESPACE="seatunnel"
PLUGIN_JAR="/Users/wangzhijun/IdeaProjects/szwego-etldevops/szwego-seatunnel-plugin-sql-lookup/target/szwego-seatunnel-plugin-sql-lookup-1.0.0.jar"

echo "========================================="
echo "SQL Lookup Plugin K8s Deployment Script"
echo "Version: 1.0.0 (Serializable Fix)"
echo "========================================="
echo ""

# Check if plugin jar exists
if [ ! -f "$PLUGIN_JAR" ]; then
    echo "❌ Error: Plugin jar not found at $PLUGIN_JAR"
    exit 1
fi

echo "✓ Plugin jar found ($(ls -lh $PLUGIN_JAR | awk '{print $5}'))"
echo ""

# Get pods
echo "Finding SeaTunnel pods..."
MASTER_POD=$(kubectl get pods -n $NAMESPACE -l app=seatunnel-master -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
WORKER_PODS=$(kubectl get pods -n $NAMESPACE -l app=seatunnel-worker -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")

if [ -z "$MASTER_POD" ]; then
    echo "❌ Error: No master pod found"
    echo "   Available pods:"
    kubectl get pods -n $NAMESPACE
    exit 1
fi

echo "✓ Master pod: $MASTER_POD"
if [ -n "$WORKER_PODS" ]; then
    echo "✓ Worker pods: $WORKER_PODS"
else
    echo "⚠ Warning: No worker pods found"
fi
echo ""

# Copy plugin jar to all pods
echo "Copying plugin jar to pods..."

# Copy to master
kubectl cp "$PLUGIN_JAR" "$NAMESPACE/$MASTER_POD:/opt/seatunnel/connectors/szwego-seatunnel-plugin-sql-lookup-1.0.0.jar"
echo "  ✓ Copied to master ($MASTER_POD)"

# Copy to workers
if [ -n "$WORKER_PODS" ]; then
    for POD in $WORKER_PODS; do
        kubectl cp "$PLUGIN_JAR" "$NAMESPACE/$POD:/opt/seatunnel/connectors/szwego-seatunnel-plugin-sql-lookup-1.0.0.jar"
        echo "  ✓ Copied to worker ($POD)"
    done
fi

echo ""

# Update plugin-mapping.properties on all pods
echo "Updating plugin-mapping.properties..."

update_plugin_mapping() {
    local POD=$1
    kubectl exec -n $NAMESPACE $POD -- bash -c '
        MAPPING_FILE="/opt/seatunnel/plugin-mapping.properties"
        if ! grep -q "SqlLookup" "$MAPPING_FILE"; then
            echo "" >> "$MAPPING_FILE"
            echo "# Custom Transform Plugins" >> "$MAPPING_FILE"
            echo "seatunnel.transform.SqlLookup = szwego-seatunnel-plugin-sql-lookup-1.0.0" >> "$MAPPING_FILE"
            echo "Updated"
        else
            echo "Already exists"
        fi
    '
}

update_plugin_mapping $MASTER_POD
echo "  ✓ Updated master ($MASTER_POD)"

if [ -n "$WORKER_PODS" ]; then
    for POD in $WORKER_PODS; do
        update_plugin_mapping $POD
        echo "  ✓ Updated worker ($POD)"
    done
fi

echo ""

# Verify installation
echo "Verifying installation..."
echo ""
echo "Master pod ($MASTER_POD):"
JAR_SIZE=$(kubectl exec -n $NAMESPACE $MASTER_POD -- ls -lh /opt/seatunnel/connectors/szwego-seatunnel-plugin-sql-lookup-1.0.0.jar 2>/dev/null | awk '{print $5}' || echo "NOT FOUND")
echo "  Jar size: $JAR_SIZE"
MAPPING=$(kubectl exec -n $NAMESPACE $MASTER_POD -- grep SqlLookup /opt/seatunnel/plugin-mapping.properties 2>/dev/null || echo "NOT FOUND")
echo "  Mapping: $MAPPING"

if [ -n "$WORKER_PODS" ]; then
    echo ""
    echo "Worker pods:"
    for POD in $WORKER_PODS; do
        JAR_SIZE=$(kubectl exec -n $NAMESPACE $POD -- ls -lh /opt/seatunnel/connectors/szwego-seatunnel-plugin-sql-lookup-1.0.0.jar 2>/dev/null | awk '{print $5}' || echo "NOT FOUND")
        echo "  $POD: Jar size = $JAR_SIZE"
    done
fi

echo ""
echo "========================================="
echo "✓ Deployment complete!"
echo "========================================="
echo ""
echo "Next steps:"
echo "1. Restart pods to load the new plugin:"
echo "   kubectl rollout restart deployment/seatunnel-master -n $NAMESPACE"
echo "   kubectl rollout restart deployment/seatunnel-worker -n $NAMESPACE"
echo ""
echo "2. Wait for pods to be ready:"
echo "   kubectl wait --for=condition=ready pod -l app=seatunnel-master -n $NAMESPACE --timeout=120s"
echo "   kubectl wait --for=condition=ready pod -l app=seatunnel-worker -n $NAMESPACE --timeout=120s"
echo ""
echo "3. Check logs to verify plugin is loaded:"
echo "   kubectl logs -f deployment/seatunnel-master -n $NAMESPACE | grep -i sqllookup"
echo ""
echo "4. Submit your job again with the test configuration"
echo ""
echo "To restart now, run:"
echo "   kubectl rollout restart deployment/seatunnel-master deployment/seatunnel-worker -n $NAMESPACE"
