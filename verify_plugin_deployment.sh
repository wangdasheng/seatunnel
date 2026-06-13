#!/bin/bash

echo "========================================="
echo "Verifying SQL Lookup Plugin Deployment"
echo "========================================="
echo ""

NAMESPACE="seatunnel"
MASTER_POD=$(kubectl get pods -n $NAMESPACE -l app=seatunnel-master -o jsonpath='{.items[0].metadata.name}')

echo "Master Pod: $MASTER_POD"
echo ""

# 1. Check file timestamp and size
echo "1. Checking jar file on NFS..."
kubectl exec -n $NAMESPACE $MASTER_POD -- ls -lh /opt/seatunnel/connectors/szwego-seatunnel-plugin-sql-lookup-1.0.0.jar
echo ""

# 2. Verify SPI configuration exists
echo "2. Verifying SPI configuration in jar..."
if kubectl exec -n $NAMESPACE $MASTER_POD -- bash -c "jar tf /opt/seatunnel/connectors/szwego-seatunnel-plugin-sql-lookup-1.0.0.jar | grep -q TableTransformFactory" 2>/dev/null; then
    echo "✓ SPI configuration found!"
    kubectl exec -n $NAMESPACE $MASTER_POD -- bash -c "jar tf /opt/seatunnel/connectors/szwego-seatunnel-plugin-sql-lookup-1.0.0.jar | grep TableTransformFactory"
else
    echo "❌ SPI configuration NOT found! Please upload the new version."
    exit 1
fi
echo ""

# 3. Check plugin-mapping.properties
echo "3. Checking plugin-mapping.properties..."
kubectl exec -n $NAMESPACE $MASTER_POD -- cat /opt/seatunnel/connectors/plugin-mapping.properties | grep SqlLookup || echo "❌ Mapping not found"
echo ""

# 4. Check if pods need restart
echo "4. Pod restart status..."
kubectl get pods -n $NAMESPACE -l app=seatunnel-master -o wide
kubectl get pods -n $NAMESPACE -l app=seatunnel-worker -o wide
echo ""

echo "========================================="
echo "Next Steps:"
echo "========================================="
echo "If the file timestamp is old (before 16:57), you need to re-upload the new jar."
echo ""
echo "After uploading, restart pods:"
echo "  kubectl rollout restart deployment/seatunnel-master deployment/seatunnel-worker -n $NAMESPACE"
echo ""
echo "Then check logs:"
echo "  kubectl logs -f deployment/seatunnel-master -n $NAMESPACE | grep -i sqllookup"
