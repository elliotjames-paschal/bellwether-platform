#!/bin/bash
# Wait for pipeline PID 38724 to finish, keeping Mac awake
echo "Waiting for pipeline (PID 38724) to finish..."
while kill -0 38724 2>/dev/null; do
    sleep 10
done
echo "Pipeline finished at $(date)"
