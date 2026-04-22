#!/bin/bash

# Test SFF scheduling by sending many requests quickly
# This will queue them up so we can see SFF in action

echo "Sending 30 mixed requests quickly to test SFF scheduling..."
echo "Watch the server logs to see small files served before large ones!"
echo ""

# Send 30 requests as fast as possible (mix of small, medium, large)
for i in {1..10}; do
    curl -s http://localhost:8080/large.html > /dev/null &
    curl -s http://localhost:8080/small.html > /dev/null &
    curl -s http://localhost:8080/medium.html > /dev/null &
done

# Wait for all to complete
wait

echo ""
echo "Done! Check server logs above - you should see:"
echo "- Small files (size=9006) picked before medium/large"
echo "- Even if large files arrived earlier (lower seq numbers)"
