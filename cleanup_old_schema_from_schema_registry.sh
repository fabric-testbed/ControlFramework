#Clean up Schema Registry
USER_NAME=fabric
PASSWORD=fabric
SR_URL=http://localhost:8081

curl -X DELETE -u $USER_NAME:$PASSWORD $SR_URL/subjects/net1-am-topic-value
curl -X DELETE -u $USER_NAME:$PASSWORD $SR_URL/subjects/net1-am-topic-value/?permanent=true

curl -X DELETE -u $USER_NAME:$PASSWORD $SR_URL/subjects/site1-am-topic-value
curl -X DELETE -u $USER_NAME:$PASSWORD $SR_URL/subjects/site1-am-topic-value/?permanent=true

curl -X DELETE -u $USER_NAME:$PASSWORD $SR_URL/subjects/broker-topic-value
curl -X DELETE -u $USER_NAME:$PASSWORD $SR_URL/subjects/broker-topic-value/?permanent=true

curl -X DELETE -u $USER_NAME:$PASSWORD $SR_URL/subjects/orchestrator-topic-value
curl -X DELETE -u $USER_NAME:$PASSWORD $SR_URL/subjects/orchestrator-topic-value/?permanent=true


curl -X DELETE -u $USER_NAME:$PASSWORD $SR_URL/subjects/managecli-topic-value
curl -X DELETE -u $USER_NAME:$PASSWORD $SR_URL/subjects/managecli-topic-value/?permanent=true