gen-go: cassandra.thrift
	rm -rf ./src/cassandra
	thrift --gen go:package_prefix=github.com/betable/gossie -out ./src ./cassandra.thrift
