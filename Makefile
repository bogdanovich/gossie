gen-go: cassandra.thrift
	rm -rf ./src/cassandra
	thrift --gen go:package_prefix=github.com/betable/gossie/src/ -out ./src ./cassandra.thrift
	sed -E -i.orig 's#git.apache.org/thrift.git/lib/go/thrift#github.com/betable/go-thrift/thrift#' src/cassandra/*.go src/cassandra/cassandra-remote/*.go
	rm src/cassandra/*.orig src/cassandra/cassandra-remote/*.orig
