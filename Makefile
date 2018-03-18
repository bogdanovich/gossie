gen-go: cassandra.thrift
	rm -rf ./src/cassandra
	thrift --gen go:package_prefix=github.com/bogdanovich/gossie/src/ -out ./src ./cassandra.thrift
	sed -E -i.orig 's#git.apache.org/thrift.git/lib/go/thrift#github.com/bogdanovich/go-thrift/thrift#' src/cassandra/*.go src/cassandra/cassandra-remote/*.go
	rm src/cassandra/*.orig src/cassandra/cassandra-remote/*.orig
