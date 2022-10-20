all:
	rm -rf test.db
	go test
	cd merge && go test
