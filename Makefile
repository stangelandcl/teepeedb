#FLAGS=-gcflags=all=-l=4
FLAGS=
all:
	rm -rf test.db
	cd internal/file && go test $(FLAGS)
	cd internal/merge && go test $(FLAGS)
	go test $(FLAGS)
