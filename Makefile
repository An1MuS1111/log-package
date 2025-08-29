## tidy: Tidy go.mod and format .go files
.PHONY: tidy
tidy:
	go mod tidy -v
	go fmt ./...

## test: Run tests
.PHONY: test
test:
	go test -v ./...

## build: Build the cmd/log-package application
.PHONY: build
build:
	go build -o ./bin/log-package ./cmd/log-package

## run: Run the compiled application
.PHONY: run
run: build
	./bin/log-package

## client: Run the client application
.PHONY: client
client:
	go run ./client/main.go

## run/live: Run the app with live reload using Air
.PHONY: run/live
run/live:
	air \
		--build.cmd "make build" \
		--build.bin "./bin/log-package" \
		--build.delay "100" \
		--build.exclude_dir "" \
		--build.include_ext "go,tpl,tmpl,html,css,scss,js,ts,sql,jpeg,jpg,gif,png,bmp,svg,webp,ico" \
		--misc.clean_on_exit "true"

## clean: Remove binary files
.PHONY: clean
clean:
	rm -f ./bin/log-package

## install-deps: Install development dependencies
.PHONY: install-deps
install-deps:
	go install github.com/cosmtrek/air@latest

## docker-build: Build the Docker image
.PHONY: docker-build
docker-build:
	docker build -t log-package:latest .

## docker-run: Run the Docker container
.PHONY: docker-run
docker-run: docker-build
	docker run -p 8080:8080 log-package:latest

## lint: Run golangci-lint
.PHONY: lint
lint:
	golangci-lint run ./...



.PHONY: compile
compile: 
	protoc api/v1/*.proto \
	--go_out=. \
	--go_opt=paths=source_relative \
	--proto_path=.