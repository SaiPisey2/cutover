# Build from the repo root: docker build -f test/e2e/demo/Dockerfile -t cutover-demo:e2e .
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY test/e2e/demo ./test/e2e/demo
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/demo ./test/e2e/demo
RUN mkdir /out/run-demo

FROM scratch
COPY --from=build /out/demo /demo
COPY --from=build /out/run-demo /run/demo
ENTRYPOINT ["/demo"]
