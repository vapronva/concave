FROM docker.io/library/golang:1.26.4-alpine AS build

WORKDIR /src

RUN sed --in-place 's!https://dl-cdn.alpinelinux.org/alpine!https://linux.sex/dl-cdn.alpinelinux.org!g' /etc/apk/repositories || true && \
    apk --verbose update && \
    apk --no-cache --verbose upgrade --available && \
    apk add --no-cache git ca-certificates tzdata && \
    update-ca-certificates

COPY go.mod ./

RUN go mod download

COPY main.go ./

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath -a -o /usher . && \
    chmod +x /usher

FROM docker.io/library/alpine:3

RUN sed --in-place 's!https://dl-cdn.alpinelinux.org/alpine!https://linux.sex/dl-cdn.alpinelinux.org!g' /etc/apk/repositories || true && \
    apk --verbose update && \
    apk --no-cache --verbose upgrade --available && \
    apk add --no-cache ca-certificates tzdata && \
    update-ca-certificates && \
    apk cache clean

COPY --from=build /usher /usher

EXPOSE 8080

USER 65532:65532

ENTRYPOINT ["/usher"]
