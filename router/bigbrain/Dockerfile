FROM docker.io/library/golang:1.26.4-alpine AS build

WORKDIR /src

RUN sed --in-place 's!https://dl-cdn.alpinelinux.org/alpine!https://linux.sex/dl-cdn.alpinelinux.org!g' /etc/apk/repositories || true && \
    apk --verbose update && \
    apk --no-cache --verbose upgrade --available && \
    apk add --no-cache git ca-certificates tzdata && \
    update-ca-certificates && \
    apk cache clean

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath -a -o /bigbrain . && \
    chmod +x /bigbrain

FROM docker.io/library/alpine:3

RUN sed --in-place 's!https://dl-cdn.alpinelinux.org/alpine!https://linux.sex/dl-cdn.alpinelinux.org!g' /etc/apk/repositories || true && \
    apk --verbose update && \
    apk --no-cache --verbose upgrade --available && \
    apk add --no-cache ca-certificates tzdata && \
    update-ca-certificates && \
    apk cache clean

COPY --from=build /bigbrain /bigbrain

EXPOSE 8081

USER 65532:65532

HEALTHCHECK --interval=10s --timeout=3s --retries=3 CMD wget --quiet --spider http://127.0.0.1:8081/healthz || exit 1

ENTRYPOINT ["/bigbrain"]
