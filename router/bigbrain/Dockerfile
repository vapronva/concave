ARG TOOLS_IMAGE=docker.horse/ci/on-alpine/tools:1

FROM $TOOLS_IMAGE AS tools

FROM docker.io/library/golang:1.26.6-alpine3.24 AS build

WORKDIR /src

ARG MIRROR_ALPINE_URL=""

ARG MIRROR_ALPINE_FALLBACK_URL=""

ARG MIRROR_GO_URL=""

COPY --from=tools /usr/local/bin/pkg-base-setup /usr/local/bin/

RUN pkg-base-setup && \
    apk add --no-cache git

COPY go.mod go.sum ./

RUN if [ -n "$MIRROR_GO_URL" ]; then go env -w GOPROXY="${MIRROR_GO_URL%/}|https://proxy.golang.org,direct"; fi

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath -o /bigbrain . && \
    chmod +x /bigbrain

FROM docker.io/library/alpine:3.24

ARG MIRROR_ALPINE_URL=""

ARG MIRROR_ALPINE_FALLBACK_URL=""

COPY --from=tools /usr/local/bin/pkg-base-setup /usr/local/bin/

RUN pkg-base-setup

COPY --from=build /bigbrain /bigbrain

EXPOSE 8081

EXPOSE 6443

USER 65532:65532

HEALTHCHECK --interval=10s --timeout=3s --retries=3 CMD wget --quiet --spider http://127.0.0.1:8081/healthz || exit 1

ENTRYPOINT ["/bigbrain"]
