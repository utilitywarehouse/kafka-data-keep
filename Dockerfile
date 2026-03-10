FROM golang:1.25.8-alpine AS build

RUN --mount=type=cache,target=/var/cache/apk \
    apk add git

ARG GIT_SHA

# Setup private repository access. Using Git helpers. See https://git-scm.com/docs/gitcredentials#_custom_helpers
ARG GITHUB_USER
RUN git config --global \
    'credential.https://github.com/utilitywarehouse.helper' \
    '!f() { [ "$1" = "get" ] && echo -e "username=$GITHUB_USER\npassword=$(cat /run/secrets/github_token)"; }; f'

WORKDIR /build

ENV GOPRIVATE=github.com/utilitywarehouse/*
ENV CGO_ENABLED=0

# Download & cache dependencies
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=secret,id=github_token \
    --mount=type=bind,source=go.sum,target=go.sum \
    --mount=type=bind,source=go.mod,target=go.mod\
    go mod download

### Build app & inject build time properties
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=secret,id=github_token \
    --mount=type=bind,target=. \
    go build -o /kafka-data-keep -ldflags "-s -w -X 'main.gitSHA=${GIT_SHA}' -X 'main.buildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)'" ./cmd/main.go

FROM alpine:3.23

RUN apk add --no-cache ca-certificates tzdata && \
    update-ca-certificates

COPY --from=build /kafka-data-keep /kafka-data-keep

# Setup non root user
RUN addgroup --system --gid 1000 svc && \
    adduser --system --disabled-password --no-create-home --ingroup svc --uid 1000 svc && \
    chown -R svc:svc /kafka-data-keep

USER svc

ENTRYPOINT [ "/kafka-data-keep" ]
