FROM golang:1.24-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/token_issue_stat_task .

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /out/token_issue_stat_task /app/token_issue_stat_task
COPY config.yaml /app/config.yaml
ENTRYPOINT ["/app/token_issue_stat_task", "-config", "/app/config.yaml"]
