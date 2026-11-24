# 用 make build 预先编译好的静态二进制，镜像里不再编译（构建更快）
FROM alpine:3.15

ARG TARGET=token-issue-stat-task
ARG BINARY=token_issue_stat_task
ARG WORKPATH=/home/tokenbank

ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone \
 && addgroup -g 500 tokenbank \
 && adduser -u 500 -D -h ${WORKPATH} -G tokenbank tokenbank

# WORKDIR 由 docker 以 root 创建，先建目录并授权
RUN mkdir -p ${WORKPATH}/${TARGET}/config \
 && chown -R tokenbank:tokenbank ${WORKPATH}

WORKDIR ${WORKPATH}/${TARGET}

COPY --chown=tokenbank:tokenbank ${BINARY} ./

USER tokenbank

ENTRYPOINT ["./token_issue_stat_task", "-config", "config/config_prod.yml"]
