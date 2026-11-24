# token_issue_stat_task —— 一次性扫描任务（扫完退出），构建/打包/发布
#
#   make            编译 linux amd64 静态二进制
#   make docker     构建镜像
#   make docker-push 推镜像
#   make apply-cm   更新 ConfigMap
#   make apply      发布/重跑 Job（每次会先删除同名 Job）
#   make logs       看 Job 日志
#   make tag        自动升版本号（需 auto_tag.sh）

TARGET  = token-issue-stat-task
BINARY  = token_issue_stat_task
VERSION = $(shell git describe --always --long --dirty --tags)
IMAGE   = registry-intl.cn-hongkong.aliyuncs.com/tokenpocketgb/$(TARGET):$(VERSION)

SOURCES = $(shell find . -name '*.go' -not -path './vendor/*')

.PHONY: all build clean docker docker-push apply apply-cm delete logs tag

all: build

build: $(BINARY)

$(BINARY): $(SOURCES) go.mod go.sum
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "-s -w" -o $@ .
	@echo "built: $@ ($(VERSION))"

clean:
	rm -f $(BINARY)

docker: $(BINARY)
	docker build -f ./Dockerfile \
		-t $(IMAGE) \
		--build-arg TARGET=$(TARGET) \
		--build-arg BINARY=$(BINARY) .

docker-push: docker
	docker push $(IMAGE)

apply-cm:
	kubectl apply -f k8s/cm-prod.yaml
	@echo "configmap applied"

# Job 是一次性任务，同名 Job 不能重复 apply，先删再建
apply:
	sed 's/TPIMAGE_VERSION/$(VERSION)/g' k8s/job-prod.yaml > k8s/tmp.job.yaml
	-kubectl delete job $(TARGET) -n tp-bserver --ignore-not-found
	kubectl apply -f k8s/tmp.job.yaml
	rm -f k8s/tmp.job.yaml
	@echo "job applied, version=$(VERSION)"

delete:
	-kubectl delete job $(TARGET) -n tp-bserver --ignore-not-found

logs:
	kubectl -n tp-bserver logs -f job/$(TARGET)

.PHONY: status
status:
	kubectl -n tp-bserver get job,pod | grep -E "NAME|$(TARGET)"

.PHONY: tag
tag:
	@sh ./auto_tag.sh 3 "update"
