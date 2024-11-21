# 定义二进制文件名称
BINARY_NAME=quai-block-monitor

# 默认目标：构建二进制文件
all: build

# 构建二进制文件
build:
	go build -o $(BINARY_NAME) main.go

# 清理生成的二进制文件
clean:
	rm -f $(BINARY_NAME)

# 伪目标（不生成实际文件）
.PHONY: all build clean run