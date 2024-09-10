# 定义编译器
CXX = g++

# 定义编译选项，-g 用于调试
CXXFLAGS = -g

# 定义链接选项
LDFLAGS = -lrdmacm -libverbs -lpthread

# 定义要编译的源文件
SOURCES = server.cc

# 定义生成的目标文件
OBJECTS = server.o

# 定义最终生成的可执行文件
TARGET = main

# 默认目标
all: $(TARGET)

# 链接目标
$(TARGET): $(OBJECTS)
	$(CXX) $(CXXFLAGS) $(OBJECTS) -o $@ $(LDFLAGS)

# 编译源文件
%.o: %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@

# 清理编译生成的文件
clean:
	rm -f $(TARGET) $(OBJECTS)

# 伪目标，防止与同名文件冲突
.PHONY: all clean
