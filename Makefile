# Top level makefile, the real shit is at src/Makefile
# 这是顶层的makefile，真正的在src/Makefile这一大坨里面

default: all

# $(MAKE) $@：递归调用子目录的 Makefile，并传递当前目标名
.DEFAULT:
	cd src && $(MAKE) $@

install:
	cd src && $(MAKE) $@

.PHONY: install
