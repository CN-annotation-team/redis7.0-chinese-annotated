# Top level makefile, the real shit is at src/Makefile

default: all

# $(MAKE) $@：递归调用子目录的 Makefile，并传递当前目标名
.DEFAULT:
	cd src && $(MAKE) $@

install:
	cd src && $(MAKE) $@

.PHONY: install
