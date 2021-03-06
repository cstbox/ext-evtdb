# CSTBox framework
#
# Makefile for building the Debian distribution package containing the
# event database extension module.
#
# author = Eric PASCUAL - CSTB (eric.pascual@cstb.fr)

# name of the CSTBox module
MODULE_NAME=ext-evtdb

include $(CSTBOX_DEVEL_HOME)/lib/makefile-dist.mk

make_extra_dirs:
# runtime data storage
	mkdir -p $(BUILD_DIR)/var/db/cstbox

copy_files: \
	copy_bin_files \
	copy_python_files \
	copy_init_scripts

