# Makefile for building the Debian distribution package containing the
# core part of the CSTBox runtime.

# author = Eric PASCUAL - CSTB (eric.pascual@cstb.fr)
# copyright = Copyright (c) 2013 CSTB
# vcsid = $Id$
# version = 1.0.0

# name of the CSTBox module
MODULE_NAME=core

include $(CSTBOX_DEVEL_HOME)/devel/makefile-dist.mk

UDEV_RULES_DIR=$(CSTBOX_INSTALL_DIR)/lib/udev-rules.d

make_extra_dirs:
# runtime data storage
	mkdir -p $(BUILD_DIR)/var/log/cstbox
	mkdir -p $(BUILD_DIR)/var/db/cstbox
	
# device support extensions metadata storage
	mkdir -p $(BUILD_DIR)/$(CSTBOX_INSTALL_DIR)/lib/python/pycstbox/devcfg.d
	
# udev rules files
	mkdir -p $(UDEV_RULES_DIR)


copy_files: \
	copy_bin_files \
	copy_python_files \
	copy_init_scripts\
	copy_init_shared_files \
	copy_etc_files
	@echo '------ copying package specific files...'
	$(RSYNC) \
	    --filter "-s_*/.*" \
	    $(ETC_FROM)/profile.d/ $(BUILD_DIR)/$(PROFILE_D_INSTALL_DIR)

	$(RSYNC) \
	    --filter "-s_*/.*" \
	    $(ETC_FROM)/logrotate.d/ $(BUILD_DIR)/$(LOGROTATE_D_INSTALL_DIR)

	$(RSYNC) \
	    --filter "-s_*/.*" \
	    $(LIB_FROM)/udev.d/ $(BUILD_DIR)/$(UDEV_RULES_DIR)


