#BLD_MONARCH_TRX_JARNAME     = esgyn_monarch-trx-$(TRAFODION_VER).jar
BLD_MONARCH_TRX_JARNAME     = $(ESGYN_MONARCH_TRX_JAR)

VFILE			    =esgyn_monarch-trx.jar.versions
GENVERS			    =../hbase-trx/genvers

all: build_all

monarch_trx: build_chk_monarch
	set -o pipefail && $(MAVEN) -f pom.xml package | tee build_trx.log
	mkdir -p $(MY_SQROOT)/export/lib
	cp -pf target/$(BLD_MONARCH_TRX_JARNAME) $(MY_SQROOT)/export/lib
	$(RM) $(VFILE)

build_all: monarch_trx

build_chk_monarch:
	$(GENVERS) > $(VFILE)
	@if [ $(GENVERS) -nt target/$(BLD_MONARCH_TRX_JARNAME) ]; then echo "update manifest"; $(RM) -f target/$(BLD_MONARCH_TRX_JARNAME); fi
	@if [ $(MY_SQROOT)/export/include/SCMBuildStr.h -nt target/$(BLD_MONARCH_TRX_JARNAME) ]; then echo "update manifest"; $(RM) -f target/$(BLD_MONARCH_TRX_JARNAME); fi

clean:
	$(RM) $(MY_SQROOT)/export/lib/$(BLD_MONARCH_TRX_JARNAME)
	-$(MAVEN) -f pom.xml clean | grep ERROR
	$(RM) build_trx.log
	$(RM) $(VFILE)
