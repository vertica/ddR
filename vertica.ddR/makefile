TPARTY := $(SOURCE)/../third-party
R_HOME  := $(shell $(TPARTY)/install/bin/R RHOME)
R_LIB := $(R_HOME)/library

RCPPLIBS := $(shell echo 'Rcpp:::LdFlags()' | $(R_HOME)/bin/R --vanilla --slave)
RINSIDELIBS := $(shell echo 'RInside:::LdFlags()'  | $(R_HOME)/bin/R --vanilla --slave)
RCPPINCLUDES := $(shell echo 'Rcpp:::CxxFlags()' | $(R_HOME)/bin/R --vanilla --slave)
RINSIDEINCLUDES := $(shell echo 'RInside:::CxxFlags()'  | $(R_HOME)/bin/R --vanilla --slave)

export PKG_CXXFLAGS = $(RCPPINCLUDES) $(RINSIDEINCLUDES)
export PKG_LIBS = $(RCPPLIBS) $(RINSIDELIBS)


all: install

clean:
	rm -rf *~
	rm -rf *.o
	rm -rf *.so

install: 
	$(TPARTY)/install/bin/R CMD INSTALL -l $(R_LIB) vertica.ddR
	R CMD INSTALL vertica.ddR
	$(VSQL) -f install.sql

uninstall:
	$(VSQL) -f uninstall.sql

test: install
	Rscript test.R