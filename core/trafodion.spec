# @@@ START COPYRIGHT @@@
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# @@@ END COPYRIGHT @@@

Summary:	EsgynDB: Transactional SQL-on-Hadoop DBMS
Name:		%{name}
Version:	%{version}
Release:	%{release}
AutoReqProv:	no
License:	Apache version 2.0 -  http://www.apache.org/licenses/LICENSE-2.0
Group:		Applications/Databases
Source0:    dcs-%{version}.tar.gz
Source1:    rest-%{version}.tar.gz
Source2:    %{name}_server-%{version}.tgz
Source3:    dbmgr-%{version}.tar.gz
Source4:    mgblty-tools-%{version}.tar.gz
BuildArch:	%{_arch}
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}
Vendor:		Esgyn Corp.
URL:        http://www.esgyn.com


%define _binary_filedigest_algorithm 1
%define _source_filedigest_algorithm 1
%define _binary_payload w9.gzdio
%define _source_payload w9.gzdio

Requires: audit-libs
Requires: coreutils
Requires: cracklib
Requires: expect
Requires: gawk
Requires: glib2
Requires: glibc
Requires: gmp
Requires: groff
Requires: gzip
Requires: keyutils-libs
Requires: libcom_err
Requires: libgcc
Requires: libxml2
Requires: log4cxx
Requires: ncurses
Requires: openssl
Requires: pam
Requires: pcre
Requires: pdsh
Requires: perl
Requires: perl-DBD-SQLite
Requires: perl-DBI
Requires: perl-Module-Pluggable
Requires: perl-Params-Validate
Requires: perl-Pod-Escapes
Requires: perl-Pod-Simple
Requires: perl-Time-HiRes
Requires: perl-version
Requires: python
Requires: readline
Requires: sqlite
Requires: tcl
Requires: unixODBC
Requires: zlib

%description
EsgynDB, based on Apache Trafodion, delivers 100x better price/performance for Operational Big Data combining the power of transactional SQL and Apache HBase with the elastic scalability of Hadoop.

%package -n esgynDB-manager
Summary:	EsgynDB Manager
Requires: gnuplot
Requires: trafodion
%description -n esgynDB-manager
EsgynDB Manager is a web based enterprise management tool for EsgynDB. DB Manager allows the user to monitor the health and status of EsgynDB services and workloads.


%prep
%setup -b 0 -b 1 -b 2 -n %{name}-%{version} -c
%setup -b 3 -b 4 -n esgynDB-manager-%{version} -c


%pre -n trafodion
getent group trafodion > /dev/null || /usr/sbin/groupadd trafodion > /dev/null 2>&1
getent passwd trafodion > /dev/null || /usr/sbin/useradd --shell /bin/bash -m trafodion -g trafodion --home /home/trafodion > /dev/null 2>&1


%build
# don't build debug info package
%define debug_package:

%install
mkdir -p %{buildroot}/home/trafodion/trafodion-%{version}
mkdir -p %{buildroot}/home/trafodion/esgynDB-manager-%{version}
cd %{_builddir}
cp -rf %{name}-%{version}/* %{buildroot}/home/trafodion/trafodion-%{version}
cp -rf esgynDB-manager-%{version}/dbmgr-%{version}/* %{buildroot}/home/trafodion/esgynDB-manager-%{version}
cp -rf esgynDB-manager-%{version}/mgblty/* %{buildroot}/home/trafodion/esgynDB-manager-%{version}

%clean
/bin/rm -rf %{buildroot}

%files
%defattr(-,trafodion,trafodion)
/home/trafodion/%{name}-%{version}

%files -n esgynDB-manager
%defattr(-,trafodion,trafodion)
/home/trafodion/esgynDB-manager-%{version}

%changelog
* Mon Jan 18 2016 Eason Zhang
- ver 1.0
