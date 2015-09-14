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
Source0:        %{name}_server-%{version}.tgz
Source1:        rest-%{version}.tar.gz
Source2:        dcs-%{version}.tar.gz
BuildArch:	%{_arch}
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}
Vendor:		Esgyn Corp.
URL:            http://www.esgyn.com

Requires: audit-libs
Requires: coreutils
Requires: coreutils-libs
Requires: cracklib
Requires: cracklib-dicts
Requires: cyrus-sasl-lib
Requires: db4
Requires: expect
Requires: gawk
Requires: glib2
Requires: glibc
Requires: gmp
Requires: groff
Requires: gzip
Requires: keyutils-libs
Requires: krb5-libs
Requires: libcom_err
Requires: libgcc
Requires: libgenders
Requires: libselinux
Requires: libstdc++
Requires: libtool-ltdl
Requires: libxml2
Requires: ncurses
Requires: ncurses-base
Requires: ncurses-libs
Requires: nspr
Requires: nss
Requires: nss-softokn-freebl
Requires: nss-util
Requires: openldap
Requires: openssl
Requires: pam
Requires: pcre
Requires: perl
Requires: perl-DBD-SQLite
Requires: perl-DBI
Requires: perl-libs
Requires: perl-Module-Pluggable
Requires: perl-Params-Validate
Requires: perl-Pod-Escapes
Requires: perl-Pod-Simple
Requires: perl-Time-HiRes
Requires: perl-version
Requires: protobuf
Requires: python
Requires: readline
Requires: setup
Requires: sqlite
Requires: tcl
Requires: tzdata
Requires: unixODBC
Requires: xerces-c
Requires: zlib


%define _binary_filedigest_algorithm 1
%define _source_filedigest_algorithm 1
%define _binary_payload w9.gzdio
%define _source_payload w9.gzdio

%description
EsgynDB, based on Apache Trafodion, delivers 100x better price/performance for Operational Big Data combining the power of transactional SQL and Apache HBase with the elastic scalability of Hadoop.

%prep
%setup -b 0 -b 1 -b 2 -n %{name}-%{version}-%{release} -c %{name}-%{version}-%{release}

%pre

%post

%preun

%postun

%build
# don't build debug info package
%define debug_package:

%install
mkdir -p %{buildroot}/opt/trafodion/%{name}-%{version}
cp -rf * %{buildroot}/opt/trafodion/%{name}-%{version}

%clean
/bin/rm -rf %{buildroot}

%files
%defattr(-,root,root)
/opt/trafodion/%{name}-%{version}

%changelog
* Wed Sep 6 2015 Eason Zhang
- ver 1.1
* Wed Jun 3 2015 Lars Fredriksen
- ver 1.0
