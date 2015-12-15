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

Summary:	EsgynDB Manager
Name:		%{name}
Version:	%{version}
Release:	%{release}
AutoReqProv:	no
License:	Apache version 2.0 -  http://www.apache.org/licenses/LICENSE-2.0
Group:		Applications/Databases
Source0:    mgblty-tools-%{version}.tar.gz
Source1:    dbmgr-%{version}.tar.gz
BuildArch:	%{_arch}
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}
Vendor:		Esgyn Corp.
URL:        http://www.esgyn.com

Requires: gnuplot
Requires: trafodion


%define _binary_filedigest_algorithm 1
%define _source_filedigest_algorithm 1
%define _binary_payload w9.gzdio
%define _source_payload w9.gzdio

%description
EsgynDB Manager is a web based enterprise management tool for EsgynDB. DB Manager allows the user to monitor the health and status of EsgynDB services and workloads.

%prep
%setup -b 0 -b 1 -n %{name}-%{version}-%{release} -c %{name}-%{version}-%{release}

%pre

%post

%preun

%postun

%build
# don't build debug info package
%define debug_package:

%install
mkdir -p %{buildroot}/home/trafodion/%{name}-%{version}
cp -rf * %{buildroot}/home/trafodion/%{name}-%{version}

%clean
/bin/rm -rf %{buildroot}

%files
%defattr(-,trafodion,trafodion)
/home/trafodion/%{name}-%{version}

%changelog
* Thu Dec 3 2015 Eason Zhang
- ver 1.0
