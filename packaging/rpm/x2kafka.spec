Version: %{__version}
Release: %{__release}%{?dist}
Name: x2kafka
License: GNU AGPLv3
URL: https://github.com/redBorder/x2kafka
Source0: %{name}-%{version}.tar.gz

BuildRequires: golang

Summary: Convert data events from Unix sockets, FIFOs, Syslog or HTTP to Kafka
Group: Development/Libraries/C and C++
Requires(pre): shadow-utils

%description
%{summary}

%prep
%setup -qn %{name}-%{version}

%build
go build -ldflags "-X main.version=%{version} -X main.revision=%{release}" -o x2kafka ./cmd/x2kafka

%install
mkdir -p %{buildroot}/usr/bin
install -p -m 755 x2kafka %{buildroot}/usr/bin/x2kafka
mkdir -p %{buildroot}/etc/%{name}
install -D -m 644 resources/systemd/x2kafka.service %{buildroot}/usr/lib/systemd/system/%{name}.service
install -D -m 644 configs_example/x2kafka_config.json %{buildroot}/etc/%{name}/config.json

%clean
rm -rf %{buildroot}

%pre
getent group %{name} >/dev/null || groupadd -r %{name}
getent passwd %{name} >/dev/null || \
    useradd -r -g %{name} -d / -s /sbin/nologin \
    -c "User of %{name} service" %{name}
exit 0

%post -p /sbin/ldconfig
%postun -p /sbin/ldconfig

%files
%defattr(755,root,root)
/usr/bin/x2kafka
%defattr(644,root,root)
%dir /etc/%{name}
%config(noreplace) /etc/%{name}/config.json
/usr/lib/systemd/system/%{name}.service

%changelog
* Sat Sep 26 2026 David Vanhoucke <dvanhoucke@redborder.com> - 1.0.0-1
- Initial release of x2kafka Go daemon
