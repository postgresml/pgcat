#!/bin/bash
#
# Build an Ubuntu deb.
#
script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
deb_dir="/tmp/pgcat-build"
export PACKAGE_VERSION=${1:-"1.1.1"}
if [[ $(arch) == "x86_64" ]]; then
  export ARCH=amd64
else
  export ARCH=arm64
fi

cd "$script_dir/.."
cargo build --release

rm -rf "$deb_dir"
mkdir -p "$deb_dir/DEBIAN"
mkdir -p "$deb_dir/usr/bin"
mkdir -p "$deb_dir/etc/systemd/system"

cp target/release/pgcat "$deb_dir/usr/bin/pgcat"
chmod +x "$deb_dir/usr/bin/pgcat"

cp pgcat.toml "$deb_dir/etc/pgcat.example.toml"
cp pgcat.service "$deb_dir/etc/systemd/system/pgcat.service"

(cat control | envsubst) > "$deb_dir/DEBIAN/control"
cp postinst "$deb_dir/DEBIAN/postinst"
cp postrm "$deb_dir/DEBIAN/postrm"
cp prerm "$deb_dir/DEBIAN/prerm"

chmod +x ${deb_dir}/DEBIAN/post*
chmod +x ${deb_dir}/DEBIAN/pre*

dpkg-deb \
  --root-owner-group \
  -z1 \
  --build "$deb_dir" \
  pgcat-${PACKAGE_VERSION}-ubuntu22.04-${ARCH}.deb
