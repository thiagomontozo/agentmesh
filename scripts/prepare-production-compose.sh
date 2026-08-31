#!/bin/sh
set -eu

usage() {
  echo "usage: $0 TLS_CERTIFICATE TLS_PRIVATE_KEY [SECRETS_DIR]" >&2
  exit 2
}

[ "$#" -ge 2 ] && [ "$#" -le 3 ] || usage
[ -f "$1" ] || { echo "TLS certificate not found: $1" >&2; exit 1; }
[ -f "$2" ] || { echo "TLS private key not found: $2" >&2; exit 1; }
command -v openssl >/dev/null 2>&1 || { echo "openssl is required" >&2; exit 1; }

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_dir=$(CDPATH= cd -- "$script_dir/.." && pwd)
secrets_dir=${3:-"$project_dir/secrets/agentmesh"}
mkdir -p "$secrets_dir"
chmod 700 "$secrets_dir"
umask 077

generate_secret() {
  target="$secrets_dir/$1"
  if [ -e "$target" ]; then
    [ -s "$target" ] || { echo "refusing empty existing secret: $target" >&2; exit 1; }
    echo "preserved $target"
    return
  fi
  openssl rand -hex 32 > "$target"
  echo "created $target"
}

generate_secret postgres_password
generate_secret nats_token
generate_secret redis_password
generate_secret dashboard_api_token
generate_secret automation_api_token
generate_secret outbound_agent_token

generate_secret dashboard_basic_auth_password
if [ -e "$secrets_dir/dashboard.htpasswd" ]; then
  [ -s "$secrets_dir/dashboard.htpasswd" ] || { echo "refusing empty existing secret: $secrets_dir/dashboard.htpasswd" >&2; exit 1; }
  echo "preserved $secrets_dir/dashboard.htpasswd"
else
  {
    printf 'agentmesh:'
    openssl passwd -apr1 -stdin < "$secrets_dir/dashboard_basic_auth_password"
  } > "$secrets_dir/dashboard.htpasswd"
  echo "created $secrets_dir/dashboard.htpasswd"
fi

for pair in "$1:tls.crt" "$2:tls.key"; do
  source_file=${pair%:*}
  target_name=${pair##*:}
  target_file="$secrets_dir/$target_name"
  if [ -e "$target_file" ]; then
    echo "preserved $target_file"
  else
    cp "$source_file" "$target_file"
    echo "created $target_file"
  fi
done
# Compose file-backed secrets retain host permissions. The directory remains
# owner-only while read permission lets the deliberately non-root containers
# consume only the individual files granted to each service.
chmod 444 "$secrets_dir"/*

echo
echo "Secrets prepared in $secrets_dir."
echo "Read the automation token with: cat '$secrets_dir/automation_api_token'"
echo "Dashboard Basic Auth username: agentmesh"
echo "Read its password with: cat '$secrets_dir/dashboard_basic_auth_password'"
echo "Do not commit this directory or print its contents in logs."
