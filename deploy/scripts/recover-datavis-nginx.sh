#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/home/ec2-user/cTrade"
MANAGED_SITE_SOURCE="${APP_DIR}/deploy/nginx/datavis.au.conf"
ACTIVE_SITE_CONF="/etc/nginx/conf.d/datavis.au.conf"
MAIN_NGINX_CONF="/etc/nginx/nginx.conf"

log() {
  printf '[recover-datavis-nginx] %s\n' "$1"
}

backup_file() {
  local target_path="$1"
  if [[ -f "$target_path" ]]; then
    sudo cp -a "$target_path" "${target_path}.bak.$(date +%Y%m%d%H%M%S)"
  fi
}

audit_nginx_state() {
  log "Auditing nginx files for datavis.au and stale upstreams"
  sudo grep -RInE 'server_name .*datavis\.au|127\.0\.0\.1:8501|/home/ec2-user/cTrade/frontend|127\.0\.0\.1:8000' \
    /etc/nginx/nginx.conf /etc/nginx/conf.d/*.conf 2>/dev/null || true
}

strip_datavis_server_blocks_from_main_conf() {
  if ! sudo grep -qE 'server_name .*datavis\.au' "$MAIN_NGINX_CONF"; then
    log "No datavis.au server block found in ${MAIN_NGINX_CONF}"
    return
  fi

  log "Removing stale datavis.au server blocks from ${MAIN_NGINX_CONF}"
  backup_file "$MAIN_NGINX_CONF"

  local temp_path
  temp_path="$(mktemp)"
  sudo awk '
    function flush_block() {
      if (!block_has_datavis) {
        printf "%s", block
      }
      block = ""
      block_has_datavis = 0
    }
    {
      line = $0 "\n"
      if (!in_server) {
        if ($0 ~ /^[[:space:]]*server[[:space:]]*\{([[:space:]]*#.*)?[[:space:]]*$/) {
          in_server = 1
          block = ""
          block_has_datavis = 0
          depth = 0
        } else {
          printf "%s", line
          next
        }
      }

      block = block line
      if ($0 ~ /server_name/ && $0 ~ /(^|[[:space:]])(www\.)?datavis\.au([[:space:];]|$)/) {
        block_has_datavis = 1
      }

      depth_line = $0
      open_count = gsub(/\{/, "{", depth_line)
      depth_line = $0
      close_count = gsub(/\}/, "}", depth_line)
      depth += open_count - close_count

      if (depth == 0) {
        flush_block()
        in_server = 0
      }
    }
  ' "$MAIN_NGINX_CONF" > "$temp_path"
  sudo install -m 0644 "$temp_path" "$MAIN_NGINX_CONF"
  rm -f "$temp_path"
}

install_managed_site_conf() {
  if [[ ! -f "$MANAGED_SITE_SOURCE" ]]; then
    log "Missing managed nginx config: ${MANAGED_SITE_SOURCE}"
    exit 1
  fi

  log "Installing managed site config to ${ACTIVE_SITE_CONF}"
  backup_file "$ACTIVE_SITE_CONF"
  sudo install -m 0644 "$MANAGED_SITE_SOURCE" "$ACTIVE_SITE_CONF"
}

validate_and_reload_nginx() {
  log "Validating nginx config"
  sudo nginx -t

  log "Reloading nginx"
  sudo systemctl reload nginx
}

main() {
  audit_nginx_state
  strip_datavis_server_blocks_from_main_conf
  install_managed_site_conf
  audit_nginx_state
  validate_and_reload_nginx
}

main "$@"
