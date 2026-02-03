#!/usr/bin/env bash
set -euo pipefail

if ! command -v tailscale >/dev/null 2>&1; then
  exit 0
fi

if ! pgrep -x tailscaled >/dev/null 2>&1; then
  sudo tailscaled --state=/var/lib/tailscale/tailscaled.state &
fi

accept_routes_arg=()
if [[ "${TAILSCALE_ACCEPT_ROUTES:-true}" == "true" ]]; then
  accept_routes_arg+=("--accept-routes")
fi

if tailscale status >/dev/null 2>&1; then
  if [[ ${#accept_routes_arg[@]} -gt 0 ]]; then
    sudo tailscale up "${accept_routes_arg[@]}" >/dev/null
  fi
  exit 0
fi

if [[ -n "${TAILSCALE_AUTHKEY:-}" ]]; then
  sudo tailscale up --authkey "${TAILSCALE_AUTHKEY}" --hostname "codespace-${CODESPACE_NAME:-dev}" "${accept_routes_arg[@]}" >/dev/null
fi
