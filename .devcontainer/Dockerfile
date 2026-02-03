FROM mcr.microsoft.com/devcontainers/python:3.11

RUN apt-get update \
    && apt-get install -y curl ca-certificates gnupg iputils-ping telnet git-lfs socat \
    && curl -fsSL https://pkgs.tailscale.com/stable/ubuntu/noble.noarmor.gpg \
        | tee /usr/share/keyrings/tailscale-archive-keyring.gpg >/dev/null \
    && curl -fsSL https://pkgs.tailscale.com/stable/ubuntu/noble.tailscale-keyring.list \
        | tee /etc/apt/sources.list.d/tailscale.list >/dev/null \
    && apt-get update \
    && apt-get install -y tailscale \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
