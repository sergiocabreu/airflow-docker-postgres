#!/bin/bash
set -e

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get upgrade -y
apt-get install -y python3-pip python3-venv python3-dev libpq-dev libssl-dev nano sudo

# Aliases de python/pip (idempotente)
touch ~/.bashrc
grep -qx "alias python=python3" ~/.bashrc || echo "alias python=python3" >> ~/.bashrc
grep -qx "alias pip=pip3" ~/.bashrc || echo "alias pip=pip3" >> ~/.bashrc

echo "Ambiente preparado."
