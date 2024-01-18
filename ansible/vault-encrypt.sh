#!/bin/sh

# Create backup directory if it doesn't exist.
mkdir -p vault_backup

# Make copy of vault.yaml if it exists.
if [ -f vault.yaml ]; then
    cp -Rvp vault.yaml \
    "vault_backup/vault.yaml.backup-$(date -u +%Y-%m-%d_T%H-%M-%S_%Z)"
fi

# Create a new/overwrite vault.yaml using vault_plain.yaml as the source.
ansible-vault encrypt --output vault.yaml vault_plain.yaml
