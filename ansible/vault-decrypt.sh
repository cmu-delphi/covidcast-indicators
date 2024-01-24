#!/bin/sh

# Create backup directory and copy vault_plain.yaml if it exists.
if [ -f vault_plain.yaml ]; then
    echo "Creating backup of vault_plain.yaml..."
    mkdir -p vault_backup
    cp -Rvp vault_plain.yaml \
    "vault_backup/vault_plain.yaml.backup-$(date -u +%Y-%m-%d_T%H-%M-%S_%Z)"
fi

# Create a new/overwrite vault_plain.yaml using vault.yaml as the source.
ansible-vault decrypt --output vault_plain.yaml vault.yaml
