# Ansible

Ansible is used in the indicators deployment process.

It aids in:

- Setting up staging and production systems to run the indicators.
- Placing templated indicators params files onto staging and production systems.
- Managing secrets.

## Managing secrets with ansible-vault

The deployment process uses [`ansible-vault`](https://docs.ansible.com/ansible/latest/vault_guide/index.html) and a corresponding file of `vault.yaml` to write secrets into template files that are placed onto staging and production systems. `vault.yaml` should always be encrypted.

To work with secrets in this repo you should follow one of these processes:

1. Work with systems administrators to add secrets.

OR

2. Obtain the vault decryption password and use the helper scripts.

- Make sure you are in the repo's `ansible` directory.

  ```shell
  cd $(git rev-parse --show-toplevel)/ansible
  ```

- Use the helper scripts to:

  - Decrypt to `vault_plain.yaml` - Creates a .gitgnored "plain" file for editing. Also a backup directory and backup file if possible.

    ```shell
    bash vault-decrypt.sh
    ```

  - Make your changes in `vault_plain.yaml`

  - Encrypt to a new `vault.yaml` - Creates a new encrypted vault file suitable for committing. Also creates a backup directory and backup file if possible.

    ```shell
    bash vault-encrypt.sh
    ```
