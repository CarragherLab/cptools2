# SSH Keys Best Practice Guide

> **Source:** <https://www.wiki.ed.ac.uk/display/ResearchServices/SSH+keys+best+practice+guide>

If you have any queries about using SSH please get in touch via the IS Helpline.

## Best Practices

### Let SSH Create the `~/.ssh` Directory

Rather than creating it in advance, let the permissions be set by the process. (Note: on Eddie your `~/.ssh` directory already exists.)

If you do not have a `~/.ssh` directory, generate a new key:

```bash
ssh-keygen -o -t ed25519
```

This creates the directory with correct permissions and generates two files:
- Private key: `~/.ssh/id_ed25519` — **never share this**
- Public key: `~/.ssh/id_ed25519.pub`

### Use a Different Key for Each Service

Using different keys means that if one key is compromised, not all services are affected. Specify a custom key name:

```bash
ssh-keygen -o -t rsa -f ~/.ssh/id_rsa_eddie
```

To use a specific key when connecting:

```bash
ssh -i ~/.ssh/id_rsa_eddie <UUN>@eddie.ecdf.ed.ac.uk
```

Or configure it permanently in `~/.ssh/config`:

```
Host eddie.ecdf.ed.ac.uk
    IdentityFile ~/.ssh/id_rsa_eddie
```

### Encrypt Private Keys with a Passphrase

Use a passphrase to encrypt private keys. Apply one retrospectively:

```bash
ssh-keygen -p -f ~/.ssh/id_rsa
```

Keys used for entry points accessible from the public internet **must always** have a passphrase.

### Assume Portable Devices Will Be Lost

- Do not store keys on USB devices unless encrypted.
- University laptops must be encrypted if used with sensitive data.

### Use 4096-bit Keys Where Possible

```bash
ssh-keygen -o -b 4096 -t rsa
```

Note: older systems may not support 4096-bit encryption.

### Only Keep Private Keys on Your Client Host

Never copy private keys to intermediary hosts. If this is a requirement, please contact the IS Helpline.

### Important: Do Not Remove Alces HPC Cluster Keys

> **Do not remove any keys from `authorised_keys` that are called "Alces HPC Cluster Key"** — doing so will break your access to Eddie.
