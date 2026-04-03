# rttys-cli

A command-line tool for managing remote devices via the rttys HTTP API.

## Build

```bash
cd cli
go build -o rttys-cli .
```

## Usage

```
rttys-cli --server <URL> --password <PWD> <command>
```

### Global Flags

| Flag | Short | Description |
|------|-------|-------------|
| `--server` | `-s` | rttys server URL (e.g. `http://admin.andless.tech`) |
| `--password` | `-p` | rttys web login password |

### Commands

#### `devices` — List online devices

```bash
rttys-cli -s http://admin.andless.tech -p 'password' devices

# Filter by group
rttys-cli -s http://admin.andless.tech -p 'password' devices --group mygroup
```

Output:
```
ID      GROUP  IP         DESCRIPTION
my-mac         10.42.0.1  MacBook Local
```

#### `exec` — Execute command on a device

```bash
rttys-cli -s http://admin.andless.tech -p 'password' exec --id my-mac --cmd "ls /tmp"
```

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--id` | | (required) | Device ID |
| `--cmd` | `-c` | (required) | Command to execute |
| `--user` | | `root` | Login username on the device |
| `--group` | `-g` | | Device group |
| `--wait` | | `30` | Timeout in seconds (0 = fire and forget) |

stdout and stderr are automatically decoded from base64. The exit code mirrors the remote command's exit code.

#### `groups` — List device groups

```bash
rttys-cli -s http://admin.andless.tech -p 'password' groups
```
