# Noid.d

Noid.d is designed to prevent malevolent actors (internal/external) from altering files on your fs - and to quickly notify you if they do.

While external threats can be mitigated with strong auth and firewalls, few tools exist to combat insider threats - people with access to your cloud, developers on your "team" and people with privileged access to your machines who just don't like you :)

This could help you:
1) Stop someone from installing a kernel level rootkit
2) Stop a "team" member from installing a backdoor which modifies your network configuration(s) after you just spent 20 hours rebuilding 10 instances
3) Catch someone while they're outright deleting sh#t from the fs

It provides one primary components

- File system integrity: checks for unplanned changes to system binaries, configuration files or other important system files


## Design


Similarly, if a file is modified, created or deleted in the directory being watched, an alert is generated. These two processes are meant to catch stealth

If an alert is generated, a notification is sent via a configurable notification source.


## Usage

- Install using pip

```
pip3 install noidd
```

- Create a configuration file, the noid.d system configuration file lives in `/etc/noid.d/config.yml`


**Note**: as of 2023-02-14, only twilio is supported

```yml
---
checksum_period: 3600
watch_directories:
 - ...
watch_files:
 - 
notification_sources:
 - email:
   addresses: []
   template: 'your_notification_template.html'
 - twilio:
   recipient_numbers: []
   twilio_api_key: '...'
   twilio_sid_token: '....'
   twilio_from_number: '....'
 - amazon_sns:
 - onesignal:
```

- start the daemon
```
noidd --start-daemon [opts]
```

Run `noidd --help` for a list of all options


## Requirements

**Leveldb**:
```
apt install leveldb
```


**inotify-tools**:

```
apt install inotify-tools
```





   




