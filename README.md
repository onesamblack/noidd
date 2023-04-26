# Noid.d

Noid.d is designed to prevent malevolent actors (internal/external) from altering files on your fs - and to quickly notify you if they do.

While external threats can be mitigated with strong auth and firewalls, few tools exist to combat insider threats - people with access to your cloud, rogue developers in your comoany or organization and people with privileged access to your machines.

This could help you:
1) Stop someone from installing a kernel level rootkit
2) Stop a "team" member from installing a backdoor which modifies your network configuration(s) after you just spent 20 hours rebuilding 10 instances
3) Catch someone while they're outright deleting sh#t from the fs

It provides one primary component

- File system integrity: checks for unplanned changes to system binaries, configuration files or other important system files


## Design

Noidd (in its current state) runs as a cron-able task

On first run, a checksum for all files (in your configurable watchlists) is created and stored in a leveldb instance.

On subsequent runs, a checksum is created and checked against the original, if changed, a notification is sent to one or more of your configurable sources.

## Wishlists/Todo

Some integration with inotify would make sense (similar to Facebook's watchman), but with much less confusion.

## Usage

- Install using pip

```
pip3 install noidd
```

- Create a configuration file, the noid.d system configuration file lives in `/etc/noid.d/config.yml`


**Note**: as of 2023-02-14, only twilio is supported

```yml
---
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
 - amazon_sns: [not implemented]
 - onesignal: [not implemented]
```

Add it to a crontab or other scheduler

```crontab
* */2 * * * noidd [opts]
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
