# Project: kigeon

## Overview

Kigeon is a tool for sending Kubernetes events to configurable receivers. It
provides a filtering system to define which events to send. The source namespace
of events can be defined by setting Kubernetes annotations on these namespaces.
Kigeon tries to not send Kubernetes events again, once it was restarted.

Built with golang. 

## Tech Stack

- **Backend:** Golang
- **Database to store already send events:** nats.io KVS
- **Testing:** standard golang tests

## Project Structure

```
cmd/
├── kigeon/       # contains the main.go file for kigeon
pkg/
├── config/       # contains configuration related files
├── eventpusher/  # component which watches for new events and sends them to the queue
├── eventqueue/   # nats.io queue which stores events and distributes them to eventsenders
├── eventsender/  # components which send the events to various receiving systems (like Loki for example)
├── filter/       # helpers to define filter methods
```
