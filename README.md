<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Apache Airflow

[![PyPI version](https://badge.fury.io/py/apache-airflow.svg)](https://badge.fury.io/py/apache-airflow)
[![GitHub Build](https://github.com/apache/airflow/workflows/CI%20Build/badge.svg)](https://github.com/apache/airflow/actions)
[![Coverage Status](https://img.shields.io/codecov/c/github/apache/airflow/main.svg)](https://codecov.io/github/apache/airflow?branch=main)
[![License](https://img.shields.io/:license-Apache%202-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0.txt)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/apache-airflow.svg)](https://pypi.org/project/apache-airflow/)
[![Docker Pulls](https://img.shields.io/docker/pulls/apache/airflow.svg)](https://hub.docker.com/r/apache/airflow)
[![Docker Stars](https://img.shields.io/docker/stars/apache/airflow.svg)](https://hub.docker.com/r/apache/airflow)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/apache-airflow)](https://pypi.org/project/apache-airflow/)
[![Artifact HUB](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/apache-airflow)](https://artifacthub.io/packages/search?repo=apache-airflow)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Twitter Follow](https://img.shields.io/twitter/follow/ApacheAirflow.svg?style=social&label=Follow)](https://twitter.com/ApacheAirflow)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://s.apache.org/airflow-slack)

[Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/) (or simply Airflow) is a platform to programmatically author, schedule, and monitor workflows.

When workflows are defined as code, they become more maintainable, versionable, testable, and collaborative.

Use Airflow to author workflows as directed acyclic graphs (DAGs) of tasks. The Airflow scheduler executes your tasks on an array of workers while following the specified dependencies. Rich command line utilities make performing complex surgeries on DAGs a snap. The rich user interface makes it easy to visualize pipelines running in production, monitor progress, and troubleshoot issues when needed.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of contents**

- [Project Focus](#project-focus)
- [Principles](#principles)
- [Version Life Cycle](#version-life-cycle)
- [Requirements](#requirements)
- [Support for Python and Kubernetes versions](#support-for-python-and-kubernetes-versions)
- [Getting started](#getting-started)
- [Installing from PyPI](#installing-from-pypi)
- [Official source code](#official-source-code)
- [Convenience packages](#convenience-packages)
- [User Interface](#user-interface)
- [Contributing](#contributing)
- [Who uses Apache Airflow?](#who-uses-apache-airflow)
- [Who Maintains Apache Airflow?](#who-maintains-apache-airflow)
- [Can I use the Apache Airflow logo in my presentation?](#can-i-use-the-apache-airflow-logo-in-my-presentation)
- [Airflow merchandise](#airflow-merchandise)
- [Links](#links)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Project Focus

Airflow works best with workflows that are mostly static and slowly changing. When DAG structure is similar from one run to the next, it allows for clarity around unit of work and continuity. Other similar projects include [Luigi](https://github.com/spotify/luigi), [Oozie](https://oozie.apache.org/) and [Azkaban](https://azkaban.github.io/).

Airflow is commonly used to process data, but has the opinion that tasks should ideally be idempotent (i.e. results of the task will be the same, and will not create duplicated data in a destination system), and should not pass large quantities of data from one task to the next (though tasks can pass metadata using Airflow's [Xcom feature](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#xcoms)). For high-volume, data-intensive tasks, a best practice is to delegate to external services that specialize on that type of work.

Airflow is not a streaming solution, but it is often used to process real-time data, pulling data off streams in batches.

## Principles

- **Dynamic**:  Airflow pipelines are configuration as code (Python), allowing for dynamic pipeline generation. This allows for writing code that instantiates pipelines dynamically.
- **Extensible**:  Easily define your own operators, executors and extend the library so that it fits the level of abstraction that suits your environment.
- **Elegant**:  Airflow pipelines are lean and explicit. Parameterizing your scripts is built into the core of Airflow using the powerful **Jinja** templating engine.
- **Scalable**:  Airflow has a modular architecture and uses a message queue to orchestrate an arbitrary number of workers.

## Version Life Cycle

Apache Airflow version life cycle:

| Version | Current Patch/Minor | State           | First Release | Limited Support | EOL/Terminated |
|---------|---------------------|-----------------|---------------|-----------------|----------------|
| 2       | 2.1.0               | Supported       | Dec 17, 2020  | Dec 2021        | TBD            |
| 1.10    | 1.10.15             | Limited Support | Aug 27, 2018  | Dec 17, 2020    | June 2021      |
| 1.9     | 1.9.0               | EOL             | Jan 03, 2018  | Aug 27, 2018    | Aug 2018       |
| 1.8     | 1.8.2               | EOL             | Mar 19, 2017  | Jan 03, 2018    | Jan 2018       |
| 1.7     | 1.7.1.2             | EOL             | Mar 28, 2016  | Mar 19, 2017    | Mar 2017       |

Limited support versions will be supported with security and critical bug fix only.
EOL versions will not get any fixes nor support.
We always recommend that all users run the latest available minor release for whatever major version is in use.
We **highly** recommend upgrading to the latest Airflow major release at the earliest convenient time and before EOL date.

## Requirements

Apache Airflow is tested with:

|                      | Main version (dev)        | Stable version (2.0.2)   | Previous version (1.10.15) |
| -------------------- | ------------------------- | ------------------------ | -------------------------  |
| Python               | 3.6, 3.7, 3.8             | 3.6, 3.7, 3.8            | 2.7, 3.5, 3.6, 3.7, 3.8    |
| Kubernetes           | 1.20, 1.19, 1.18          | 1.20, 1.19, 1.18         | 1.18, 1.17, 1.16           |
| PostgreSQL           | 9.6, 10, 11, 12, 13       | 9.6, 10, 11, 12, 13      | 9.6, 10, 11, 12, 13        |
| MySQL                | 5.7, 8                    | 5.7, 8                   | 5.6, 5.7                   |
| SQLite               | 3.15.0+                   | 3.15.0+                  | 3.15.0+                    |
| MSSQL(Experimental)  | 2017,2019                 |                          |                            |

**Note:** MySQL 5.x versions are unable to or have limitations with
running multiple schedulers -- please see the [Scheduler docs](https://airflow.apache.org/docs/apache-airflow/stable/scheduler.html).
MariaDB is not tested/recommended.

**Note:** SQLite is used in Airflow tests. Do not use it in production. We recommend
using the latest stable version of SQLite for local development.

## Support for Python and Kubernetes versions

As of Airflow 2.0 we agreed to certain rules we follow for Python and Kubernetes support.
They are based on the official release schedule of Python and Kubernetes, nicely summarized in the
[Python Developer's Guide](https://devguide.python.org/#status-of-python-branches) and
[Kubernetes version skew policy](https://kubernetes.io/docs/setup/release/version-skew-policy/).

1. We drop support for Python and Kubernetes versions when they reach EOL. We drop support for those
   EOL versions in main right after EOL date, and it is effectively removed when we release the
   first new MINOR (Or MAJOR if there is no new MINOR version) of Airflow
   For example for Python 3.6 it means that we drop support in main right after 23.12.2021, and the first
   MAJOR or MINOR version of Airflow released after will not have it.

2. The "oldest" supported version of Python/Kubernetes is the default one. "Default" is only meaningful
   in terms of "smoke tests" in CI PRs which are run using this default version and default reference
   image available in DockerHub. Currently ``apache/airflow:latest`` and ``apache/airflow:2.0.2`` images
   are both Python 3.6 images, however the first MINOR/MAJOR release of Airflow release after 23.12.2021 will
   become Python 3.7 images.

3. We support a new version of Python/Kubernetes in main after they are officially released, as soon as we
   make them work in our CI pipeline (which might not be immediate due to dependencies catching up with
   new versions of Python mostly) we release a new images/support in Airflow based on the working CI setup.

### Additional notes on Python version requirements

* Previous version [requires](https://github.com/apache/airflow/issues/8162) at least Python 3.5.3
  when using Python 3

## Getting started

Visit the official Airflow website documentation (latest **stable** release) for help with
[installing Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation.html),
[getting started](https://airflow.apache.org/docs/apache-airflow/stable/start/index.html), or walking
through a more complete [tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html).

> Note: If you're looking for documentation for main branch (latest development branch): you can find it on [s.apache.org/airflow-docs](https://s.apache.org/airflow-docs/).

For more information on Airflow Improvement Proposals (AIPs), visit
the [Airflow Wiki](https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Improvements+Proposals).

Official Docker (container) images for Apache Airflow are described in [IMAGES.rst](IMAGES.rst).

## Installing from PyPI

We publish Apache Airflow as `apache-airflow` package in PyPI. Installing it however might be sometimes tricky
because Airflow is a bit of both a library and application. Libraries usually keep their dependencies open and
applications usually pin them, but we should do neither and both at the same time. We decided to keep
our dependencies as open as possible (in `setup.py`) so users can install different versions of libraries
if needed. This means that from time to time plain `pip install apache-airflow` will not work or will
produce unusable Airflow installation.

In order to have repeatable installation, however, we also keep a set of "known-to-be-working" constraint
files in the orphan `constraints-main`, `constraints-2-0` branches. We keep those "known-to-be-working"
constraints files separately per major/minor Python version.
You can use them as constraint files when installing Airflow from PyPI. Note that you have to specify
correct Airflow tag/version/branch and Python versions in the URL.


1. Installing just Airflow:

NOTE!!!

Only `pip` installation is currently officially supported.

While they are some successes with using other tools like [poetry](https://python-poetry.org) or
[pip-tools](https://pypi.org/project/pip-tools), they do not share the same workflow as
`pip` - especially when it comes to constraint vs. requirements management.
Installing via `Poetry` or `pip-tools` is not currently supported.

If you wish to install airflow using those tools you should use the constraint files and convert
them to appropriate format and workflow that your tool requires.


```bash
pip install apache-airflow==2.0.2 \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.0.2/constraints-3.7.txt"
```

2. Installing with extras (for example postgres,google)

```bash
pip install apache-airflow[postgres,google]==2.0.2 \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.0.2/constraints-3.7.txt"
```

For information on installing provider packages check
[providers](http://airflow.apache.org/docs/apache-airflow-providers/index.html).

## Official source code

Apache Airflow is an [Apache Software Foundation](https://www.apache.org) (ASF) project,
and our official source code releases:

- Follow the [ASF Release Policy](https://www.apache.org/legal/release-policy.html)
- Can be downloaded from [the ASF Distribution Directory](https://downloads.apache.org/airflow)
- Are cryptographically signed by the release manager
- Are officially voted on by the PMC members during the
  [Release Approval Process](https://www.apache.org/legal/release-policy.html#release-approval)

Following the ASF rules, the source packages released must be sufficient for a user to build and test the
release provided they have access to the appropriate platform and tools.

## Convenience packages

There are other ways of installing and using Airflow. Those are "convenience" methods - they are
not "official releases" as stated by the `ASF Release Policy`, but they can be used by the users
who do not want to build the software themselves.

Those are - in the order of most common ways people install Airflow:

- [PyPI releases](https://pypi.org/project/apache-airflow/) to install Airflow using standard `pip` tool
- [Docker Images](https://hub.docker.com/r/apache/airflow) to install airflow via
  `docker` tool, use them in Kubernetes, Helm Charts, `docker-compose`, `docker swarm` etc. You can
  read more about using, customising, and extending the images in the
  [Latest docs](https://airflow.apache.org/docs/apache-airflow/stable/production-deployment.html), and
  learn details on the internals in the [IMAGES.rst](IMAGES.rst) document.
- [Tags in GitHub](https://github.com/apache/airflow/tags) to retrieve the git project sources that
  were used to generate official source packages via git

All those artifacts are not official releases, but they are prepared using officially released sources.
Some of those artifacts are "development" or "pre-release" ones, and they are clearly marked as such
following the ASF Policy.

## User Interface

- **DAGs**: Overview of all DAGs in your environment.

  ![DAGs](/docs/apache-airflow/img/dags.png)

- **Tree**: Tree representation of a DAG that spans across time.

  ![Tree](/docs/apache-airflow/img/tree.png)

- **Graph**: Visualization of a DAG's dependencies and their current status for a specific run.

  ![Graph](/docs/apache-airflow/img/graph.png)

- **Task Duration**: Total time spent on different tasks over time.

  ![Task Duration](/docs/apache-airflow/img/duration.png)

- **Gantt**: Duration and overlap of a DAG.

  ![Gantt](/docs/apache-airflow/img/gantt.png)

- **Code**:  Quick way to view source code of a DAG.

  ![Code](/docs/apache-airflow/img/code.png)


## Contributing

Want to help build Apache Airflow? Check out our [contributing documentation](https://github.com/apache/airflow/blob/main/CONTRIBUTING.rst).

## Who uses Apache Airflow?

More than 400 organizations are using Apache Airflow
[in the wild](https://github.com/apache/airflow/blob/main/INTHEWILD.md).

## Who Maintains Apache Airflow?

Airflow is the work of the [community](https://github.com/apache/airflow/graphs/contributors),
but the [core committers/maintainers](https://people.apache.org/committers-by-project.html#airflow)
are responsible for reviewing and merging PRs as well as steering conversation around new feature requests.
If you would like to become a maintainer, please review the Apache Airflow
[committer requirements](https://github.com/apache/airflow/blob/main/COMMITTERS.rst#guidelines-to-become-an-airflow-committer).

## Can I use the Apache Airflow logo in my presentation?

Yes! Be sure to abide by the Apache Foundation [trademark policies](https://www.apache.org/foundation/marks/#books) and the Apache Airflow [Brandbook](https://cwiki.apache.org/confluence/display/AIRFLOW/Brandbook). The most up to date logos are found in [this repo](/docs/apache-airflow/img/logos) and on the Apache Software Foundation [website](https://www.apache.org/logos/about.html).

## Airflow merchandise

If you would love to have Apache Airflow stickers, t-shirt etc. then check out
[Redbubble Shop](https://www.redbubble.com/i/sticker/Apache-Airflow-by-comdev/40497530.EJUG5).

## Links

- [Documentation](https://airflow.apache.org/docs/apache-airflow/stable/)
- [Chat](https://s.apache.org/airflow-slack)
