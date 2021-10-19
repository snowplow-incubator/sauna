# Sauna

[![Build Status][travis-image]][travis]
[![Release][release-image]][releases] 
[![License][license-image]][license]

Sauna is a decisioning and response framework, powered by Scala, Akka actors and Iglu.

## Quickstart

Download the latest Sauna from Bintray:

```bash
$ wget http://dl.bintray.com/snowplow/snowplow-generic/sauna_0.1.0.zip
$ unzip sauna_0.1.0.zip
$ ./sauna --configurations {{path-to-avroconfigs}}
```

Assuming you have a recent JVM installed.

Sauna can be [configured][configuration] using Self-describing Avro (in non-binary JSON format) and these Avros should be placed into `{{path-to-avroconfigs}}` directory.
You can read more about how to install and configure Sauna in [Guide for Devops][devops-guide].

## Find out more

|  **[Devops Guide][devops-guide]**     | **[Analysts Guide][analysts-guide]**     | **[Developers Guide][developers-guide]**     |
|:--------------------------------------:|:-----------------------------------------:|:---------------------------------------------:|


## Features

- [Optimizely][optimizely] [Targeting lists][targeting-lists] upload
- [Optimizely][optimizely] [Dynamic Customer Profiles][dcp] upload
- [Sendgrid][sendgrid] [Recipients list][recipients-list] upload
- Listening events on [Local filesystem][local-observer]
- Listening events on [Amazon S3][s3-observer]


## Contributing

Sauna designed to have extremely loosely-coupled architecture and we would love to get your contributions within each of the three sub-systems.

If you would like help implementing a new responder, observer or logger check out our **[Guide for developers] [developers-guide]** page on the wiki!

## Questions or need help?

Check out the **[Talk to us] [talk-to-us]** page on our wiki.

## Copyright and license

Sauna is copyright 2016 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0] [license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


[travis]: https://travis-ci.org/snowplow/sauna
[travis-image]: https://travis-ci.org/snowplow/sauna.png?branch=master

[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0

[release-image]: http://img.shields.io/badge/release-0.1.0-blue.svg?style=flat
[releases]: https://github.com/snowplow/sauna/releases

[vagrant-install]: http://docs.vagrantup.com/v2/installation/index.html
[virtualbox-install]: https://www.virtualbox.org/wiki/Downloads

[targeting-lists]: https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#targeting-list
[dcp]: https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#dcp-batch
[recipients-list]: https://github.com/snowplow/sauna/wiki/SendGrid-responder-user-guide

[optimizely]: https://optimizely.com/
[sendgrid]: https://sendgrid.com/

[s3-observer]: https://github.com/snowplow/sauna/wiki/Amazon-S3-Observer-setup-guide
[local-observer]: https://github.com/snowplow/sauna/wiki/Local-Filesystem-Observer-setup-guide

[configuration]: https://github.com/snowplow/sauna/wiki/Setting-up-Sauna#configuration

[analysts-guide]: https://github.com/snowplow/sauna/wiki/Guide-for-analysts
[developers-guide]: https://github.com/snowplow/sauna/wiki/Guide-for-developers
[devops-guide]: https://github.com/snowplow/sauna/wiki/Guide-for-devops

[talk-to-us]: https://github.com/snowplow/snowplow/wiki/Talk-to-us
