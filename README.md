# Sauna

[ ![Build Status] [travis-image] ] [travis]  [ ![Release] [release-image] ] [releases] [ ![License] [license-image] ] [license]

Sauna is a decisioning and response framework, powered by Scala, Akka and Iglu.

Current primary features include:

- [Optimizely][optimizely] [Targeting lists][targeting-lists] upload
- [Optimizely][optimizely] [Dynamic Customer Profiles][dcp] upload
- [Sendgrid][sendgrid] [Recipients list][recipients-list] upload
- Listening events on local filesystem
- Listening events on [Amazon S3][s3]

## User Quickstart

Download the latest Sauna from Bintray:

```bash
$ wget http://dl.bintray.com/snowplow/sauna/sauna_0.1.0.zip
$ unzip sauna_0.1.0.zip
$ ./sauna --configurations {{path-to-avroconfigs}} --port {{port}}
```

Assuming you have a recent JVM installed.

Sauna can be configured using Self-describing Avro (in non-binary JSON format)
and these Avros should be placed into `{{path-to-avroconfigs}}` directory.

## Developer Quickstart

Assuming git, **[Vagrant] [vagrant-install]** and **[VirtualBox] [virtualbox-install]** installed:

```bash
 host$ git clone https://github.com/snowplow/sauna.git
 host$ cd sauna
 host$ vagrant up && vagrant ssh
guest$ cd /vagrant
guest$ sbt assembly
```


[travis]: https://travis-ci.org/snowplow/sauna
[travis-image]: https://travis-ci.org/snowplow/sauna.png?branch=master

[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0

[release-image]: http://img.shields.io/badge/release-0.1.0--rc1-blue.svg?style=flat
[releases]: https://github.com/snowplow/sauna/releases

[vagrant-install]: http://docs.vagrantup.com/v2/installation/index.html
[virtualbox-install]: https://www.virtualbox.org/wiki/Downloads

[targeting-lists]: https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#targeting-list
[dcp]: https://github.com/snowplow/sauna/wiki/Optimizely-responder-user-guide#dcp-batch
[recipients-list]: https://github.com/snowplow/sauna/wiki/SendGrid-responder-user-guide

[optimizely]: https://optimizely.com/
[sendgrid]: https://sendgrid.com/
[s3]: http://docs.aws.amazon.com/AmazonS3/latest/dev/Welcome.html
