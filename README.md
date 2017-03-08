# Elasticsearch Mesos Framework

## Documentation

[http://mesos-elasticsearch.readthedocs.org/en/latest/](http://mesos-elasticsearch.readthedocs.org/en/latest/)

## Support

Get in touch with the Mesos Elasticsearch framework developers and community via the [Mesos Elasticsearch Google Group](https://groups.google.com/forum/#!forum/mesos-elasticsearch)

## Sponsors

This project is sponsored by Cisco Cloud Services

## License

Apache License 2.0

## TAMR Specific Modifications

* Added TAMR\_ES\_EXTRA\_DOCKER_PARAMS
  * Can be used to added arbitrary docker CLI args when launching the
    Elasticsearch nodes.  Syntax: (SEP Key SEP Value)*  SEP is any single char.
* TAMR_ES\_BOOTSTRAP_MLOCKALL
  * "true"|"false" - sets bootstrap.mlockall option for Elasticsearch.
* pushToTamrNexus.sh
  * Script to upload the elasticsearch scheduler jar to nexus with Maven POM
    files that make it consumable by
    javasrc/docker/mesos-elasticsearch-scheduler
  * Requires Maven 3.3.9 or greater to be installed locally
