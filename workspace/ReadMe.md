
### Build the kafka image from docker file.

Navigate to /workspace/kafka-setup/docker-image directory and issue below docker commands to build the kafka-image.

```bash
$ docker build --tag=<repository-name>/<imagae-name> .
$ docker build --tag=ssamantr/reactive-kafka .
```

Push the kafka image to docker hub
```bash
$ docker push <repository-name>/<imagae-name>
$ docker push ssamantr/reactive-kafka
```

Run the kafka image
```bash
$ docker run <repository-name>/<imagae-name>
$ docker run ssamantr/reactive-kafka
```

As an alternative you can also run docker image from docker compose

Navigate to /workspace/kafka-setup/docker-compose directory

```bash
$ docker-compose up
```


