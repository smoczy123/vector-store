# Scripts to create a scylladb + vector-store cluster in AWS

These scripts were created for benchmarking a scylladb + vector-store cluster
in AWS in 2025, for the [P99 conference](p99conf.io).

At the beginning, you need to create 3 EC2 instances for the scylla (ie.
i4i.xlarge) and 3 EC2 instances for the vector-store (ie. r7i.xlarge). Then
create the file `env-aws-cluster` with environment values - you can use
`env-aws-cluster-example` as a template. This file is sourced by the scripts
below - it is a source of truth for cluster configuration.

The environment variables are:
- `DB_IP_O_EXT`: external IP of the first scylla node
- `DB_IP_O_INT`: internal IP of the first scylla node
- `DB_IP_1_EXT`: external IP of the second scylla node
- `DB_IP_1_INT`: internal IP of the second scylla node
- `DB_IP_2_EXT`: external IP of the third scylla node
- `DB_IP_2_INT`: internal IP of the third scylla node
- `VS_IP_O_EXT`: external IP of the first vector-store node
- `VS_IP_O_INT`: internal IP of the first vector-store node
- `VS_IP_1_EXT`: external IP of the second vector-store node
- `VS_IP_1_INT`: internal IP of the second vector-store node
- `VS_IP_2_EXT`: external IP of the third vector-store node
- `VS_IP_2_INT`: internal IP of the third vector-store node
- `SCYLLA_DOCKER_IMAGE`: tag for the scylla docker image to use
- `LOCAL_SSH_PUB_KEY`: path to your local ssh public key - it will be copied
  to the nodes to allow passwordless ssh
- `AWS_SSH_KEY`: path to your AWS ssh private key - it will be used to
  connect to the nodes for the first time while preparing them

There are two scripts to prepare the nodes, which should be run after creating
instances in AWS (for every instance): `make-aws-scylla-instance` and
`make-aws-vs-instance`.

Running scylla node is done by `run-scylla-in-aws-cluster` and vector-store
node by `run-vs-in-aws-cluster`.

There are scripts to run additional tools in the cluster:
`run-cqlsh-in-aws-cluster`, `run-nodetool-in-aws-cluster`.

