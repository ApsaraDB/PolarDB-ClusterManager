## What is PolarDB Cluster Manager 

PolarDB Cluster Manager is the cluster management component of PolarDB for PostgreSQL, responsible for topology management, high availability, configuration management, and plugin extensions. It supports both Shared-Nothing cluster and Shared-Storage cluster.

## Code Structure

PolarDB Cluster Manager contains the following sub-directories:

- detector directory：The detection component, through the heartbeat request, detects the availability of the node
- collector directory：Collection components, collect system metrcis (cpu/io...) and database metrics (lock/buffer...) of cluster nodes
- status directory： Cluster status component, maintains the status of the cluster at various latitudes, triggers events through collectors and detectors, and drives cluster status changes
- decision directory： The decision-making component, according to the status of each latitude, makes the decision-making judgment of the corresponding cluster operation, such as the high-availability switching operation
- action directory:  Action component, which specifically executes the operation made by Decision component
- meta directory:  Metadata component, responsible for the persistence of metadata and the high availability of Cluster Manager itself
- service directory:  Service component, responsible for the external HTTP interface, such as status query, manual switching, etc.

## Quick Start
### Build 
0. Install Docker

1. Modify the CLUSTER_MANAGER_IMAGE_NAME
```
vi Makefile
```

2. Build Image
```
make
```

### Deploy
Currently, it is recommended to automate the deployment of PolarDB clusters through [PolarDB-Stack-Operator](https://github.com/ApsaraDB/PolarDB-Stack-Operator), and the manual deployment documents will be released later.

## Contribution

Your contribution is greatly appreciated. Refer to contributing for development and pull request.

## Software license description

PolarDB Cluster Manager's code was released under the Apache version 2.0 software license. See[License](https://github.com/alibaba/PolarDB-for-PostgreSQL/blob/master/LICENSE) and [NOTICE](https://github.com/alibaba/PolarDB-for-PostgreSQL/blob/master/NOTICE)。

## Contact us

Use the DingTalk to scan the following QR code and join the PolarDB technology group.

