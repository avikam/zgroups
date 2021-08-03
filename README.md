# zgroups
Distributed group membership with zookeeper.

A group is defined by its name and capacity. A cluster is a set of groups.
Each node in the cluster will assign itself to one of the groups according to its capacity. If this assignment is
successful, it will reduce the group's capacity by one, and will start a process.
The process will have an environment variable specifying the group membership. When the process is done, the
parent process also dies along with the assignment, increasing the capacity back by one.

This utility helps allocating resources in a dynamic cluster where nodes may come and go occasionally. 

# Todos:
1. Namespace groups in a parent znode and manage all of the ephemeral connections under it