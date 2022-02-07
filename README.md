# distributed_project_2021f

- The goal of this project is to implement Dynamos, a weak form of consistency, and analyze its performance on reads and writes

## Running the Application

1. Install Vagrant and VirtualBox
2. git clone [project]
3. cd [project_folder]
4. vagrant up
5. vagrant ssh
6. cd /vagrant
7. mix deps.get
   1. Install dependencies
8. mix
   1. Compile
9. mix test
   1. mix test [file]
10. mix run -e '[function]'

## Related Work

1. [Dynamo: Amazon’s Highly Available Key-value Store](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf).
2. [Quantifying eventual consistency with PBS](https://shivaram.org/publications/pbs-vldb-journal.pdf).
3. [Discord Ring](https://github.com/discord/ex_hash_ring)

## Merkle Tree

- Implemented using a Binary Search Tree
- Each node holds the hash of itself and its children
- Only supports appending because Dynamos will only make 2 calls, get() and put()

## Paper

- [PDF](https://docs.google.com/document/d/1oiG08IjbvRc3l7J00PemFD1FKO7obR6JsDMkekbTIDE/edit?usp=sharing)
