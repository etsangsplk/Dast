1. In the dispatch phase, the coordinator dispatches the ''ready'' pieces to nearest shards, let the shards to pre-execute them,
get the outputs, so that the coordinator can pop-up the inputs of remaining pieces.
The dispatch process is run recursively.    
    * In other words, the dispatch is also part of the CC. 
2. Problem: The current implementation badly supports NF-Replication. 
    * It does not matter for full replication, as the non-full replication needs WAN RTT for pre-accept. 

TODO:
1. The multi-version row
2. Re-implement the workload into independent
3. Implement Ocean Vista
4. Think about the slow path. How should slow path work. 