# Test for different server CPU limit
__[ worker node 1 ]__ : 30%   
__[ worker node 2 ]__ : 50%  
__[ worker node 3 ]__ : 70%  

```shell
$ sudo docker stack deploy -c [COMPOSE FILE NAME] [STACK NAME]
```

Set worker nodes with different CPU limit with docker compose file

```yml
    version: '3.8'
    services:
    ...
```