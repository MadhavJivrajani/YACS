#!/bin/bash

sched=$1    #scheduling policy
n=$2        #no. of workers

docker pull aditiahuja/yacs_master
docker pull aditiahuja/yacs_worker

docker run -d --network host -e sched=$sched --name master aditiahuja/yacs_master

sched=$1
n=$2

for (( c=1; c<=$n; c++ ))
do  
   echo "Enter ID and port for worker $c:"
   read id p
   cont_name=w$c
   #echo $cont_name
   docker run -d --network host -e id=$id -e port=$p --name $cont_name aditiahuja/yacs_worker

done

echo "Enter no. of requests:"
read r

docker exec -it master bash -c "python3 gen_requests.py $r"

docker exec -it master bash -c "cat yacs.log"
