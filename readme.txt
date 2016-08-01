# How to start MapReduce framework

# Starts dns server(requires separate console)
docker run --name dns --rm -v /var/run/docker.sock:/docker.sock phensley/docker-dns --domain example.com

# Start 4 Slave nodes, looks for dns ip from dns container
docker run --name node1 --rm --dns $(docker inspect -f '{{.NetworkSettings.IPAddress}}' dns) --dns-search example.com 13.92.240.181:5000/kittens/map_reduce python /src/ga1/node1.py &

docker run --name node2 --rm --dns $(docker inspect -f '{{.NetworkSettings.IPAddress}}' dns) --dns-search example.com 13.92.240.181:5000/kittens/map_reduce python /src/ga1/node2.py &

docker run --name node3 --rm --dns $(docker inspect -f '{{.NetworkSettings.IPAddress}}' dns) --dns-search example.com 13.92.240.181:5000/kittens/map_reduce python /src/ga1/node3.py &

# Start one of them in interactive mode (preferably in separate console)
# Use this one to interact with the framework
docker run -ti --name node4 --rm --dns $(docker inspect -f '{{.NetworkSettings.IPAddress}}' dns) --dns-search example.com 13.92.240.181:5000/kittens/map_reduce python /src/ga1/node4.py

# Starts Master node, looks for dns ip from dns container
docker run --name server --rm --dns $(docker inspect -f '{{.NetworkSettings.IPAddress}}' dns) --dns-search example.com 13.92.240.181:5000/kittens/map_reduce python /src/ga1/master.py &
