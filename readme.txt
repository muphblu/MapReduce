# How to start distributed file system

# Starts dns server(requires separate console)
docker run --name dns --rm -v /var/run/docker.sock:/docker.sock phensley/docker-dns --domain example.com

# Starts 4 storage servers, looks for dns ip from dns container
docker run --name node1 --rm --dns $(docker inspect -f '{{.NetworkSettings.IPAddress}}' dns) --dns-search example.com 13.92.240.181:5000/kittens/dfs python /src/ga1/storage_server.py 1 &

docker run --name node2 --rm --dns $(docker inspect -f '{{.NetworkSettings.IPAddress}}' dns) --dns-search example.com 13.92.240.181:5000/kittens/dfs python /src/ga1/storage_server.py 2 &

docker run --name node3 --rm --dns $(docker inspect -f '{{.NetworkSettings.IPAddress}}' dns) --dns-search example.com 13.92.240.181:5000/kittens/dfs python /src/ga1/storage_server.py 3 &

docker run --name node4 --rm --dns $(docker inspect -f '{{.NetworkSettings.IPAddress}}' dns) --dns-search example.com 13.92.240.181:5000/kittens/dfs python /src/ga1/storage_server.py 4 &

# Starts naming server, looks for dns ip from dns container
docker run --name server --rm --dns $(docker inspect -f '{{.NetworkSettings.IPAddress}}' dns) --dns-search example.com 13.92.240.181:5000/kittens/dfs python /src/ga1/naming_server.py &

# Starts interactive client, looks for dns ip from dns container
docker run -ti --name client --rm --dns $(docker inspect -f '{{.NetworkSettings.IPAddress}}' dns) --dns-search example.com 13.92.240.181:5000/kittens/dfs python /src/ga1/client.py
