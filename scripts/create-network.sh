network_name="node-service-net"
sub_network="10.0.0.0/24"

sudo docker network create --driver overlay --subnet $sub_network $network_name