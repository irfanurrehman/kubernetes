export OS_TENANT_ID=2e153d10ab954ca58b2144617e6ea7bf
export OS_TENANT_NAME="RedisFramework"
export OS_PROJECT_NAME="RedisFramework"

# In addition to the owning entity (tenant), OpenStack stores the entity
# performing the action as the **user**.
export OS_USERNAME="irfan"

# With Keystone you pass the keystone password.
export OS_PASSWORD="<pass>"

export OS_AUTH_URL=http://10.145.208.11:5000/v2.0
export OS_IDENTITY_API_VERSION=2.0
export OS_IMAGE_API_VERSION=2
export OS_VOLUME_API_VERSION=2
export OS_REGION_NAME=RegionOne
 
#export KUBERNETES_KEYPAIR_NAME=<keypair-name>            # e.g. my-keypair
export KUBERNETES_KEYPAIR_NAME=federation-kp            # e.g. my-keypair
export CLIENT_PUBLIC_KEY_PATH=/root/work/kp/federation-kp.pub                   # e.g. ~/.ssh/authorized_keys
 
export STACK_NAME=irf-k8s2                                                                # you stack name, change this to create more k8s clusters
export NUMBER_OF_MINIONS=1
export MAX_NUMBER_OF_MINIONS=1
export MASTER_FLAVOR=m1.medium
export MINION_FLAVOR=m1.medium
export FIXED_NETWORK_CIDR=192.168.8.0/24                              # recommended to use different network for k8s clusters
export EXTERNAL_NETWORK=ext-net
export LBAAS_VERSION=v2
export DNS_SERVER=8.8.8.8
export CREATE_IMAGE=false
export DOWNLOAD_IMAGE=false
export IMAGE_ID=0d3fca8d-1225-4dbb-8b8b-f7acf1ffd9cd            # this is uploaded centos 7 image
export SWIFT_SERVER_URL=http://10.145.208.11:7480
export ENABLE_PROXY=false

export K8S_SWIFT_CONTAINER=irf-swift-container
 
export KUBERNETES_PROVIDER=openstack-heat
