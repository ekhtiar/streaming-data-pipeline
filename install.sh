# update the apt package index
sudo apt-get update

# install packages to allow apt to use a repository
sudo apt-get install \
    apt-transport-https \
    ca-certificates \
    curl \
    software-properties-common

# add Docker’s official GPG key
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

# set up the stable repository
sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"

# update the apt package index.
sudo apt-get -y update

# install docker-ce
sudo apt-get install -y docker-ce

# check the current release and if necessary update
sudo curl -L https://github.com/docker/compose/releases/download/1.21.2/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose

# set the permissions
sudo chmod +x /usr/local/bin/docker-compose

# get git
sudo apt-get install -y git

# run jupyter notebook
sudo docker run -d --rm -p 8888:8888 jupyter/pyspark-notebook