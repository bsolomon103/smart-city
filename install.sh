#!/bin/bash

# Install Git
sudo yum install git -y

# Install Docker
sudo amazon-linux-extras install docker -y
sudo systemctl start docker
sudo systemctl enable docker

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Confirm Docker and Docker Compose versions
docker --version
docker-compose --version

# Add the user to the Docker group
sudo usermod -aG docker ec2-user

# Print "Process complete"
echo "Process complete exit terminal to activate changes"