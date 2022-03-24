#!/bin/bash

echo "hadoop soft nofile 655350" >> /etc/security/limits.conf
echo "hadoop hard nofile 655350" >> /etc/security/limits.conf
