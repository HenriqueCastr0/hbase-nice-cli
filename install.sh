#!/bin/bash
cd "$( dirname "${BASH_SOURCE[0]}" )"
mvn clean install
sudo chmod +x /tmp/hbase-cli/copy-all.sh
sudo /tmp/hbase-cli/copy-all.sh