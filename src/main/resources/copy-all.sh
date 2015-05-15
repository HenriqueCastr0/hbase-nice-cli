#!/bin/bash
sudo    cp -f ${tmp.dir}/* ${dependency.dir}
        cp -f ${project.basedir}/target/hbase-cli-1.1-SNAPSHOT.jar /usr/lib/hbase-cli/hbase-cli.jar
        cp -f ${project.basedir}/target/classes/hbase-cli /usr/bin/hbase-cli
        chmod +x /usr/bin/hbase-cli
        cp -f ${project.basedir}/target/classes/bash-completion /etc/bash_completion.d/hbase-cli
        source /etc/bash_completion.d/hbase-cli

