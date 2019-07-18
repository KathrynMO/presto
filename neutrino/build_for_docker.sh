#!/bin/bash -ex
set -x
set -e
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $SCRIPT_DIR/..
mvn -P neutrino clean package -pl presto-aresdb-plugin,presto-event-listener,presto-main,presto-pinot-plugin,presto-rta,presto-server-neutrino,presto-spi -am -TC1 -DskipTests $*
