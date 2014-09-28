#SETUP IMPALA CONFIGURATION
export IMPALA_HOME=`pwd`
echo $IMPALA_HOME
. bin/impala-config.sh >> /dev/null


#START STATESTORE DAEMON
if [  -z "`ps -elf | pgrep statestored`" ] ; then
	echo "Starting Statestore Daemon..."
	be/build/debug/statestore/statestored & >> /dev/null
fi

#START CATALOGD DAEMON
if [  -z "`ps -elf | pgrep catalogd`" ] ; then
	echo "Starting Catalog Daemon..."
	bin/start-catalogd.sh & >> /dev/null
fi


ps -elf | grep statestored
ps -elf | grep catalogd
