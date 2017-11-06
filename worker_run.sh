#!/bin/bash -e

echo "Running on:" $(hostname -f)

# Template script to run sframe_main on worker node
# Required to setup correct paths etc as LD_LIBRARY_PATH not exposed
# Args: XML filename
WORKDIR=$(pwd)
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd $CMSSW_BASE/src
eval `scramv1 runtime -sh`
cd $SFRAME_DIR
unset SFRAME_DIR
source setup.sh
cd $WORKDIR
sframe_main $1
